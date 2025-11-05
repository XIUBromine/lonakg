"""
黑名单uid节点关联异常节点分析脚本

该脚本分析blacklisted uid节点的k跳关联异常节点特征，并与正常uid节点进行对比。

异常节点定义：
1. blacklisted状态的节点（uid、phone_num、identity_no）
2. 非uid节点的associated_uid_count > 1

分析维度：
- 异常节点数量分布
- 异常节点类型分布
- 关联度分布
- k跳距离分析
"""
import json
from typing import Dict, List, Any, Set, Tuple
from dataclasses import dataclass

from neo4j import GraphDatabase, Session
from dotenv import load_dotenv
import os
from sklearn.metrics import roc_auc_score, classification_report
import numpy as np

# 加载Neo4j连接信息
load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "123456")

# 所有节点类型
ALL_NODE_LABELS = [
    "uid",
    "phone_num",
    "identity_no",
    "card_no",
    "device_no",
    "td_device_id",
    "remote_ip",
    "geo_code",
]

# 可能有黑名单状态的节点类型
BLACKLISTABLE_LABELS = ["uid", "phone_num", "identity_no"]


@dataclass
class AnomalyNode:
    """异常节点数据结构"""

    node_type: str  # blacklisted 或 anomalous
    label: str  # 节点标签
    associated_uid_count: int  # 关联uid数量
    hop_distance: int  # 跳数距离
    node_key: str = ""  # 节点key（用于去重）


class AnomalyDetection:
    """黑名单邻域分析器"""

    def __init__(self, max_k_hops: int = 3, weights=None):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        self.max_k_hops = max_k_hops
        self.weights = weights

    def close(self):
        """关闭数据库连接"""
        self.driver.close()

    def get_risk_score(self, session: Session, uid_key: str) -> float:
        """分析单个uid的邻域异常节点"""

        # 一次性获取所有k跳的异常节点
        all_anomaly_nodes = self.find_anomaly_nodes_k_hop(
            session, uid_key, self.max_k_hops
        )

        risk_score = 0
        for node in all_anomaly_nodes:
            # 构建权重键：node_type + "_" + label
            weight_key = f"{node.node_type}_{node.label}"
            hop_weights = self.weights[node.hop_distance - 1]
            weight = hop_weights.get(weight_key, 0)
            if weight_key == "blacklisted_uid":
                risk_score += weight
            else:
                risk_score += weight * (node.associated_uid_count - 1)

        return risk_score


    def find_anomaly_nodes_k_hop(
        self, session: Session, start_uid: str, k: int = 3
    ) -> List[AnomalyNode]:
        """查找从指定uid开始k跳内的所有异常节点"""

        # 构建k跳查询
        # 这里使用变长路径查询，限制最大跳数
        query = f"""
        MATCH (start:uid {{uid_key: $start_uid}})
        MATCH path = (start)-[*1..{k}]-(n)
        WHERE labels(n)[0] IN $all_labels AND n <> start
        WITH DISTINCT n, length(path) as hop_distance
        
        // 检查是否为异常节点
        WITH n, hop_distance,
             CASE 
                 WHEN labels(n)[0] IN $blacklistable_labels AND n.status = 'blacklisted' THEN 'blacklisted'
                 WHEN labels(n)[0] <> 'uid' AND n.associated_uid_count > 1 THEN 'anomalous'
                 ELSE 'normal'
             END as node_type

        WHERE node_type IN ['blacklisted', 'anomalous']
        
        RETURN 
            node_type,
            labels(n)[0] as label,
            COALESCE(n.associated_uid_count, 0) as associated_uid_count,
            hop_distance,
            CASE 
                WHEN labels(n)[0] = 'uid' THEN n.uid_key
                ELSE n.key
            END as node_key
        ORDER BY hop_distance, node_type, label
        """

        result = session.run(
            query,
            start_uid=start_uid,
            all_labels=ALL_NODE_LABELS,
            blacklistable_labels=BLACKLISTABLE_LABELS,
        )

        anomaly_nodes = []
        seen_nodes = set()  # 用于去重

        for record in result:
            node_key = f"{record['label']}_{record['node_key']}"
            if node_key not in seen_nodes:
                seen_nodes.add(node_key)

                anomaly_nodes.append(
                    AnomalyNode(
                        node_type=record["node_type"],
                        label=record["label"],
                        associated_uid_count=record["associated_uid_count"],
                        hop_distance=record["hop_distance"],
                        node_key=node_key,
                    )
                )

        return anomaly_nodes

def main():
    """主函数"""
    MAX_K_HOPS = 3
    weights = [
        # 1跳权重
        {
            "blacklisted_phone_num": 1,
            "blacklisted_identity_no": 1,
            "anomalous_phone_num": 1,
            "anomalous_identity_no": 1,
            "anomalous_card_no": 1,
            "anomalous_device_no": 1,
            "anomalous_td_device_id": 1,
            "anomalous_remote_ip": 1,
            "anomalous_geo_code": 1
        },
        # 2跳权重
        {
            "blacklisted_uid": 10
        },
        # 3跳权重
        {
            "blacklisted_phone_num": 10,
            "blacklisted_identity_no": 10,
            "anomalous_phone_num": 0.1,
            "anomalous_identity_no": 0.1,
            "anomalous_card_no": 0.1,
            "anomalous_device_no": 0.1,
            "anomalous_td_device_id": 0.1,
            "anomalous_remote_ip": 0.1,
            "anomalous_geo_code": 0.1
        },
    ]

    model = AnomalyDetection(max_k_hops=MAX_K_HOPS, weights=weights)

    try:
        with model.driver.session() as session:
            # 读取黑名单和正常uid
            with open("data_analysis/normal_user_ids.txt", "r", encoding="utf-8") as f:
                normal_uids = [line.strip() for line in f if line.strip()]
            with open(
                "data_analysis/blacklist_user_ids.txt", "r", encoding="utf-8"
            ) as f:
                blacklist_uids = [line.strip() for line in f if line.strip()]

            print(f"📊 开始分析 {len(normal_uids)} 个正常用户和 {len(blacklist_uids)} 个黑名单用户")
            
            risk_scores = []
            true_labels = []
            uids_list = []
            
            # 处理黑名单用户（标签为1）
            print("🔍 分析黑名单用户...")
            for i, uid in enumerate(blacklist_uids):
                risk_score = model.get_risk_score(session, uid)
                risk_scores.append(risk_score)
                true_labels.append(1)  # 黑名单用户标签为1
                uids_list.append(uid)
                if (i + 1) % 100 == 0:
                    print(f"  已处理黑名单用户: {i + 1}/{len(blacklist_uids)}")
                print(f"UID: {uid}, Status: blacklisted, Risk Score: {risk_score}")
            
            # 处理正常用户（标签为0）
            print("🔍 分析正常用户...")
            for i, uid in enumerate(normal_uids):
                risk_score = model.get_risk_score(session, uid)
                risk_scores.append(risk_score)
                true_labels.append(0)  # 正常用户标签为0
                uids_list.append(uid)
                if (i + 1) % 100 == 0:
                    print(f"  已处理正常用户: {i + 1}/{len(normal_uids)}")
                print(f"UID: {uid}, Status: normal, Risk Score: {risk_score}")
            
            # 转换为numpy数组
            risk_scores = np.array(risk_scores)
            true_labels = np.array(true_labels)
            
            print(f"\n📈 模型评估结果:")
            print(f"总样本数: {len(risk_scores)}")
            print(f"正样本数（黑名单）: {sum(true_labels)}")
            print(f"负样本数（正常用户）: {len(true_labels) - sum(true_labels)}")
            
            # 计算AUC
            if len(set(true_labels)) > 1:  # 确保有两种标签
                auc_score = roc_auc_score(true_labels, risk_scores)
                print(f"🎯 AUC Score: {auc_score:.4f}")
                
                # 显示风险分数统计
                print(f"\n📊 风险分数统计:")
                print(f"黑名单用户风险分数 - 均值: {np.mean(risk_scores[true_labels==1]):.4f}, "
                      f"标准差: {np.std(risk_scores[true_labels==1]):.4f}")
                print(f"正常用户风险分数 - 均值: {np.mean(risk_scores[true_labels==0]):.4f}, "
                      f"标准差: {np.std(risk_scores[true_labels==0]):.4f}")
                
                # 保存结果到文件
                results = {
                    'auc_score': float(auc_score),
                    'total_samples': len(risk_scores),
                    'positive_samples': int(sum(true_labels)),
                    'negative_samples': int(len(true_labels) - sum(true_labels)),
                    'blacklist_risk_mean': float(np.mean(risk_scores[true_labels==1])),
                    'blacklist_risk_std': float(np.std(risk_scores[true_labels==1])),
                    'normal_risk_mean': float(np.mean(risk_scores[true_labels==0])),
                    'normal_risk_std': float(np.std(risk_scores[true_labels==0])),
                    'weights': weights
                }
                
                with open("model/evaluation_results.json", "w", encoding="utf-8") as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
                print(f"📁 评估结果已保存到 model/evaluation_results.json")
                
                # 保存详细预测结果
                detailed_results = []
                for i, (uid, true_label, risk_score) in enumerate(zip(uids_list, true_labels, risk_scores)):
                    detailed_results.append({
                        'uid': uid,
                        'true_label': int(true_label),
                        'risk_score': float(risk_score),
                        'status': 'blacklisted' if true_label == 1 else 'normal'
                    })
                
                with open("model/detailed_predictions.json", "w", encoding="utf-8") as f:
                    json.dump(detailed_results, f, ensure_ascii=False, indent=2)
                print(f"📁 详细预测结果已保存到 model/detailed_predictions.json")
                
            else:
                print("⚠️  警告: 只有一种标签，无法计算AUC")
            

    except Exception as e:
        print(f"❌ 分析过程中出现错误: {e}")
        raise

    finally:
        model.close()


if __name__ == "__main__":
    main()

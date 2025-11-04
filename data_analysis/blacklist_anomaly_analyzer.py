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

from __future__ import annotations

from html import parser
import json
import statistics
from typing import Dict, List, Any, Set, Tuple
from dataclasses import dataclass
from collections import defaultdict, Counter

from neo4j import GraphDatabase, Session
from dotenv import load_dotenv
import os

# 加载Neo4j连接信息
load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "123456")

# 所有节点类型
ALL_NODE_LABELS = [
    "uid", "phone_num", "identity_no", "card_no", 
    "device_no", "td_device_id", "remote_ip", "geo_code"
]

# 可能有黑名单状态的节点类型
BLACKLISTABLE_LABELS = ["uid", "phone_num", "identity_no"]


@dataclass
class AnomalyNode:
    """异常节点数据结构"""
    node_type: str  # blacklisted 或 common
    label: str      # 节点标签
    associated_uid_count: int  # 关联uid数量
    hop_distance: int         # 跳数距离
    node_key: str = ""        # 节点key（用于去重）


@dataclass
class AnomalyNodeStats:
    """异常节点统计信息"""
    node_type: str  # 'blacklisted_uid', 'blacklisted_phone_num', 'common_phone_num' 等
    count: int
    uid_association_counts: List[int]  # 这些异常节点各自关联的uid数量


@dataclass
class HopAnalysis:
    """单跳分析结果"""
    hop_distance: int
    total_anomaly_nodes: int
    anomaly_stats_by_type: Dict[str, AnomalyNodeStats]
    
    def get_avg_anomaly_nodes(self) -> float:
        return self.total_anomaly_nodes
    
    def get_type_distribution(self) -> Dict[str, int]:
        return {node_type: stats.count for node_type, stats in self.anomaly_stats_by_type.items()}
    
    def get_uid_association_stats(self) -> Dict[str, Dict[str, float]]:
        """获取每种类型异常节点的关联uid数量统计"""
        stats = {}
        for node_type, anomaly_stats in self.anomaly_stats_by_type.items():
            if anomaly_stats.uid_association_counts:
                stats[node_type] = {
                    'mean': statistics.mean(anomaly_stats.uid_association_counts),
                    'median': statistics.median(anomaly_stats.uid_association_counts),
                    'max': max(anomaly_stats.uid_association_counts),
                    'min': min(anomaly_stats.uid_association_counts)
                }
            else:
                stats[node_type] = {'mean': 0, 'median': 0, 'max': 0, 'min': 0}
        return stats


@dataclass
class UidNeighborhoodAnalysis:
    """单个uid的邻域分析结果"""
    uid_key: str
    is_blacklisted: bool
    hop_analyses: Dict[int, HopAnalysis]  # key: hop_distance, value: HopAnalysis
    
    def get_total_anomaly_nodes_by_hop(self) -> Dict[int, int]:
        return {hop: analysis.total_anomaly_nodes for hop, analysis in self.hop_analyses.items()}


@dataclass
class GroupAnalysisResult:
    """群体分析结果"""
    group_name: str
    uid_count: int
    uid_analyses: List[UidNeighborhoodAnalysis]
    
    def get_avg_anomaly_nodes_by_hop(self) -> Dict[int, float]:
        """获取每跳平均异常节点数"""
        hop_totals = defaultdict(list)
        
        for uid_analysis in self.uid_analyses:
            for hop, analysis in uid_analysis.hop_analyses.items():
                hop_totals[hop].append(analysis.total_anomaly_nodes)
        
        return {hop: statistics.mean(counts) if counts else 0 
                for hop, counts in hop_totals.items()}
    
    def get_type_distribution_by_hop(self) -> Dict[int, Dict[str, List[int]]]:
        """获取每跳每种类型异常节点的分布"""
        hop_type_distributions = defaultdict(lambda: defaultdict(list))
        
        for uid_analysis in self.uid_analyses:
            for hop, analysis in uid_analysis.hop_analyses.items():
                type_dist = analysis.get_type_distribution()
                for node_type, count in type_dist.items():
                    hop_type_distributions[hop][node_type].append(count)
        
        return dict(hop_type_distributions)
    
    def get_uid_association_distribution_by_hop(self) -> Dict[int, Dict[str, List[List[int]]]]:
        """获取每跳每种类型异常节点的uid关联数分布"""
        hop_assoc_distributions = defaultdict(lambda: defaultdict(list))
        
        for uid_analysis in self.uid_analyses:
            for hop, analysis in uid_analysis.hop_analyses.items():
                for node_type, stats in analysis.anomaly_stats_by_type.items():
                    hop_assoc_distributions[hop][node_type].append(stats.uid_association_counts)
        
        return dict(hop_assoc_distributions)
    
    def get_isolated_blacklist_stats(self, isolation_threshold: int = 2) -> Dict[str, Any]:
        """获取孤立黑名单节点统计"""
        # 只统计黑名单uid
        blacklist_analyses = [analysis for analysis in self.uid_analyses if analysis.is_blacklisted]
        
        if not blacklist_analyses:
            return {
                "total_blacklist_uids": 0,
                "isolated_blacklist_uids": 0,
                "isolation_rate": 0.0,
                "isolated_uid_list": [],
                "non_isolated_stats": {
                    "count": 0,
                    "avg_total_anomaly_nodes": 0,
                    "max_total_anomaly_nodes": 0
                }
            }
        
        isolated_uids = []
        non_isolated_anomaly_counts = []
        
        for analysis in blacklist_analyses:
            # 计算该uid的总异常节点数（所有跳数的总和）
            total_anomaly_nodes = sum(hop_analysis.total_anomaly_nodes 
                                    for hop_analysis in analysis.hop_analyses.values())
            
            if total_anomaly_nodes <= isolation_threshold:
                isolated_uids.append({
                    "uid_key": analysis.uid_key,
                    "total_anomaly_nodes": total_anomaly_nodes,
                    "hop_breakdown": {hop: hop_analysis.total_anomaly_nodes 
                                    for hop, hop_analysis in analysis.hop_analyses.items()}
                })
            else:
                non_isolated_anomaly_counts.append(total_anomaly_nodes)
        
        isolation_rate = len(isolated_uids) / len(blacklist_analyses)
        
        return {
            "total_blacklist_uids": len(blacklist_analyses),
            "isolated_blacklist_uids": len(isolated_uids),
            "isolation_rate": isolation_rate,
            "isolation_threshold": isolation_threshold,
            "isolated_uid_list": isolated_uids,
            "non_isolated_stats": {
                "count": len(non_isolated_anomaly_counts),
                "avg_total_anomaly_nodes": statistics.mean(non_isolated_anomaly_counts) if non_isolated_anomaly_counts else 0,
                "max_total_anomaly_nodes": max(non_isolated_anomaly_counts) if non_isolated_anomaly_counts else 0,
                "min_total_anomaly_nodes": min(non_isolated_anomaly_counts) if non_isolated_anomaly_counts else 0
            }
        }


class BlacklistAnalyzer:
    """黑名单邻域分析器"""
    
    def __init__(self, max_k_hops: int = 3):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        self.max_k_hops = max_k_hops
    
    def close(self):
        """关闭数据库连接"""
        self.driver.close()
    
    def get_all_uids(self, session: Session, limit: int = None) -> List[Tuple[str, bool]]:
        """获取所有uid及其黑名单状态"""
        query = """
        MATCH (u:uid)
        RETURN u.uid_key as uid_key, 
               COALESCE(u.status = 'blacklisted', false) as is_blacklisted
        """
        if limit:
            query += f" LIMIT {limit}"
        
        result = session.run(query)
        uids = [(record["uid_key"], record["is_blacklisted"]) for record in result]
        
        blacklist_count = sum(1 for _, is_bl in uids if is_bl)
        normal_count = len(uids) - blacklist_count
        
        print(f"获取到 {len(uids)} 个uid: {blacklist_count} 个黑名单, {normal_count} 个正常")
        return uids
    
    def analyze_uid_neighborhood(self, session: Session, uid_key: str, is_blacklisted: bool) -> UidNeighborhoodAnalysis:
        """分析单个uid的邻域异常节点"""
        
        # 一次性获取所有k跳的异常节点
        all_anomaly_nodes = self.find_anomaly_nodes_k_hop(session, uid_key, self.max_k_hops)
        
        # 按跳数分组
        nodes_by_hop = defaultdict(list)
        for node in all_anomaly_nodes:
            nodes_by_hop[node.hop_distance].append(node)
        
        hop_analyses = {}
        
        for hop in range(1, self.max_k_hops + 1):
            # 获取该跳数的异常节点
            hop_nodes = nodes_by_hop.get(hop, [])
            
            # 按类型分组统计
            anomaly_stats_by_type = self._group_anomaly_nodes_by_type(hop_nodes)
            
            hop_analyses[hop] = HopAnalysis(
                hop_distance=hop,
                total_anomaly_nodes=len(hop_nodes),
                anomaly_stats_by_type=anomaly_stats_by_type
            )
        
        return UidNeighborhoodAnalysis(
            uid_key=uid_key,
            is_blacklisted=is_blacklisted,
            hop_analyses=hop_analyses
        )
    
    def find_anomaly_nodes_k_hop(self, session: Session, start_uid: str, k: int = 2) -> List[AnomalyNode]:
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
                 WHEN labels(n)[0] <> 'uid' AND n.associated_uid_count > 1 THEN 'common'
                 ELSE 'normal'
             END as node_type
        
        WHERE node_type IN ['blacklisted', 'common']
        
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
        
        result = session.run(query, 
                           start_uid=start_uid, 
                           all_labels=ALL_NODE_LABELS,
                           blacklistable_labels=BLACKLISTABLE_LABELS)
        
        anomaly_nodes = []
        seen_nodes = set()  # 用于去重
        
        for record in result:
            node_key = f"{record['label']}_{record['node_key']}"
            if node_key not in seen_nodes:
                seen_nodes.add(node_key)
                
                anomaly_nodes.append(AnomalyNode(
                    node_type=record["node_type"],
                    label=record["label"],
                    associated_uid_count=record["associated_uid_count"],
                    hop_distance=record["hop_distance"],
                    node_key=node_key
                ))
        
        return anomaly_nodes
    
    def _group_anomaly_nodes_by_type(self, anomaly_nodes: List[AnomalyNode]) -> Dict[str, AnomalyNodeStats]:
        """按详细类型分组异常节点"""
        
        type_groups = defaultdict(list)
        
        for node in anomaly_nodes:
            # 构建详细类型名称
            if node.node_type == 'blacklisted':
                detailed_type = f"blacklisted_{node.label}"
            else:  # common
                detailed_type = f"common_{node.label}"
            
            type_groups[detailed_type].append(node)
        
        # 为每个类型计算统计信息
        anomaly_stats = {}
        for detailed_type, nodes in type_groups.items():
            uid_association_counts = [node.associated_uid_count for node in nodes 
                                    if node.associated_uid_count > 0]
            
            anomaly_stats[detailed_type] = AnomalyNodeStats(
                node_type=detailed_type,
                count=len(nodes),
                uid_association_counts=uid_association_counts
            )
        
        return anomaly_stats
    
    def batch_analyze_neighborhoods(self, session: Session, uids: List[Tuple[str, bool]], 
                                  sample_size: int = None) -> List[UidNeighborhoodAnalysis]:
        """批量分析uid邻域"""
        
        if sample_size and len(uids) > sample_size:
            import random
            uids = random.sample(uids, sample_size)
            print(f"随机采样 {sample_size} 个uid进行分析")
        
        print(f"开始分析 {len(uids)} 个uid的邻域特征...")
        
        analyses = []
        processed = 0
        
        for uid_key, is_blacklisted in uids:
            try:
                analysis = self.analyze_uid_neighborhood(session, uid_key, is_blacklisted)
                analyses.append(analysis)
                
                processed += 1
                if processed % 50 == 0:
                    print(f"  已处理 {processed}/{len(uids)} 个uid")
                    
            except Exception as e:
                print(f"  处理uid {uid_key} 时出错: {e}")
                continue
        
        print(f"  完成处理，分析了 {len(analyses)} 个uid")
        return analyses
    
    def generate_group_analysis(self, uid_analyses: List[UidNeighborhoodAnalysis], 
                               group_name: str) -> GroupAnalysisResult:
        """生成群体分析结果"""
        return GroupAnalysisResult(
            group_name=group_name,
            uid_count=len(uid_analyses),
            uid_analyses=uid_analyses
        )
    
    def print_group_analysis(self, group_result: GroupAnalysisResult):
        """打印群体分析结果"""
        print(f"\n{'='*80}")
        print(f"{group_result.group_name} 邻域分析结果")
        print(f"{'='*80}")
        print(f"分析uid数量: {group_result.uid_count}")
        
        # 如果是黑名单群体，显示孤立节点统计
        blacklist_count = sum(1 for analysis in group_result.uid_analyses if analysis.is_blacklisted)
        if blacklist_count > 0:
            print(f"\n{'*'*60}")
            print("孤立黑名单节点分析")
            print(f"{'*'*60}")
            
            isolation_stats = group_result.get_isolated_blacklist_stats()
            print(f"总黑名单uid数: {isolation_stats['total_blacklist_uids']}")
            print(f"孤立黑名单uid数: {isolation_stats['isolated_blacklist_uids']}")
            print(f"孤立率: {isolation_stats['isolation_rate']:.2%}")
            print(f"孤立阈值: ≤{isolation_stats['isolation_threshold']} 个异常节点")
            
            if isolation_stats['isolated_uid_list']:
                print(f"\n孤立黑名单uid详情 (前10个):")
                for i, isolated_uid in enumerate(isolation_stats['isolated_uid_list'][:10]):
                    hop_info = ', '.join([f"{hop}跳:{count}" for hop, count in isolated_uid['hop_breakdown'].items() if count > 0])
                    print(f"  {i+1}. {isolated_uid['uid_key']} (总计{isolated_uid['total_anomaly_nodes']}个: {hop_info})")
            
            if isolation_stats['non_isolated_stats']['count'] > 0:
                print(f"\n非孤立黑名单uid统计:")
                print(f"  数量: {isolation_stats['non_isolated_stats']['count']}")
                print(f"  平均异常节点数: {isolation_stats['non_isolated_stats']['avg_total_anomaly_nodes']:.2f}")
                print(f"  最大异常节点数: {isolation_stats['non_isolated_stats']['max_total_anomaly_nodes']}")
                print(f"  最小异常节点数: {isolation_stats['non_isolated_stats']['min_total_anomaly_nodes']}")
        
        # 按跳数分析
        avg_anomaly_by_hop = group_result.get_avg_anomaly_nodes_by_hop()
        type_dist_by_hop = group_result.get_type_distribution_by_hop()
        assoc_dist_by_hop = group_result.get_uid_association_distribution_by_hop()
        
        for hop in sorted(avg_anomaly_by_hop.keys()):
            print(f"\n{'-'*60}")
            print(f"{hop}-hop 分析:")
            print(f"{'-'*60}")
            
            # 平均异常节点数
            print(f"平均异常节点数: {avg_anomaly_by_hop[hop]:.2f}")
            
            # 异常节点类型分布
            print(f"\n异常节点类型分布:")
            if hop in type_dist_by_hop:
                for node_type, counts in type_dist_by_hop[hop].items():
                    if counts:  # 只显示有数据的类型
                        avg_count = statistics.mean(counts)
                        max_count = max(counts)
                        min_count = min(counts)
                        print(f"  {node_type}:")
                        print(f"    平均数量: {avg_count:.2f}")
                        print(f"    最大数量: {max_count}")
                        print(f"    最小数量: {min_count}")
                        print(f"    出现在 {len([c for c in counts if c > 0])}/{len(counts)} 个uid中")
            
            # uid关联数分布
            print(f"\n异常节点关联uid数量分布:")
            if hop in assoc_dist_by_hop:
                for node_type, assoc_lists in assoc_dist_by_hop[hop].items():
                    # 展平所有关联数
                    all_assoc_counts = []
                    for assoc_list in assoc_lists:
                        all_assoc_counts.extend(assoc_list)
                    
                    if all_assoc_counts:
                        print(f"  {node_type}:")
                        print(f"    平均关联uid数: {statistics.mean(all_assoc_counts):.2f}")
                        print(f"    中位数关联uid数: {statistics.median(all_assoc_counts):.1f}")
                        print(f"    最大关联uid数: {max(all_assoc_counts)}")
                        print(f"    最小关联uid数: {min(all_assoc_counts)}")
                        
                        # 关联数分布
                        assoc_counter = Counter(all_assoc_counts)
                        top_assoc = sorted(assoc_counter.items())[:10]
                        print(f"    关联数分布(前10): {top_assoc}")
    
    def compare_groups(self, blacklist_result: GroupAnalysisResult, normal_result: GroupAnalysisResult):
        """对比黑名单和正常uid群体"""
        print(f"\n{'='*80}")
        print("黑名单 vs 正常uid 对比分析")
        print(f"{'='*80}")
        
        # 孤立节点对比
        bl_isolation_stats = blacklist_result.get_isolated_blacklist_stats()
        normal_isolation_stats = normal_result.get_isolated_blacklist_stats()  # 这里实际上返回空结果，因为正常uid没有黑名单
        
        print(f"\n孤立节点对比:")
        print(f"{'指标':<25} {'黑名单uid':<15} {'说明':<30}")
        print(f"{'-'*70}")
        print(f"{'总黑名单uid数':<25} {bl_isolation_stats['total_blacklist_uids']:<15} {'仅统计黑名单uid':<30}")
        print(f"{'孤立黑名单uid数':<25} {bl_isolation_stats['isolated_blacklist_uids']:<15} {'≤2个异常节点的黑名单uid':<30}")
        print(f"{'孤立率':<25} {bl_isolation_stats['isolation_rate']:.2%}{'':<12} {'孤立黑名单占总黑名单比例':<30}")
        
        if bl_isolation_stats['non_isolated_stats']['count'] > 0:
            print(f"{'非孤立平均异常数':<25} {bl_isolation_stats['non_isolated_stats']['avg_total_anomaly_nodes']:.2f}{'':<12} {'非孤立黑名单的平均异常节点数':<30}")
        
        # 跳数对比
        bl_avg_by_hop = blacklist_result.get_avg_anomaly_nodes_by_hop()
        normal_avg_by_hop = normal_result.get_avg_anomaly_nodes_by_hop()
        
        print(f"\n跳数异常节点对比:")
        print(f"{'跳数':<10} {'黑名单平均':<15} {'正常平均':<15} {'差异倍数':<10}")
        print(f"{'-'*55}")
        
        for hop in sorted(set(bl_avg_by_hop.keys()) | set(normal_avg_by_hop.keys())):
            bl_avg = bl_avg_by_hop.get(hop, 0)
            normal_avg = normal_avg_by_hop.get(hop, 0)
            ratio = bl_avg / normal_avg if normal_avg > 0 else float('inf')
            
            print(f"{hop}-hop{'':<5} {bl_avg:<15.2f} {normal_avg:<15.2f} {ratio:<10.2f}")
        
        # 风险洞察
        print(f"\n风险洞察:")
        if bl_isolation_stats['isolation_rate'] > 0.3:
            print(f"⚠️  高孤立率警告: {bl_isolation_stats['isolation_rate']:.1%} 的黑名单uid是孤立的")
            print("   这可能表明存在大量单点欺诈行为")
        
        total_ratio = sum(bl_avg_by_hop.values()) / sum(normal_avg_by_hop.values()) if sum(normal_avg_by_hop.values()) > 0 else float('inf')
        if total_ratio > 2:
            print(f"⚠️  高关联风险: 黑名单uid的异常关联是正常uid的 {total_ratio:.1f} 倍")
            print("   建议加强关联度监控")
    
    def export_analysis_results(self, blacklist_result: GroupAnalysisResult, 
                               normal_result: GroupAnalysisResult,
                               filename: str = "neighborhood_analysis.json"):
        """导出分析结果"""
        
        def serialize_group_result(group_result: GroupAnalysisResult) -> Dict:
            return {
                "group_name": group_result.group_name,
                "uid_count": group_result.uid_count,
                "avg_anomaly_nodes_by_hop": group_result.get_avg_anomaly_nodes_by_hop(),
                "isolated_blacklist_stats": group_result.get_isolated_blacklist_stats(),
                "type_distribution_by_hop": {
                    str(hop): {
                        node_type: {
                            "counts": counts,
                            "avg": statistics.mean(counts) if counts else 0,
                            "max": max(counts) if counts else 0,
                            "min": min(counts) if counts else 0
                        }
                        for node_type, counts in type_dist.items()
                    }
                    for hop, type_dist in group_result.get_type_distribution_by_hop().items()
                },
                "uid_analyses": [
                    {
                        "uid_key": analysis.uid_key,
                        "is_blacklisted": analysis.is_blacklisted,
                        "total_anomaly_nodes": sum(hop_analysis.total_anomaly_nodes for hop_analysis in analysis.hop_analyses.values()),
                        "hop_analyses": {
                            str(hop): {
                                "total_anomaly_nodes": hop_analysis.total_anomaly_nodes,
                                "anomaly_stats_by_type": {
                                    node_type: {
                                        "count": stats.count,
                                        "uid_association_counts": stats.uid_association_counts
                                    }
                                    for node_type, stats in hop_analysis.anomaly_stats_by_type.items()
                                }
                            }
                            for hop, hop_analysis in analysis.hop_analyses.items()
                        }
                    }
                    for analysis in group_result.uid_analyses
                ]
            }
        
        export_data = {
            "analysis_config": {
                "max_k_hops": self.max_k_hops
            },
            "blacklist_analysis": serialize_group_result(blacklist_result),
            "normal_analysis": serialize_group_result(normal_result)
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n详细分析结果已导出到: {filename}")


def main():
    """主函数"""
    MAX_K_HOPS = 3
    SAMPLE_SIZE = 10000
    EXPORT_FILENAME = "neighborhood_analysis.json"

    analyzer = BlacklistAnalyzer(max_k_hops=MAX_K_HOPS)
    
    try:
        with analyzer.driver.session() as session:
            print(f"开始邻域异常节点分析...")
            print(f"配置: 最大跳数={MAX_K_HOPS}, 采样数={SAMPLE_SIZE}")
            
            # 获取所有uid
            all_uids = analyzer.get_all_uids(session, limit=SAMPLE_SIZE * 3)

            if not all_uids:
                print("❌ 未找到uid数据，请检查数据库")
                return
            
            # 分离黑名单和正常uid
            blacklist_uids = [(uid, is_bl) for uid, is_bl in all_uids if is_bl]
            normal_uids = [(uid, is_bl) for uid, is_bl in all_uids if not is_bl]
            
            print(f"找到 {len(blacklist_uids)} 个黑名单uid, {len(normal_uids)} 个正常uid")
            
            # 分析黑名单uid邻域
            blacklist_analyses = analyzer.batch_analyze_neighborhoods(
                session, blacklist_uids, sample_size=min(len(blacklist_uids), SAMPLE_SIZE // 2)
            )
            
            # 分析正常uid邻域
            normal_analyses = analyzer.batch_analyze_neighborhoods(
                session, normal_uids, sample_size=min(len(normal_uids), SAMPLE_SIZE // 2)
            )
            
            if not blacklist_analyses and not normal_analyses:
                print("❌ 未能分析任何uid")
                return
            
            # 生成群体分析结果
            blacklist_result = analyzer.generate_group_analysis(blacklist_analyses, "黑名单uid")
            normal_result = analyzer.generate_group_analysis(normal_analyses, "正常uid")
            
            # 打印分析结果
            if blacklist_analyses:
                # 临时修改孤立阈值
                original_method = blacklist_result.get_isolated_blacklist_stats
                blacklist_result.get_isolated_blacklist_stats = lambda threshold=2: original_method(threshold)
                analyzer.print_group_analysis(blacklist_result)
            
            if normal_analyses:
                analyzer.print_group_analysis(normal_result)
            
            # 对比分析
            if blacklist_analyses and normal_analyses:
                analyzer.compare_groups(blacklist_result, normal_result)
            
            # 导出详细结果
            analyzer.export_analysis_results(blacklist_result, normal_result, EXPORT_FILENAME)
            
            print(f"\n✅ 分析完成！")
            print(f"分析了 {len(blacklist_analyses)} 个黑名单uid, {len(normal_analyses)} 个正常uid")
            
            # 输出孤立统计摘要
            if blacklist_analyses:
                isolation_stats = blacklist_result.get_isolated_blacklist_stats(2)
                print(f"孤立黑名单uid: {isolation_stats['isolated_blacklist_uids']}/{isolation_stats['total_blacklist_uids']} ({isolation_stats['isolation_rate']:.1%})")
            
            print(f"结果已导出到: {EXPORT_FILENAME}")
    
    except Exception as e:
        print(f"❌ 分析过程中出现错误: {e}")
        raise
    
    finally:
        analyzer.close()


if __name__ == "__main__":
    main()
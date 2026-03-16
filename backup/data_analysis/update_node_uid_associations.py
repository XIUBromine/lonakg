"""
更新非uid节点的关联uid信息脚本 - 简化版

该脚本会遍历Neo4j数据库中的所有非uid节点，计算并更新它们与uid节点的关联信息。
简化设计，只保留核心的两个属性：

设计的属性：
- uid_associations: 每个uid的最新关联时间(JSON格式) 
- associated_uid_count: 关联的uid数量
"""

from __future__ import annotations

import json
from typing import Dict, List, Any
from dataclasses import dataclass

from neo4j import GraphDatabase, Session, Transaction
from dotenv import load_dotenv
import os

# 加载Neo4j连接信息
load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "123456")

# 配置参数
BATCH_SIZE = 1000

# 非uid节点类型列表（便于扩展）
NON_UID_NODE_LABELS = [
    "phone_num",
    "identity_no", 
    "card_no",
    "device_no",
    "td_device_id",
    "remote_ip",
    "geo_code"
]


@dataclass
class NodeAssociation:
    """节点关联信息数据类 - 简化版"""
    node_key: str
    node_label: str
    uid_associations: Dict[str, str]  # uid -> latest_time
    
    def to_update_params(self) -> Dict[str, Any]:
        """转换为Neo4j更新参数"""
        return {
            "uid_associations": json.dumps(self.uid_associations, ensure_ascii=False),
            "associated_uid_count": len(self.uid_associations)
        }


def update_all_nodes_efficiently(session: Session, label: str) -> int:
    """使用APOC高效更新指定类型的所有节点"""
    print(f"正在处理 {label} 节点...")
    
    # 使用APOC的单个查询处理所有节点
    query = f"""
    MATCH (n:{label})
    OPTIONAL MATCH (u:uid)-[r]->(n)
    WITH n, collect(DISTINCT {{
        uid: u.uid_key,
        time: CASE 
            WHEN r.event_time IS NOT NULL THEN toString(r.event_time)
            WHEN r.modify_time IS NOT NULL THEN toString(r.modify_time)
            WHEN r.create_time IS NOT NULL THEN toString(r.create_time)
            WHEN r.apply_time IS NOT NULL THEN toString(r.apply_time)
            WHEN r.sign_time IS NOT NULL THEN toString(r.sign_time)
            ELSE toString(datetime())
        END
    }}) as uid_data
    WHERE size([x IN uid_data WHERE x.uid IS NOT NULL]) > 0
    WITH n, [x IN uid_data WHERE x.uid IS NOT NULL] as valid_uid_data
    WITH n, apoc.map.fromPairs([x IN valid_uid_data | [x.uid, x.time]]) as uid_associations
    SET n.uid_associations = apoc.convert.toJson(uid_associations),
        n.associated_uid_count = size(keys(uid_associations))
    RETURN count(n) as updated_count
    """
    
    result = session.run(query)
    updated_count = result.single()["updated_count"]
    print(f"更新了 {updated_count} 个 {label} 节点")
    
    return updated_count


def clear_existing_properties(session: Session) -> None:
    """清理已有的关联属性（可选，用于重新计算）"""
    print("正在清理现有的关联属性...")
    
    properties_to_clear = ["uid_associations", "associated_uid_count"]
    
    for label in NON_UID_NODE_LABELS:
        for prop in properties_to_clear:
            query = f"MATCH (n:{label}) REMOVE n.{prop}"
            try:
                session.run(query)
            except Exception as e:
                print(f"清理 {label}.{prop} 时出错: {e}")
    
    print("属性清理完成")


def main(clear_properties: bool = False) -> None:
    """主函数"""
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    try:
        with driver.session() as session:
            if clear_properties:
                clear_existing_properties(session)
            
            total_updated = 0
            
            # 逐个处理每种节点类型
            for label in NON_UID_NODE_LABELS:
                try:
                    updated_count = update_all_nodes_efficiently(session, label)
                    total_updated += updated_count
                except Exception as e:
                    print(f"处理 {label} 节点时出错: {e}")
                    continue
            
            print(f"\n✅ 所有节点处理完成！")
            print(f"总共更新了 {total_updated} 个节点")
    
    except Exception as e:
        print(f"❌ 处理过程中出现错误: {e}")
        raise
    
    finally:
        driver.close()


def get_statistics(session: Session) -> None:
    """获取更新后的统计信息"""
    print("\n=== 更新统计信息 ===")
    
    for label in NON_UID_NODE_LABELS:
        # 统计该类型节点的关联情况
        query = f"""
        MATCH (n:{label})
        WHERE n.associated_uid_count IS NOT NULL
        RETURN 
            count(n) as total_nodes,
            avg(n.associated_uid_count) as avg_uid_count,
            max(n.associated_uid_count) as max_uid_count,
            min(n.associated_uid_count) as min_uid_count
        """
        
        result = session.run(query)
        stats = result.single()
        
        if stats and stats["total_nodes"] > 0:
            print(f"\n{label} 节点统计:")
            print(f"  - 有关联的节点数: {stats['total_nodes']}")
            print(f"  - 平均关联uid数: {stats['avg_uid_count']:.2f}")
            print(f"  - 最大关联uid数: {stats['max_uid_count']}")
            print(f"  - 最小关联uid数: {stats['min_uid_count']}")


def demo_query_usage(session: Session) -> None:
    """演示如何使用更新后的属性进行查询"""
    print("\n=== 查询使用示例 ===")
    
    # 示例1：查找关联最多uid的phone_num
    print("\n1. 关联最多uid的phone_num节点:")
    query1 = """
    MATCH (p:phone_num)
    WHERE p.associated_uid_count IS NOT NULL
    RETURN p.key, p.associated_uid_count, p.uid_associations
    ORDER BY p.associated_uid_count DESC
    LIMIT 3
    """
    
    result1 = session.run(query1)
    for record in result1:
        print(f"  手机号: {record['p.key'][:10]}... | 关联{record['p.associated_uid_count']}个uid")
        # 解析JSON展示部分关联信息
        uid_assocs = json.loads(record['p.uid_associations'])
        sample_uids = list(uid_assocs.keys())[:2]
        for uid in sample_uids:
            print(f"    - {uid}: {uid_assocs[uid]}")
    
    # 示例2：查找特定uid关联的所有节点
    print("\n2. 查找某个uid关联的所有节点:")
    sample_uid_query = "MATCH (u:uid) RETURN u.uid_key LIMIT 1"
    sample_result = session.run(sample_uid_query)
    sample_record = sample_result.single()
    
    if sample_record:
        sample_uid = sample_record["u.uid_key"]
        print(f"  查询UID: {sample_uid}")
        
        for label in NON_UID_NODE_LABELS[:3]:  # 只查询前3种类型
            query2 = f"""
            MATCH (n:{label})
            WHERE n.uid_associations CONTAINS $uid
            RETURN n.key, n.associated_uid_count
            LIMIT 2
            """
            
            result2 = session.run(query2, uid=sample_uid)
            nodes = list(result2)
            if nodes:
                print(f"    {label}: {len(nodes)}个节点")
                for node in nodes:
                    print(f"      - {node['n.key'][:15]}... (共关联{node['n.associated_uid_count']}个uid)")
    
    # 示例3：统计各类型节点的关联分布
    print("\n3. 各类型节点关联分布:")
    for label in NON_UID_NODE_LABELS:
        query3 = f"""
        MATCH (n:{label})
        WHERE n.associated_uid_count IS NOT NULL
        WITH n.associated_uid_count as uid_count, count(n) as node_count
        RETURN uid_count, node_count
        ORDER BY uid_count
        LIMIT 5
        """
        
        result3 = session.run(query3)
        distributions = list(result3)
        if distributions:
            print(f"  {label}:")
            for dist in distributions:
                print(f"    关联{dist['uid_count']}个uid: {dist['node_count']}个节点")


if __name__ == "__main__":
    CLEAR = False   # 清理现有的关联属性（重新计算）
    STATS_ONLY = False  # 只显示统计信息，不执行更新
    DEMO = False    # 演示查询使用方法

    if STATS_ONLY or DEMO:
        # 显示统计信息或演示
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        try:
            with driver.session() as session:
                if STATS_ONLY:
                    get_statistics(session)
                if DEMO:
                    demo_query_usage(session)
        finally:
            driver.close()
    else:
        # 执行更新
        main(clear_properties=CLEAR)
        
        # 更新完成后显示统计信息
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        try:
            with driver.session() as session:
                get_statistics(session)
                demo_query_usage(session)
        finally:
            driver.close()
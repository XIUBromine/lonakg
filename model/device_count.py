import os
from pathlib import Path
from neo4j import GraphDatabase

dir_model = Path(__file__).parent
output_dir = dir_model / "output"
output_file = output_dir / "device_count.txt"

BATCH_SIZE = 10000

def get_device_user_counts(session, device_keys: list) -> dict:
    """查询指定列表中每个设备节点关联的用户 (uid) 数量"""
    if not device_keys:
        return {}
    query = """
    MATCH (u:uid)-[]-(d:device_no)
    WHERE d.key IN $device_keys
    RETURN d.key AS device_key, count(DISTINCT u) AS user_count
    """
    result = session.run(query, device_keys=device_keys)
    return {record["device_key"]: record["user_count"] for record in result}

def main():
    # 确保输出目录存在
    output_dir.mkdir(parents=True, exist_ok=True)

    # 连接配置
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD")
    database = os.getenv("NEO4J_DATABASE", "prod")

    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        print("开始边获取边处理设备节点...")
        
        # 获取总数以便显示进度 (Neo4j 对标签的 count 是利用统计信息的，速度极快)
        with driver.session(database=database) as count_session:
            total_count = count_session.run("MATCH (d:device_no) RETURN count(d) AS total").single()["total"]
            print(f"发现共有 {total_count} 个设备节点，即将开始批量抽取关联数据...")
            
        # 使用一个独立的 session 流式读取设备 key，避免被其他查询中断游标
        with driver.session(database=database) as read_session:
            query = "MATCH (d:device_no) RETURN d.key AS device_key"
            result = read_session.run(query)
            
            with open(output_file, "w", encoding="utf-8") as f_out:
                batch_keys = []
                processed = 0
                
                # 使用另一个 session 来查询计数，这样它们不会互相干扰
                with driver.session(database=database) as query_session:
                    for record in result:
                        batch_keys.append(record["device_key"])
                        
                        if len(batch_keys) >= BATCH_SIZE:
                            counts_dict = get_device_user_counts(query_session, batch_keys)
                            
                            for d_key in batch_keys:
                                count = counts_dict.get(d_key, 0)
                                f_out.write(f'"{d_key}"\t{count}\n')
                            
                            f_out.flush() # 实时将写入的数据刷盘
                            processed += len(batch_keys)
                            print(f"进度: {processed} / {total_count} 个设备节点已处理 ({(processed / total_count) * 100:.2f}%)")
                            batch_keys = []
                    
                    # 循环结束后处理最后不足一批的数据
                    if batch_keys:
                        counts_dict = get_device_user_counts(query_session, batch_keys)
                        for d_key in batch_keys:
                            count = counts_dict.get(d_key, 0)
                            f_out.write(f'"{d_key}"\t{count}\n')
                        
                        f_out.flush()
                        processed += len(batch_keys)
                        print(f"进度: {processed} / {total_count} 个设备节点已处理 ({(processed / total_count) * 100:.2f}%)")

        print("处理全部完成！")

    finally:
        driver.close()

if __name__ == "__main__":
    main()

import os
from pathlib import Path
from neo4j import GraphDatabase

dir_model = Path(__file__).parent
input_file = dir_model / "output/forecast.txt"
output_file = dir_model / "output/forecast1.txt"

TARGET_DEVICE_NO = "f67f4b0a2bc2636f24a5be7eb6ffa719"

BATCH_SIZE = 20

def get_uids_with_device(session, uids: list, target_device: str) -> set:
    """
    批量检查一组 uid 节点是否通过1跳直接关系与目标设备节点(device_no)相连。
    """
    if not uids:
        return set()
    query = """
    MATCH (u:uid)-[]-(d:device_no {key: $target_device})
    WHERE u.key IN $uids
    RETURN DISTINCT u.key AS uid
    """
    result = session.run(query, uids=uids, target_device=target_device)
    return {record["uid"] for record in result}

def main():
    if not input_file.exists():
        print(f"输入文件未找到: {input_file}")
        return

    # 连接配置参考 batch_risk_calculation.py
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD")
    database = os.getenv("NEO4J_DATABASE", "prod")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    
    total_processed = 0
    kept_count = 0
    
    try:
        with driver.session(database=database) as session:
            with open(input_file, "r", encoding="utf-8") as f_in:
                lines = [line.strip() for line in f_in if line.strip()]
            
            total = len(lines)
            print(f"共读取到 {total} 条数据，开始进行批量过滤...")
            
            with open(output_file, "w", encoding="utf-8") as f_out:
                for i in range(0, total, BATCH_SIZE):
                    batch_lines = lines[i:i + BATCH_SIZE]
                    batch_data = []
                    batch_uids = []
                    
                    for line in batch_lines:
                        parts = line.split()
                        if len(parts) >= 2:
                            uid = parts[0]
                            risk = parts[1]
                            batch_data.append((uid, risk))
                            batch_uids.append(uid)
                    
                    total_processed += len(batch_uids)
                    
                    # 批量检查这批 uid 中哪些与目标设备直接（1跳）相连
                    uids_with_device = get_uids_with_device(session, batch_uids, TARGET_DEVICE_NO)
                    
                    # 把没有关联到指定设备的uid保留并输出
                    for uid, risk in batch_data:
                        if uid not in uids_with_device:
                            f_out.write(f"{uid}\t{risk}\n")
                            kept_count += 1
                            
                    print(f"进度: {min(i + BATCH_SIZE, total)} / {total} 条数据已处理")
                            
        print(f"过滤完成，原记录数: {total_processed}，过滤后保留记录数: {kept_count}")
        print(f"结果已保存到: {output_file}")

    finally:
        driver.close()

if __name__ == "__main__":
    main()

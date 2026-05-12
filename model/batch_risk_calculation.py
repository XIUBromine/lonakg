import os
import sys
from pathlib import Path
from rule_based_risk import RuleBasedKHopRiskEngine

# 输入和输出文件路径
dir_model = Path(__file__).parent
uids_file = dir_model / "output/uids_repaying.txt"
output_file = dir_model / "output/uids_repaying_risks.txt"


# 每批处理数量
BATCH_SIZE = 100

# 可选：从第几条开始处理（0-based）
def parse_start_index():
    import argparse
    parser = argparse.ArgumentParser(description="Batch risk calculation with optional start index.")
    parser.add_argument('--start', type=int, default=0, help='Start index (0-based) in the uids file')
    args = parser.parse_args()
    return args.start

def read_uids(uids_path):
    with open(uids_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def batch_write(results, out_path, mode="a"):
    with open(out_path, mode, encoding="utf-8") as f:
        for uid, risk in results:
            f.write(f"{uid}\t{risk}\n")


def main():
    start_index = parse_start_index()
    uids = read_uids(uids_file)
    total = len(uids)
    print(f"Total uids: {total}")
    if start_index >= total:
        print(f"Start index {start_index} >= total uids {total}, nothing to process.")
        return

    # 连接参数可根据实际情况调整
    engine = RuleBasedKHopRiskEngine(
        uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        user=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD"),
        database=os.getenv("NEO4J_DATABASE", "prod"),
    )
    try:
        for i in range(start_index, total, BATCH_SIZE):
            batch = uids[i:i+BATCH_SIZE]
            results = []
            for uid in batch:
                try:
                    risk = engine.compute_risk(node_type="uid", node_key=uid, k=2)
                except Exception as e:
                    risk = None
                results.append((uid, risk))
            batch_write(results, output_file, mode="a")
            print(f"Processed {min(i+BATCH_SIZE, total)}/{total}")
    finally:
        engine.close()

if __name__ == "__main__":
    main()

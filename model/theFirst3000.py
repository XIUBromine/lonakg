import os
import heapq

def extract_top_risks(input_file, output_file, max_lines_to_read=180000, top_k=3000, batch_size=1000):
    """
    读取文件，去重后统计风险值前K大的数据并输出
    """
    uid_risk_map = {}
    lines_processed = 0

    print(f"开始读取文件: {input_file}")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split()
                if len(parts) >= 2:
                    uid = parts[0]
                    try:
                        risk_score = float(parts[1])
                        # 去重策略：如果出现相同 UID，保留风险值最高的那一条
                        if uid not in uid_risk_map or risk_score > uid_risk_map[uid]:
                            uid_risk_map[uid] = risk_score
                    except ValueError:
                        pass
                        
                lines_processed += 1
                
                # 进度显示 (取代手动batch)
                if lines_processed % batch_size == 0:
                    print(f"已处理 {lines_processed} 条数据...")

                # 达到最大读取行数时停止
                if lines_processed >= max_lines_to_read:
                    break

        print(f"读取完毕，共处理 {lines_processed} 条数据。去重后共有 {len(uid_risk_map)} 个独立用户。")

        # 使用 heapq 优化查找前 K 个最大值的性能 (O(N log K) 代替 O(N log N))
        print(f"正在提取风险值最高的前 {top_k} 条数据...")
        top_records = heapq.nlargest(top_k, uid_risk_map.items(), key=lambda x: x[1])

        print(f"开始写入结果到文件: {output_file}")
        # 确保输出目录存在，增加对空路径的容错
        out_dir = os.path.dirname(output_file)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

        with open(output_file, 'w', encoding='utf-8') as f_out:
            for uid, risk in top_records:
                f_out.write(f"{uid}\t{risk}\n")

        print(f"处理完成！结果已保存至 {output_file}")
        
    except FileNotFoundError:
        print(f"错误: 找不到输入文件 {input_file}")
    except PermissionError:
        print(f"错误: 权限不足，无法读取或写入文件。")
    except Exception as e:
        print(f"处理时发生错误: {e}")

if __name__ == "__main__":
    # 获取当前脚本所在目录的绝对路径 (.../model/)
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # 配置参数（可修改）
    INPUT_FILE = os.path.join(BASE_DIR, "output", "uids_repaying_risks.txt")
    OUTPUT_FILE = os.path.join(BASE_DIR, "output", "uids_repaying_risks_first3000.txt")
    MAX_LINES_TO_READ = 180000  # 读取的最大条数
    TOP_K = 3000                # 提取前多少条
    BATCH_SIZE = 10000           # 进度显示的步长
    
    extract_top_risks(
        input_file=INPUT_FILE, 
        output_file=OUTPUT_FILE, 
        max_lines_to_read=MAX_LINES_TO_READ, 
        top_k=TOP_K,
        batch_size=BATCH_SIZE
    )

import os
from pathlib import Path
import heapq

dir_model = Path(__file__).parent
input_file = dir_model / "output/device_count.txt"
output_file = dir_model / "output/device_count_sort.txt"

BATCH_SIZE = 5000
TOP_N = 10000

def main():
    if not input_file.exists():
        print(f"输入文件未找到: {input_file}")
        return

    # A: 使用文件大小估算进度，避免 sum(1 for line in f_in) 带来的全表扫描
    total_size = os.path.getsize(input_file)
    print(f"文件大小约为 {total_size / (1024 * 1024):.2f} MB，开始边读边去重...")

    # C: 内存风险 - 我们依然需要用数据字典去重，但在海量数据时，提取 Top N 用 heapq 会大幅度节省内存和排序开销
    device_data = {}
    
    processed_lines = 0
    with open(input_file, "r", encoding="utf-8") as f_in:
        # 使用 readline 循环，避免 "for line in f_in" 触发 next() 后 tell() 不可用
        while True:
            line = f_in.readline()
            if not line:
                break
            processed_lines += 1
            line = line.strip()
            if not line:
                continue
            parts = line.split('\t')
            if len(parts) >= 2:
                d_key = parts[0]
                try:
                    count = int(parts[1])
                    # 去重逻辑：如果已存在，则保留较大的关联用户数
                    if d_key in device_data:
                        if count > device_data[d_key]:
                            device_data[d_key] = count
                    else:
                        device_data[d_key] = count
                except ValueError:
                    pass
            
            # 定期打印进度
            if processed_lines % BATCH_SIZE == 0:
                current_pos = f_in.tell()
                print(f"进度: 已处理 {processed_lines} 行 (约 {(current_pos / total_size) * 100:.2f}%)")
        
        # 最后的进度展示
        current_pos = f_in.tell()
        print(f"进度: 已处理 {processed_lines} 行 (约 {(current_pos / total_size) * 100:.2f}%)")

    print(f"去重后共剩余 {len(device_data)} 个唯一设备节点。")
    print(f"开始使用堆（heapq）提取前 {TOP_N} 个最大关联用户数的设备节点...")
    
    # C: 堆排序提取前 10000，不需要对所有的数据进行全量倒叙排序 (O(N) 性能强于 O(N log N))
    top_devices = heapq.nlargest(TOP_N, device_data.items(), key=lambda x: x[1])
    
    print(f"提取完成，正在将前 {len(top_devices)} 个数据输出到文件: {output_file} ...")
    
    # 为了保证输出目录存在
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, "w", encoding="utf-8") as f_out:
        for d_key, count in top_devices:
            # 这里的 d_key 来源于文件读取，原来带有引号则会原样保留，格式依然是 `key\tcount`
            f_out.write(f'{d_key}\t{count}\n')
            
    print("全部处理并输出完成！")

if __name__ == "__main__":
    main()

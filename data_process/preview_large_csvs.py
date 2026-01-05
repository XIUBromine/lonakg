import os
import pandas as pd

# 目标目录
DATA_DIR = '/data/aiimport_1119'

# 获取所有csv文件
csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]

print(f"在{DATA_DIR}下找到{len(csv_files)}个csv文件：")
for f in csv_files:
    print(f"  - {f}")

# 设置pandas显示所有列
pd.set_option('display.max_columns', None)
# 依次读取每个csv文件的前10行
for csv_file in csv_files:
    file_path = os.path.join(DATA_DIR, csv_file)
    print(f"\n==== {csv_file} 前10行 ====")
    try:
        df = pd.read_csv(file_path, nrows=10)
        print(df)
    except Exception as e:
        print(f"读取{csv_file}失败: {e}")

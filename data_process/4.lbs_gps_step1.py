# import os
# import pandas as pd
# from pathlib import Path

# # 输入输出路径
# INPUT_FILE = '/data/aiimport_1119/lbs_gps信息.csv'
# OUTPUT_DIR = '/data/processed/lbs_gps'

# # 确保输出目录存在
# Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# # 读取前10000行
# usecols = ['cid', 'geo_code', 'create_date']
# df = pd.read_csv(INPUT_FILE, usecols=usecols, nrows=10000)

# # 处理create_date为datetime
# if df['create_date'].dtype != 'datetime64[ns]':
#     df['create_date'] = pd.to_datetime(df['create_date'], errors='coerce')

# # 统计无效日期数量
# invalid_dates = df['create_date'].isna().sum()
# print(f"无效日期（异常数据）数量: {invalid_dates}")

# # 按create_date排序，去除无效日期
# df = df.dropna(subset=['create_date'])
# df = df.sort_values('create_date')

# # 按月分组并保存
# for (year, month), group in df.groupby([df['create_date'].dt.year, df['create_date'].dt.month]):
#     out_name = f"{year:04d}-{month:02d}.csv"
#     out_path = os.path.join(OUTPUT_DIR, out_name)
#     group.to_csv(out_path, index=False)
#     print(f"保存: {out_path}，共{len(group)}条记录")


import os
import pandas as pd
from pathlib import Path

# 输入输出路径
INPUT_FILE = '/data/aiimport_1119/lbs_gps信息.csv'
OUTPUT_DIR = '/data/processed_v3/lbs_gps'

# 确保输出目录存在
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

usecols = ['cid', 'geo_code', 'create_date']
# df = pd.read_csv(INPUT_FILE, usecols=usecols)
df = pd.read_csv(INPUT_FILE)

# 处理create_date为datetime
if df['create_date'].dtype != 'datetime64[ns]':
    df['create_date'] = pd.to_datetime(df['create_date'], errors='coerce')

# 统计无效日期数量
invalid_dates = df['create_date'].isna().sum()
print(f"无效日期（异常数据）数量: {invalid_dates}")

# 按create_date排序，去除无效日期
df = df.dropna(subset=['create_date'])
df = df.sort_values('create_date')

# 👉 按 年-月-日 分组
for (year, month, day), group in df.groupby([
    df['create_date'].dt.year,
    df['create_date'].dt.month,
    df['create_date'].dt.day
]):
    
    # 👉 月份文件夹（例如 2023-01）
    month_dir = os.path.join(OUTPUT_DIR, f"{year:04d}-{month:02d}")
    Path(month_dir).mkdir(parents=True, exist_ok=True)

    # 👉 每天一个文件
    file_name = f"{year:04d}-{month:02d}-{day:02d}.csv"
    out_path = os.path.join(month_dir, file_name)

    group.to_csv(out_path, index=False)

    print(f"保存: {out_path}，共{len(group)}条记录")

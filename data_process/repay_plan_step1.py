# import os
# import pandas as pd
# from pathlib import Path

# # 输入输出路径
# INPUT_FILE = '/data/aiimport_1119/还款计划.csv'
# OUTPUT_DIR = '/data/processed/repay_plan'

# # 确保输出目录存在
# Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# # 保留的列（去掉 id 和 create_date）
# usecols = [
#     'id', 'order_id', 'contract_no', 'current_repay_date', 'repayed_date',
#     'early_repay_mark', 'overdue_mark', 'overdue_days', 'current_repay_status', 'modify_date'
# ]

# # 读取 CSV
# try:
#     df = pd.read_csv(INPUT_FILE, usecols=usecols, engine='python')
# except Exception as e:
#     print("读取 CSV 失败:", e)
#     raise

# # 处理 modify_date 为 datetime
# if df['modify_date'].dtype != 'datetime64[ns]':
#     df['modify_date'] = pd.to_datetime(df['modify_date'], errors='coerce')

# # 处理 current_repay_date 和 repayed_date 为 datetime
# if df['current_repay_date'].dtype != 'datetime64[ns]':
#     df['current_repay_date'] = pd.to_datetime(df['current_repay_date'], errors='coerce')
# if df['repayed_date'].dtype != 'datetime64[ns]':
#     df['repayed_date'] = pd.to_datetime(df['repayed_date'], errors='coerce')

# # 统计无效 modify_date 数量
# invalid_dates = df['modify_date'].isna().sum()
# print(f"无效 modify_date（异常数据）数量: {invalid_dates}")

# # 去除无效 modify_date 并按 modify_date 排序
# df = df.dropna(subset=['modify_date'])
# df = df.sort_values('modify_date')

# # 按月份拆分保存
# for (year, month), group in df.groupby([df['modify_date'].dt.year, df['modify_date'].dt.month]):
#     out_name = f"{year:04d}-{month:02d}.csv"
#     out_path = os.path.join(OUTPUT_DIR, out_name)
#     group.to_csv(out_path, index=False)
#     print(f"保存: {out_path}，共{len(group)}条记录")


import os
import pandas as pd
from pathlib import Path

# 输入输出路径
INPUT_FILE = '/data/aiimport_1119/还款计划.csv'
OUTPUT_DIR = '/data/processed_v3/repay_plan'

# 确保输出目录存在
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# 保留的列（去掉 id 和 create_date）
usecols = [
    'id', 'order_id', 'contract_no', 'current_repay_date', 'repayed_date',
    'early_repay_mark', 'overdue_mark', 'overdue_days', 'current_repay_status', 'modify_date'
]

# 读取 CSV
try:
    # df = pd.read_csv(INPUT_FILE, usecols=usecols, engine='python')
    df = pd.read_csv(INPUT_FILE, engine='python')
except Exception as e:
    print("读取 CSV 失败:", e)
    raise

# 处理 modify_date 为 datetime
if df['modify_date'].dtype != 'datetime64[ns]':
    df['modify_date'] = pd.to_datetime(df['modify_date'], errors='coerce')

# 处理 current_repay_date 和 repayed_date 为 datetime
if df['current_repay_date'].dtype != 'datetime64[ns]':
    df['current_repay_date'] = pd.to_datetime(df['current_repay_date'], errors='coerce')
if df['repayed_date'].dtype != 'datetime64[ns]':
    df['repayed_date'] = pd.to_datetime(df['repayed_date'], errors='coerce')

# 统计无效 modify_date 数量
invalid_dates = df['modify_date'].isna().sum()
print(f"无效 modify_date（异常数据）数量: {invalid_dates}")

# 去除无效 modify_date 并按 modify_date 排序
df = df.dropna(subset=['modify_date'])
df = df.sort_values('modify_date')

# # 👉 按 年-月-日 分组
for (year, month, day), group in df.groupby([
    df['modify_date'].dt.year,
    df['modify_date'].dt.month,
    df['modify_date'].dt.day
]):
    
    # 👉 月份文件夹（例如 2023-01）
    month_dir = os.path.join(OUTPUT_DIR, f"{year:04d}-{month:02d}")
    Path(month_dir).mkdir(parents=True, exist_ok=True)

    # 👉 每天一个文件
    file_name = f"{year:04d}-{month:02d}-{day:02d}.csv"
    out_path = os.path.join(month_dir, file_name)

    group.to_csv(out_path, index=False)

    print(f"保存: {out_path}，共{len(group)}条记录")
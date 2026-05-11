# import os
# from pathlib import Path
# import pandas as pd

# # 目录配置
# RAW_ROOT = '/data/aiimport_1119'
# PROCESSED_ROOT = '/data/processed'
# OUTPUT_DIR = os.path.join(PROCESSED_ROOT, 'blacklist_log')
# Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# INPUT_FILE = os.path.join(RAW_ROOT, '黑名单log.csv')

# # 要保留的列
# usecols = [
#     'c_customer_no',
#     'c_reason_list',
#     'c_mobile_phone',
#     'c_identity_no',
#     'd_begin_date',
#     'n_banned_days',
#     'n_duration',
# ]

# # 读取 CSV
# df = pd.read_csv(INPUT_FILE, usecols=usecols)

# # 处理生效日期为 datetime
# df['d_begin_date'] = pd.to_datetime(df['d_begin_date'], errors='coerce')

# # 丢弃异常日期
# invalid_count = df['d_begin_date'].isna().sum()
# print(f'无效日期（异常数据）数量: {invalid_count}')
# df = df.dropna(subset=['d_begin_date'])

# # 排序
# df = df.sort_values('d_begin_date')

# # 按月份拆分保存
# for ym, group in df.groupby(df['d_begin_date'].dt.to_period('M')):
#     out_path = os.path.join(OUTPUT_DIR, f'{ym}.csv')
#     group.to_csv(out_path, index=False, encoding='utf-8')
#     print(f'保存: {out_path}，共 {len(group)} 条记录')


import os
from pathlib import Path
import pandas as pd

# 目录配置
RAW_ROOT = '/data/aiimport_1119'
PROCESSED_ROOT = '/data/processed_v3'
OUTPUT_DIR = os.path.join(PROCESSED_ROOT, 'blacklist_log')
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

INPUT_FILE = os.path.join(RAW_ROOT, '黑名单log.csv')

# 要保留的列
usecols = [
    'c_customer_no',
    'c_reason_list',
    'c_mobile_phone',
    'c_identity_no',
    'd_begin_date',
    'n_banned_days',
    'n_duration',
]

# 读取 CSV
# df = pd.read_csv(INPUT_FILE, usecols=usecols)
df = pd.read_csv(INPUT_FILE)

# 处理生效日期为 datetime
df['d_begin_date'] = pd.to_datetime(df['d_begin_date'], errors='coerce')

# 丢弃异常日期
invalid_count = df['d_begin_date'].isna().sum()
print(f'无效日期（异常数据）数量: {invalid_count}')
df = df.dropna(subset=['d_begin_date'])

# 排序
df = df.sort_values('d_begin_date')

# 👉 按 年-月-日 分组
for (year, month, day), group in df.groupby([
    df['d_begin_date'].dt.year,
    df['d_begin_date'].dt.month,
    df['d_begin_date'].dt.day
]):
    
    # 👉 月份文件夹（例如 2023-01）
    month_dir = os.path.join(OUTPUT_DIR, f"{year:04d}-{month:02d}")
    Path(month_dir).mkdir(parents=True, exist_ok=True)

    # 👉 每天一个文件
    file_name = f"{year:04d}-{month:02d}-{day:02d}.csv"
    out_path = os.path.join(month_dir, file_name)

    group.to_csv(out_path, index=False)

    print(f"保存: {out_path}，共{len(group)}条记录")
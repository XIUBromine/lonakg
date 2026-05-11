# import os
# import pandas as pd
# from pathlib import Path

# # 输入输出路径
# INPUT_FILE = '/data/aiimport_1119/消保案件工单.csv'
# OUTPUT_DIR = '/data/processed/consumer_protection_case'

# # 确保输出目录存在
# Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# # 读取需要的列
# usecols = [
#     'c_id',
#     'c_contact_phone_no',
#     'c_register_phone_no',
#     'c_customer_name',
#     'c_identity_no',
#     'd_update'
# ]
# df = pd.read_csv(INPUT_FILE, usecols=usecols)

# # 处理 d_update 为 datetime
# df['d_update'] = pd.to_datetime(df['d_update'], errors='coerce')

# # 统计无效日期数量
# invalid_dates = df['d_update'].isna().sum()
# print(f"无效日期（异常数据）数量: {invalid_dates}")

# # 去除无效日期并按 d_update 排序
# df = df.dropna(subset=['d_update'])
# df = df.sort_values('d_update')

# # 按月分组并保存
# for (year, month), group in df.groupby([df['d_update'].dt.year, df['d_update'].dt.month]):
#     out_name = f"{year:04d}-{month:02d}.csv"
#     out_path = os.path.join(OUTPUT_DIR, out_name)
#     group.to_csv(out_path, index=False)
#     print(f"保存: {out_path}，共{len(group)}条记录")


import os
import pandas as pd
from pathlib import Path

# 输入输出路径
INPUT_FILE = '/data/aiimport_1119/消保案件工单.csv'
OUTPUT_DIR = '/data/processed_v3/consumer_protection_case'

# 确保输出目录存在
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# 读取需要的列
usecols = [
    'c_id',
    'c_contact_phone_no',
    'c_register_phone_no',
    'c_customer_name',
    'c_identity_no',
    'd_update'
]
# df = pd.read_csv(INPUT_FILE, usecols=usecols)
df = pd.read_csv(INPUT_FILE)

# 处理 d_update 为 datetime
df['d_update'] = pd.to_datetime(df['d_update'], errors='coerce')

# 统计无效日期数量
invalid_dates = df['d_update'].isna().sum()
print(f"无效日期（异常数据）数量: {invalid_dates}")

# 去除无效日期并按 d_update 排序
df = df.dropna(subset=['d_update'])
df = df.sort_values('d_update')

# 👉 按 年-月-日 分组
for (year, month, day), group in df.groupby([
    df['d_update'].dt.year,
    df['d_update'].dt.month,
    df['d_update'].dt.day
]):
    
    # 👉 月份文件夹（例如 2023-01）
    month_dir = os.path.join(OUTPUT_DIR, f"{year:04d}-{month:02d}")
    Path(month_dir).mkdir(parents=True, exist_ok=True)

    # 👉 每天一个文件
    file_name = f"{year:04d}-{month:02d}-{day:02d}.csv"
    out_path = os.path.join(month_dir, file_name)

    group.to_csv(out_path, index=False)

    print(f"保存: {out_path}，共{len(group)}条记录")
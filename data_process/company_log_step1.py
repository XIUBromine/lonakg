import os
import pandas as pd
from pathlib import Path

# 输入输出路径
INPUT_FILE = '/data/aiimport_1119/公司信息log.csv'
OUTPUT_DIR = '/data/processed_v3/company_log'

# 确保输出目录存在
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# 读取全量数据，保留关键列
usecols = ['cid', 'company_name', 'company_address', 'tel_phone', 'modify_date']
# df = pd.read_csv(INPUT_FILE, usecols=usecols)
df = pd.read_csv(INPUT_FILE)

# 处理 modify_date 为 datetime
if df['modify_date'].dtype != 'datetime64[ns]':
    df['modify_date'] = pd.to_datetime(df['modify_date'], errors='coerce')

# 统计无效日期数量
invalid_dates = df['modify_date'].isna().sum()
print(f"无效日期（异常数据）数量: {invalid_dates}")

# 去除无效日期并按 modify_date 排序
df = df.dropna(subset=['modify_date'])
df = df.sort_values('modify_date')

# 👉 按 年-月-日 分组
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
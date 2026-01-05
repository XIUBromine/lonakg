import os
import pandas as pd
from pathlib import Path

# 输入输出路径
INPUT_FILE = '/data/aiimport_1119/客户信息log.csv'
OUTPUT_DIR = '/data/processed/customer_log'

# 确保输出目录存在
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# 读取全量数据
usecols = ['baseid', 'identity_no', 'mobile_phone', 'log_create_date']
df = pd.read_csv(INPUT_FILE, usecols=usecols)

# 处理log_create_date为datetime
# 兼容带时间和不带时间的情况
if df['log_create_date'].dtype != 'datetime64[ns]':
    df['log_create_date'] = pd.to_datetime(df['log_create_date'], errors='coerce')

# 统计无效日期数量
invalid_dates = df['log_create_date'].isna().sum()
print(f"无效日期（异常数据）数量: {invalid_dates}")

# 按log_create_date排序
# 去除无效日期
df = df.dropna(subset=['log_create_date'])
df = df.sort_values('log_create_date')

# 按月分组并保存
for (year, month), group in df.groupby([df['log_create_date'].dt.year, df['log_create_date'].dt.month]):
    out_name = f"{year:04d}-{month:02d}.csv"
    out_path = os.path.join(OUTPUT_DIR, out_name)
    group.to_csv(out_path, index=False)
    print(f"保存: {out_path}，共{len(group)}条记录")

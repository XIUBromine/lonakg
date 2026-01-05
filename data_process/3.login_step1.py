import os
import pandas as pd
from pathlib import Path

# 输入输出路径
INPUT_FILE = '/data/aiimport_1119/登录信息.csv'
OUTPUT_DIR = '/data/processed/login'

# 确保输出目录存在
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# 读取前10000行
usecols = [
    'phone_num', 'cif_user_id', 'login_time', 'device_no', 'remote_ip', 'td_device_id'
]
df = pd.read_csv(INPUT_FILE, usecols=usecols, nrows=10000)

# 处理login_time为datetime
if df['login_time'].dtype != 'datetime64[ns]':
    df['login_time'] = pd.to_datetime(df['login_time'], errors='coerce')

# 统计无效日期数量
invalid_dates = df['login_time'].isna().sum()
print(f"无效日期（异常数据）数量: {invalid_dates}")

# 按login_time排序，去除无效日期
df = df.dropna(subset=['login_time'])
df = df.sort_values('login_time')

# 按月分组并保存
for (year, month), group in df.groupby([df['login_time'].dt.year, df['login_time'].dt.month]):
    out_name = f"{year:04d}-{month:02d}.csv"
    out_path = os.path.join(OUTPUT_DIR, out_name)
    group.to_csv(out_path, index=False)
    print(f"保存: {out_path}，共{len(group)}条记录")

import pandas as pd

# 文件路径
LOG_FILE = '/data/aiimport_1119/客户信息log.csv'
BASE_FILE = '/data/aiimport_1119/客户信息.csv'

# 只读取必要的列
log_df = pd.read_csv(LOG_FILE, usecols=['baseid'])
base_df = pd.read_csv(BASE_FILE, usecols=['id'])

# 去重
log_baseids = set(log_df['baseid'].dropna().astype(str).unique())
base_ids = set(base_df['id'].dropna().astype(str).unique())

# 检查完备性
missing_ids = base_ids - log_baseids
print(f"客户信息.csv中id总数: {len(base_ids)}")
print(f"客户信息log.csv中baseid总数: {len(log_baseids)}")
print(f"未在log中找到的id数量: {len(missing_ids)}")
if missing_ids:
    print("未匹配到的id示例:", list(missing_ids)[:10])
else:
    print("所有客户信息.csv中的id都能在客户信息log.csv中找到对应baseid！")

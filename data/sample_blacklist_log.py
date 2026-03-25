import pandas as pd

# 源文件路径（服务器上的原始黑名单log.csv）
source_file = '/data/aiimport_1119/订单order信息.csv'
# 目标文件路径（项目data目录下）
target_file = './data/订单order_sample.csv'

# 读取前1000行
try:
    df = pd.read_csv(source_file, nrows=1000, encoding='utf-8')
except UnicodeDecodeError:
    # 若utf-8失败，尝试gbk
    df = pd.read_csv(source_file, nrows=1000, encoding='gbk')

# 保存到目标文件
# 保留原始列名和内容
# index=False避免保存行号
# encoding='utf-8-sig'保证中文兼容

df.to_csv(target_file, index=False, encoding='utf-8-sig')

print(f'已保存前1000行到 {target_file}')

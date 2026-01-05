import pandas as pd

# 设置pandas显示所有列
pd.set_option('display.max_columns', None)

file = '/data/processed/first_linkman_derived/2023-11.csv'
df = pd.read_csv(file, nrows=20)
print(df.columns)
print(df.head())
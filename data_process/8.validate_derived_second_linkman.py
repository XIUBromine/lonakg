import pandas as pd

# 文件路径
DERIVED_FILE = '/data/aiimport_1119/手机号关联其他联系人衍生.csv'
LINKMAN_FILE = '/data/aiimport_1119/其它联系人.csv'

# 只读取必要的列
derived_df = pd.read_csv(DERIVED_FILE, usecols=['cid', 'second_mobile_phone'])
linkman_df = pd.read_csv(LINKMAN_FILE, usecols=['cid', 'second_mobile_phone'])

# 转为字符串，防止类型不一致
for col in ['cid', 'second_mobile_phone']:
    derived_df[col] = derived_df[col].astype(str)
    linkman_df[col] = linkman_df[col].astype(str)

# 合并查找
merged = derived_df.merge(linkman_df.drop_duplicates(), on=['cid', 'second_mobile_phone'], how='left', indicator=True)

found = merged['_merge'] == 'both'
not_found = ~found

found_count = found.sum()
not_found_count = not_found.sum()
total = len(merged)

print(f"总数: {total}")
print(f"能找到的数量: {found_count}，比例: {found_count/total:.2%}")
print(f"找不到的数量: {not_found_count}，比例: {not_found_count/total:.2%}")

if not_found_count > 0:
    print("找不到的示例:")
    print(merged.loc[not_found, ['cid', 'second_mobile_phone']].head(10))
else:
    print("全部都能找到")

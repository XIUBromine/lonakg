#!/usr/bin/env python3
"""
读取黑名单 CSV 并打印表结构与前10行。

用法:
    python read_blacklist_preview.py [--path /data/aiimport_1119/黑名单log.csv]

脚本会尝试使用 utf-8, gbk, latin1 编码依次打开文件。
"""
import argparse
import os
import sys
from typing import Optional

try:
    import pandas as pd
except Exception as e:
    print("需要 pandas 库来运行此脚本。请先安装：pip install pandas")
    raise


def try_read_csv(path: str, nrows: Optional[int] = None):
    """尝试不同编码读取 CSV，返回 (df, encoding) 或抛出异常。"""
    encodings = ["utf-8", "gbk", "latin1"]
    last_err = None
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc, nrows=nrows, low_memory=False)
            return df, enc
        except Exception as e:
            last_err = e
    raise last_err


def print_schema(df: pd.DataFrame):
    print("\n表结构（列名 -> pandas dtype）：")
    for col, dtype in df.dtypes.items():
        print(f"- {col} -> {dtype}")


def main():
    parser = argparse.ArgumentParser(description="读取登录信息CSV 并打印表结构与前10行")
    parser.add_argument(
        "--path",
        "-p",
        # default="/data/aiimport_1119/黑名单log.csv",
        # help="CSV 文件路径，默认 /data/aiimport_1119/黑名单log.csv",
        default="/data/aiimport_1119/客户信息log.csv",
        help="CSV 文件路径，默认 /data/aiimport_1119/客户信息log.csv",
    )
    args = parser.parse_args()
    path = args.path

    if not os.path.exists(path):
        print(f"文件不存在: {path}")
        sys.exit(2)

    print(f"尝试读取文件: {path}")
    try:
        # 先读取少量行以推断表结构
        sample_df, enc = try_read_csv(path, nrows=200)
    except Exception as e:
        print("读取文件失败（尝试了多种编码）：", str(e))
        sys.exit(3)

    print(f"成功读取（用于推断） 使用编码: {enc}，共检测列数: {len(sample_df.columns)}")
    print_schema(sample_df)

    # 读取前10行并显示
    try:
        head_df, enc2 = try_read_csv(path, nrows=10)
    except Exception as e:
        print("读取前10行失败：", str(e))
        sys.exit(4)

    print("\n文件前10行（显示）：")
    # 尽量以表格形式打印
    with pd.option_context('display.max_columns', None, 'display.width', 200):
        print(head_df.head(10).to_string(index=False))


if __name__ == '__main__':
    main()

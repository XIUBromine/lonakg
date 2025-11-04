#!/usr/bin/env python3
"""
用户ID抽取脚本

从订单CSV文件中根据订单状态抽取正常状态的user_id，
从黑名单CSV文件中抽取黑名单用户uid，
并确保正常用户集合中不包含黑名单用户（去重处理）。

输出文件：
- normal_user_ids.txt: 正常状态订单的用户ID（已排除黑名单用户）
- blacklist_user_ids.txt: 黑名单用户UID
- overlap_user_ids.txt: 既在正常订单又在黑名单中的重叠用户ID（供后续分析）

用于二分类任务的训练数据准备。
"""

from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Set, List
import argparse

# 订单状态分类定义
EXCLUDED_STATUSES = {
    "INIT", "INITLOAN", "VERIFCANCEL", "PREVERIF", "LOAN_REFUSE",
    "ON_ROUTE", "HOLD_ON", "RISK_CONFIRM", "PRESIGN", "SINGFAIL",
    "FINANCING", "CANCEL", "PRELOAN", "FAIL", "REPAYING", "LOAN_SUCESS_PRE_WP", "EARLYREPAYING", "REFUNDSETTLED", "WAITING_PAYMENT", "WAITING_WITHDRAW",
    "WITHDRAWING", "WITHDRAW_SUCESS", "SETTLED", "NOTICE_SETTLE", "WITHDRAW_FAIL_CARD"
}

NORMAL_STATUSES = {
    "EARLY_REPAYED",
    "REPAYED"
}

ABNORMAL_STATUSES = {
    "OVERDUE"
}


def load_order_data(csv_path: str) -> pd.DataFrame:
    """加载订单CSV数据"""
    try:
        df = pd.read_csv(
            csv_path,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            encoding="utf-8-sig"
        )
        print(f"✅ 成功加载订单数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        print(f"❌ 加载CSV文件失败: {e}")
        raise


def extract_user_ids_by_status(df: pd.DataFrame, status_set: Set[str]) -> List[str]:
    """根据订单状态集合抽取用户ID"""
    # 筛选符合状态的订单
    filtered_df = df[df['order_status'].isin(status_set)]
    
    # 获取去重的用户ID列表
    user_ids = filtered_df['user_id'].unique().tolist()
    
    # 过滤掉空值
    user_ids = [uid for uid in user_ids if uid and uid.strip()]
    
    return sorted(user_ids)


def save_user_ids_to_file(user_ids: List[str], output_path: str) -> None:
    """将用户ID列表保存到文件"""
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            for uid in user_ids:
                f.write(f"{uid}\n")
        print(f"✅ 成功保存 {len(user_ids)} 个用户ID到: {output_path}")
    except Exception as e:
        print(f"❌ 保存文件失败: {e}")
        raise


def analyze_order_status_distribution(df: pd.DataFrame) -> None:
    """分析订单状态分布"""
    print(f"\n{'='*60}")
    print("订单状态分布分析")
    print(f"{'='*60}")
    
    status_counts = df['order_status'].value_counts()
    
    print(f"总订单数: {len(df)}")
    print(f"不同状态数: {len(status_counts)}")
    
    print(f"\n状态分布统计:")
    for status, count in status_counts.items():
        percentage = count / len(df) * 100
        
        if status in NORMAL_STATUSES:
            category = "正常"
        elif status in ABNORMAL_STATUSES:
            category = "异常"
        elif status in EXCLUDED_STATUSES:
            category = "排除"
        else:
            category = "未分类"
        
        print(f"  {status:<20} {count:>6} ({percentage:>5.1f}%) [{category}]")
    
    # 统计各分类的订单数
    normal_count = len(df[df['order_status'].isin(NORMAL_STATUSES)])
    abnormal_count = len(df[df['order_status'].isin(ABNORMAL_STATUSES)])
    excluded_count = len(df[df['order_status'].isin(EXCLUDED_STATUSES)])
    unclassified_count = len(df) - normal_count - abnormal_count - excluded_count
    
    print(f"\n分类汇总:")
    print(f"  正常状态订单: {normal_count} ({normal_count/len(df)*100:.1f}%)")
    print(f"  异常状态订单: {abnormal_count} ({abnormal_count/len(df)*100:.1f}%)")
    print(f"  排除状态订单: {excluded_count} ({excluded_count/len(df)*100:.1f}%)")
    if unclassified_count > 0:
        print(f"  未分类状态订单: {unclassified_count} ({unclassified_count/len(df)*100:.1f}%)")


def load_blacklist_data(csv_path: str) -> pd.DataFrame:
    """加载黑名单CSV数据"""
    try:
        df = pd.read_csv(
            csv_path,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            encoding="utf-8-sig"
        )
        print(f"✅ 成功加载黑名单数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        print(f"❌ 加载黑名单CSV文件失败: {e}")
        raise


def extract_blacklist_uids(df: pd.DataFrame) -> List[str]:
    """从黑名单数据中抽取UID"""
    # 获取去重的UID列表（使用id字段）
    uids = df['id'].unique().tolist()
    
    # 过滤掉空值
    uids = [uid for uid in uids if uid and uid.strip()]
    
    return sorted(uids)


def main():
    parser = argparse.ArgumentParser(description="从订单和黑名单CSV文件中抽取用户ID")
    parser.add_argument("--order-input", "-oi", type=str, default="data/订单order信息.csv", 
                       help="订单CSV文件路径")
    parser.add_argument("--blacklist-input", "-bi", type=str, default="data/黑名单.csv", 
                       help="黑名单CSV文件路径")
    parser.add_argument("--output-dir", "-o", type=str, default="data_analysis", 
                       help="输出目录")
    parser.add_argument("--analyze-only", action="store_true", 
                       help="仅分析状态分布，不生成输出文件")
    
    args = parser.parse_args()
    
    # 检查输入文件
    order_path = Path(args.order_input)
    blacklist_path = Path(args.blacklist_input)
    
    if not order_path.exists():
        print(f"❌ 订单文件不存在: {order_path}")
        return
    
    if not blacklist_path.exists():
        print(f"❌ 黑名单文件不存在: {blacklist_path}")
        return
    
    # 创建输出目录
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    
    # 加载订单数据
    print("加载订单数据...")
    order_df = load_order_data(str(order_path))
    
    # 分析状态分布
    analyze_order_status_distribution(order_df)
    
    # 加载黑名单数据
    print("\n加载黑名单数据...")
    blacklist_df = load_blacklist_data(str(blacklist_path))
    
    if args.analyze_only:
        print("\n⏹️  仅分析模式，不生成输出文件")
        return
    
    # 抽取正常状态用户ID
    print(f"\n{'='*60}")
    print("抽取用户ID")
    print(f"{'='*60}")
    
    normal_user_ids = extract_user_ids_by_status(order_df, NORMAL_STATUSES)
    print(f"✅ 从订单中抽取正常状态用户: {len(normal_user_ids)} 个")
    
    # 抽取黑名单用户UID
    blacklist_uids = extract_blacklist_uids(blacklist_df)
    print(f"✅ 从黑名单中抽取用户UID: {len(blacklist_uids)} 个")
    
    # 检查重叠并移除黑名单用户
    blacklist_set = set(blacklist_uids)
    normal_set = set(normal_user_ids)
    original_normal_count = len(normal_user_ids)
    
    # 找出重叠的用户ID
    overlap_user_ids = sorted(list(normal_set & blacklist_set))
    
    # 从正常用户中移除黑名单用户
    clean_normal_user_ids = [uid for uid in normal_user_ids if uid not in blacklist_set]
    removed_count = original_normal_count - len(clean_normal_user_ids)
    
    if removed_count > 0:
        print(f"🔄 从正常用户中移除了 {removed_count} 个黑名单用户")
        print(f"📊 发现 {len(overlap_user_ids)} 个重叠用户（既在正常订单又在黑名单中）")
    
    # 保存文件
    normal_output_path = output_dir / "normal_user_ids.txt"
    save_user_ids_to_file(clean_normal_user_ids, str(normal_output_path))
    
    blacklist_output_path = output_dir / "blacklist_user_ids.txt"
    save_user_ids_to_file(blacklist_uids, str(blacklist_output_path))
    
    # 保存重叠用户ID
    if overlap_user_ids:
        overlap_output_path = output_dir / "overlap_user_ids.txt"
        save_user_ids_to_file(overlap_user_ids, str(overlap_output_path))
        print(f"📋 重叠用户ID已保存到: {overlap_output_path}")
    
    # 统计信息
    print(f"\n✅ 抽取完成!")
    print(f"正常状态用户数（已去重黑名单）: {len(clean_normal_user_ids)}")
    print(f"黑名单用户数: {len(blacklist_uids)}")
    print(f"重叠用户数: {len(overlap_user_ids)}")
    print(f"总计可用于分类的用户数: {len(clean_normal_user_ids) + len(blacklist_uids)}")
    
    # 显示重叠用户的详细信息
    if overlap_user_ids:
        print(f"\n🔍 重叠用户分析:")
        print(f"  - 这些用户既有正常还款订单，又在黑名单中")
        print(f"  - 可能表示用户行为变化或数据质量问题")
        print(f"  - 前5个重叠用户示例: {overlap_user_ids[:5]}")
        print(f"  - 重叠用户占正常用户比例: {len(overlap_user_ids)/original_normal_count*100:.2f}%")
        print(f"  - 重叠用户占黑名单比例: {len(overlap_user_ids)/len(blacklist_uids)*100:.2f}%")
    
    # 检查未分类状态
    all_statuses = set(order_df['order_status'].unique())
    classified_statuses = NORMAL_STATUSES | ABNORMAL_STATUSES | EXCLUDED_STATUSES
    unclassified_statuses = all_statuses - classified_statuses
    
    if unclassified_statuses:
        print(f"⚠️  发现未分类的订单状态: {unclassified_statuses}")
        print("请检查状态定义是否完整")


if __name__ == "__main__":
    main()
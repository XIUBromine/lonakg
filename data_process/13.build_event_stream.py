import json
import os
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pandas as pd

# 目录配置
PROCESSED_ROOT = '/data/processed'
RAW_ROOT = '/data/aiimport_1119'

OUTPUT_DIR = os.path.join(PROCESSED_ROOT, 'events')
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

MONTH_FILE_RE = re.compile(r'(\d{4})-(\d{2})\.csv$')


def list_months(dir_path: str) -> Set[Tuple[int, int]]:
    months: Set[Tuple[int, int]] = set()
    if not os.path.isdir(dir_path):
        return months
    for name in os.listdir(dir_path):
        m = MONTH_FILE_RE.match(name)
        if m:
            months.add((int(m.group(1)), int(m.group(2))))
    return months


def parse_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors='coerce')


def read_month_csv(path: str, usecols: List[str], time_col: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        return pd.DataFrame(columns=usecols)
    df = pd.read_csv(path, usecols=usecols)
    df[time_col] = parse_datetime(df[time_col])
    df = df.dropna(subset=[time_col])
    return df


def load_labels() -> Dict[str, pd.DataFrame]:
    def load_or_empty(path: str, usecols: List[str]) -> pd.DataFrame:
        if os.path.isfile(path):
            return pd.read_csv(path, usecols=usecols).fillna('')
        return pd.DataFrame(columns=usecols)

    labels: Dict[str, pd.DataFrame] = {}
    labels['blacklist'] = load_or_empty(
        os.path.join(RAW_ROOT, '黑名单.csv'),
        ['c_customer_no', 'c_reason_list', 'c_mobile_phone', 'c_identity_no'],
    )
    labels['complaint'] = load_or_empty(
        os.path.join(RAW_ROOT, '客诉工单信息.csv'),
        ['c_contact_phone_no', 'c_register_phone_no', 'c_identity_no'],
    )
    labels['consumer'] = load_or_empty(
        os.path.join(RAW_ROOT, '消保案件工单.csv'),
        ['c_contact_phone_no', 'c_register_phone_no', 'c_identity_no'],
    )
    return labels


def build_risk_label(order_row: pd.Series, labels: Dict[str, pd.DataFrame], id_type: str, id_value: str) -> Dict:
    order_status = order_row.get('order_status')

    bl_df = labels['blacklist']
    cs_df = labels['complaint']
    cp_df = labels['consumer']

    if not id_value:
        return {
            'blacklist': [],
            'complaint': [],
            'consumer_case': [],
            'order_status': order_status,
        }

    if id_type == 'uid':
        bl_hits = bl_df[bl_df['c_customer_no'] == id_value]
        comp_hits = cs_df.iloc[0:0]
        cp_hits = cp_df.iloc[0:0]
    elif id_type == 'identity_no':
        bl_hits = bl_df[bl_df['c_identity_no'] == id_value]
        comp_hits = cs_df[cs_df['c_identity_no'] == id_value]
        cp_hits = cp_df[cp_df['c_identity_no'] == id_value]
    elif id_type == 'phone_num':
        bl_hits = bl_df[bl_df['c_mobile_phone'] == id_value]
        comp_hits = cs_df[(cs_df['c_contact_phone_no'] == id_value) | (cs_df['c_register_phone_no'] == id_value)]
        cp_hits = cp_df[(cp_df['c_contact_phone_no'] == id_value) | (cp_df['c_register_phone_no'] == id_value)]
    else:
        bl_hits = bl_df.iloc[0:0]
        comp_hits = cs_df.iloc[0:0]
        cp_hits = cp_df.iloc[0:0]

    return {
        'blacklist': bl_hits.to_dict(orient='records'),
        'complaint': comp_hits.to_dict(orient='records'),
        'consumer_case': cp_hits.to_dict(orient='records'),
        'order_status': order_status,
    }


def append_events_from_df(events: List[Dict], df: pd.DataFrame, time_col: str, event_type: str, payload_cols: List[str]):
    for _, row in df.iterrows():
        ts = row[time_col]
        events.append({
            'type': event_type,
            'ts': ts.isoformat(),
            'data': {col: row.get(col) for col in payload_cols},
        })


def process_month(year: int, month: int, labels: Dict[str, Dict[str, Set[str]]] ):
    events: List[Dict] = []
    ym = f"{year:04d}-{month:02d}"
    # 数据源文件路径
    customer_log_path = os.path.join(PROCESSED_ROOT, 'customer_log', f'{ym}.csv')
    login_path = os.path.join(PROCESSED_ROOT, 'login', f'{ym}.csv')
    gps_path = os.path.join(PROCESSED_ROOT, 'lbs_gps', f'{ym}.csv')
    first_linkman_path = os.path.join(PROCESSED_ROOT, 'first_linkman', f'{ym}.csv')
    second_linkman_path = os.path.join(PROCESSED_ROOT, 'second_linkman', f'{ym}.csv')
    first_linkman_derived_path = os.path.join(PROCESSED_ROOT, 'first_linkman_derived', f'{ym}.csv')
    second_linkman_derived_path = os.path.join(PROCESSED_ROOT, 'second_linkman_derived', f'{ym}.csv')
    logout_path = os.path.join(PROCESSED_ROOT, 'logout', f'{ym}.csv')
    order_path = os.path.join(PROCESSED_ROOT, 'order', f'{ym}.csv')

    # 客户信息修改
    df = read_month_csv(customer_log_path, ['baseid', 'identity_no', 'mobile_phone', 'log_create_date'], 'log_create_date')
    append_events_from_df(events, df, 'log_create_date', 'customer_update', ['baseid', 'identity_no', 'mobile_phone'])

    # 登录
    df = read_month_csv(login_path, ['phone_num', 'cif_user_id', 'login_time', 'device_no', 'remote_ip', 'td_device_id'], 'login_time')
    append_events_from_df(events, df, 'login_time', 'login', ['phone_num', 'cif_user_id', 'device_no', 'remote_ip', 'td_device_id'])

    # GPS
    df = read_month_csv(gps_path, ['cid', 'geo_code', 'create_date'], 'create_date')
    append_events_from_df(events, df, 'create_date', 'gps', ['cid', 'geo_code'])

    # 联系人编辑：四类联系人源合并
    contact_sources = [
        (first_linkman_path, ['cid', 'mobile_phone', 'create_date'], 'create_date', 'mobile_phone'),
        (second_linkman_path, ['cid', 'second_mobile_phone', 'create_date'], 'create_date', 'second_mobile_phone'),
        (first_linkman_derived_path, ['cid', 'mobile_phone', 'create_date'], 'create_date', 'mobile_phone'),
        (second_linkman_derived_path, ['cid', 'second_mobile_phone', 'create_date'], 'create_date', 'second_mobile_phone'),
    ]
    for path, cols, tcol, phone_col in contact_sources:
        df = read_month_csv(path, cols, tcol)
        if not df.empty:
            df = df.rename(columns={phone_col: 'mobile_phone'})
            append_events_from_df(events, df, tcol, 'contact_edit', ['cid', 'mobile_phone'])

    # 注销
    df = read_month_csv(logout_path, ['cid', 'identity_no', 'mobile_phone', 'create_date'], 'create_date')
    append_events_from_df(events, df, 'create_date', 'logout', ['cid', 'identity_no', 'mobile_phone'])

    # 订单与风险
    if os.path.isfile(order_path):
        df = pd.read_csv(order_path, usecols=[
            'id', 'user_id', 'apply_loan_tel', 'apply_ident_no', 'apply_card_no',
            'apply_bank_mobile', 'repay_card_no', 'repay_bank_mobile', 'order_status', 'create_date'
        ])
        df['create_date'] = parse_datetime(df['create_date'])
        df = df.dropna(subset=['create_date'])
        df = df.sort_values('create_date')
        for _, row in df.iterrows():
            ts = row['create_date']
            order_payload = {
                'order_id': row['id'],
                'user_id': row['user_id'],
                'apply_loan_tel': row['apply_loan_tel'],
                'apply_ident_no': row['apply_ident_no'],
                'apply_card_no': row['apply_card_no'],
                'apply_bank_mobile': row['apply_bank_mobile'],
                'repay_card_no': row['repay_card_no'],
                'repay_bank_mobile': row['repay_bank_mobile'],
            }
            events.append({
                'type': 'order',
                'ts': ts.isoformat(),
                'data': order_payload,
            })
            # 针对每个标识符生成独立的风险事件，标签仅用该标识符匹配
            id_targets: List[Tuple[str, str]] = []
            if pd.notna(row.get('user_id')) and str(row['user_id']) != '':
                id_targets.append(('uid', str(row['user_id'])))
            if pd.notna(row.get('apply_ident_no')) and str(row['apply_ident_no']) != '':
                id_targets.append(('identity_no', str(row['apply_ident_no'])))
            mobile_vals = [row.get(col) for col in ['apply_loan_tel', 'apply_bank_mobile', 'repay_bank_mobile']]
            mobile_vals = [str(m) for m in mobile_vals if pd.notna(m) and str(m) != '']
            for m in sorted(set(mobile_vals)):
                id_targets.append(('phone_num', m))

            for id_type, id_value in id_targets:
                label = build_risk_label(row, labels, id_type, id_value)
                events.append({
                    'type': 'risk_assessment',
                    'ts': ts.isoformat(),
                    'id_type': id_type,
                    'id_value': id_value,
                    'labels': label,
                })

    # 排序并输出
    events.sort(key=lambda x: x['ts'])
    out_path = os.path.join(OUTPUT_DIR, f'{ym}.jsonl')
    with open(out_path, 'w', encoding='utf-8') as f:
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False) + '\n')
    print(f'{ym} 完成，事件数: {len(events)} -> {out_path}')


def main():
    labels = load_labels()
    month_sets = [
        list_months(os.path.join(PROCESSED_ROOT, 'customer_log')),
        list_months(os.path.join(PROCESSED_ROOT, 'login')),
        list_months(os.path.join(PROCESSED_ROOT, 'lbs_gps')),
        list_months(os.path.join(PROCESSED_ROOT, 'first_linkman')),
        list_months(os.path.join(PROCESSED_ROOT, 'second_linkman')),
        list_months(os.path.join(PROCESSED_ROOT, 'first_linkman_derived')),
        list_months(os.path.join(PROCESSED_ROOT, 'second_linkman_derived')),
        list_months(os.path.join(PROCESSED_ROOT, 'logout')),
        list_months(os.path.join(PROCESSED_ROOT, 'order')),
    ]
    months: Set[Tuple[int, int]] = set()
    for s in month_sets:
        months |= s
    for year, month in sorted(months):
        process_month(year, month, labels)


if __name__ == '__main__':
    main()

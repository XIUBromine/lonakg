import os

def find_intersection_and_forecast(blacklist_file, risk_file, output_file):
    """
    找出同时存在于黑名单和风险文件中的用户，并输出其风险值
    """
    black_uids = set()
    
    print(f"开始加载黑名单 UID: {blacklist_file}")
    try:
        with open(blacklist_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # 假设黑名单文件第一列是 UID
                uid = line.split()[0]
                black_uids.add(uid)
        print(f"加载完毕，共读取到 {len(black_uids)} 个独立的黑名单 UID。")
    except FileNotFoundError:
        print(f"错误: 找不到黑名单输入文件 {blacklist_file}")
        return
    except Exception as e:
        print(f"读取黑名单文件时出错: {e}")
        return

    print(f"\n开始匹配风险文件: {risk_file}")
    matched_results = []
    try:
        with open(risk_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split()
                if len(parts) >= 2:
                    uid = parts[0]
                    risk_score = parts[1]
                    
                    # 检查是否同时存在于黑名单中
                    if uid in black_uids:
                        matched_results.append((uid, risk_score))
                        
        print(f"匹配完成！共找到 {len(matched_results)} 个同时存在于两个文件中的 UID。")
    except FileNotFoundError:
        print(f"错误: 找不到风险输入文件 {risk_file}")
        return
    except Exception as e:
        print(f"匹配风险文件时出错: {e}")
        return

    print(f"\n准备将结果输出至: {output_file}")
    
    # 确保输出目录存在
    out_dir = os.path.dirname(output_file)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        
    try:
        with open(output_file, 'w', encoding='utf-8') as f_out:
            for uid, risk in matched_results:
                f_out.write(f"{uid}\t{risk}\n")
        print("处理完成！数据已成功保存。")
    except PermissionError:
        print("错误: 没有权限写入输出文件。")
    except Exception as e:
        print(f"写入文件时发生错误: {e}")

if __name__ == "__main__":
    # 获取当前脚本所在目录的绝对路径 (.../model/)
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # 配置文件路径
    BLACKLIST_FILE = os.path.join(BASE_DIR, "output", "black_uid_1120_0323.txt")
    RISK_FILE = os.path.join(BASE_DIR, "output", "uids_repaying_risks_first3000.txt")
    OUTPUT_FILE = os.path.join(BASE_DIR, "output", "forecast.txt")

    find_intersection_and_forecast(
        blacklist_file=BLACKLIST_FILE,
        risk_file=RISK_FILE,
        output_file=OUTPUT_FILE
    )

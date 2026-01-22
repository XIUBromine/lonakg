import os

INPUT_FILE = '/data/processed/events/2025-04.jsonl'
OUTPUT_FILE = 'example.jsonl'

start = 5000  # 起始行为第5001行（0-based）
count = 10000  # 取1w行

with open(INPUT_FILE, 'r', encoding='utf-8') as fin:
    lines = fin.readlines()

mid_lines = lines[start:start+count]

with open(OUTPUT_FILE, 'w', encoding='utf-8') as fout:
    fout.writelines(mid_lines)

print(f'已写入{len(mid_lines)}行到 {OUTPUT_FILE}')

import requests
import re
import json
import os
from datetime import datetime, timedelta, timezone

JS_FILE = 'taifex_data.js'
MAX_DAYS = 2500

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://www.taifex.com.tw/cht/3/futContractsDate'
}

TW_TZ = timezone(timedelta(hours=8))


def now_tw():
    return datetime.now(TW_TZ)


def format_tw(dt):
    return dt.strftime('%Y/%m/%d %H:%M TW')


def extract_json_var(content, var_name):
    pattern = rf'var\s+{var_name}\s*=\s*(.*?);'
    match = re.search(pattern, content, re.DOTALL)
    if not match:
        return None
    raw = match.group(1).strip()
    try:
        return json.loads(raw)
    except Exception:
        return None


def load_existing():
    if not os.path.exists(JS_FILE):
        print("📂 找不到舊 taifex_data.js，將建立新檔")
        return [], {}

    try:
        with open(JS_FILE, 'r', encoding='utf-8') as f:
            content = f.read()

        data = extract_json_var(content, 'TAIFEX_DATA')
        meta = extract_json_var(content, 'TAIFEX_META')

        if not isinstance(data, list):
            print("📂 舊資料格式異常，改視為空資料")
            return [], meta if isinstance(meta, dict) else {}

        print(f"📂 舊資料: {len(data)} 筆")
        return data, meta if isinstance(meta, dict) else {}
    except Exception as e:
        print(f"📂 讀取舊資料失敗: {e}")
        return [], {}


def parse_int(v):
    if v is None:
        return 0
    s = str(v).replace(',', '').replace('+', '').strip()
    if s in ('', '-', '--'):
        return 0
    try:
        return int(s)
    except Exception:
        return 0


def normalize_record(record):
    if not isinstance(record, dict):
        return None
    if 'date' not in record:
        return None

    out = {'date': record['date']}

    if isinstance(record.get('inst_tx'), dict):
        out['inst_tx'] = {
            'long': parse_int(record['inst_tx'].get('long', 0)),
            'short': parse_int(record['inst_tx'].get('short', 0)),
            'net': parse_int(record['inst_tx'].get('net', 0)),
        }

    if isinstance(record.get('inst_mtx'), dict):
        out['inst_mtx'] = {
            'long': parse_int(record['inst_mtx'].get('long', 0)),
            'short': parse_int(record['inst_mtx'].get('short', 0)),
            'net': parse_int(record['inst_mtx'].get('net', 0)),
        }

    if isinstance(record.get('near'), dict):
        out['near'] = {
            'top5_l': parse_int(record['near'].get('top5_l', 0)),
            'top5_s': parse_int(record['near'].get('top5_s', 0)),
            'top5_net': parse_int(record['near'].get('top5_net', 0)),
            'top10_l': parse_int(record['near'].get('top10_l', 0)),
            'top10_s': parse_int(record['near'].get('top10_s', 0)),
            'top10_net': parse_int(record['near'].get('top10_net', 0)),
        }

    if isinstance(record.get('allm'), dict):
        out['allm'] = {
            'top5_l': parse_int(record['allm'].get('top5_l', 0)),
            'top5_s': parse_int(record['allm'].get('top5_s', 0)),
            'top5_net': parse_int(record['allm'].get('top5_net', 0)),
            'top10_l': parse_int(record['allm'].get('top10_l', 0)),
            'top10_s': parse_int(record['allm'].get('top10_s', 0)),
            'top10_net': parse_int(record['allm'].get('top10_net', 0)),
        }

    return out


def is_meaningful_record(record):
    if not record or 'date' not in record:
        return False

    keys = ['inst_tx', 'inst_mtx', 'near', 'allm']
    has_any_block = any(isinstance(record.get(k), dict) and len(record.get(k)) > 0 for k in keys)
    return has_any_block


def fetch_latest_data(days=7):
    """
    這裡先保留安全框架：
    若未成功抓到完整新資料，就回傳 []，避免覆蓋舊資料。
    你後續可把真實抓 TX/MTX/近月/全月解析邏輯補進來。
    """
    results = []
    end_date = now_tw()

    for i in range(days):
        query_date = (end_date - timedelta(days=i)).strftime('%Y/%m/%d')
        print(f"📅 檢查 {query_date} ...")

        # 這裡先不硬覆蓋舊資料
        # 若你之後已經有穩定解析邏輯，可在這裡 append 真實 record
        # results.append({...})

    return results


def merge_data(old_data, new_data):
    merged = {}

    for row in old_data:
        n = normalize_record(row)
        if n and 'date' in n:
            merged[n['date']] = n

    for row in new_data:
        n = normalize_record(row)
        if n and 'date' in n and is_meaningful_record(n):
            if n['date'] in merged:
                base = merged[n['date']]
                for key in ['inst_tx', 'inst_mtx', 'near', 'allm']:
                    if key in n and isinstance(n[key], dict) and n[key]:
                        base[key] = n[key]
                merged[n['date']] = base
            else:
                merged[n['date']] = n

    final = sorted(
        merged.values(),
        key=lambda x: datetime.strptime(x['date'], '%Y/%m/%d'),
        reverse=True
    )[:MAX_DAYS]

    return final


def save_js(final_data, old_meta=None):
    meta = old_meta.copy() if isinstance(old_meta, dict) else {}
    meta['updated_at'] = format_tw(now_tw())

    with open(JS_FILE, 'w', encoding='utf-8') as f:
        f.write(f"var TAIFEX_META = {json.dumps(meta, ensure_ascii=False, separators=(',', ':'))};\n")
        f.write(f"var TAIFEX_DATA = {json.dumps(final_data, ensure_ascii=False, separators=(',', ':'))};\n")


def main():
    print("=== TAIFEX 安全更新模式 ===")

    old_data, old_meta = load_existing()
    new_data = fetch_latest_data(days=7)

    valid_new = [x for x in new_data if is_meaningful_record(x)]
    print(f"📦 新資料有效筆數: {len(valid_new)}")

    if not old_data and not valid_new:
        print("⚠️ 沒有舊資料，也沒有有效新資料，停止寫入，避免產生空殼檔")
        return

    if not valid_new:
        print("⚠️ 本次沒有抓到有效新資料，保留舊資料，只更新 updated_at")
        final_data = old_data
    else:
        final_data = merge_data(old_data, valid_new)

    if not final_data:
        print("⚠️ 合併後資料為空，停止寫入")
        return

    save_js(final_data, old_meta=old_meta)

    print(f"🎉 完成，保留/輸出 {len(final_data)} 筆")
    print("最新三筆：")
    for row in final_data[:3]:
        print(f" {row.get('date')} keys={','.join([k for k in row.keys() if k != 'date'])}")


if __name__ == '__main__':
    main()

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


def fetch_latest_tx_data(days=7):
    all_data = []
    end_date = datetime.now(TW_TZ)

    for i in range(days):
        query_date = (end_date - timedelta(days=i)).strftime('%Y/%m/%d')
        print(f"📅 抓 {query_date}...")

        url = "https://www.taifex.com.tw/cht/3/futContractsDate"
        params = {
            'queryStartDate': query_date,
            'queryEndDate': query_date,
            'commodityId': 'TX'
        }

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=10)
            if resp.status_code == 200:
                tx_data = parse_html_for_tx(resp.text, query_date)
                if tx_data:
                    all_data.append(tx_data)
                    print(f"✅ {query_date}: 成功")
                else:
                    print(f"⚠️ {query_date} 無 TX")
            else:
                print(f"❌ {query_date} HTTP {resp.status_code}")
        except Exception as e:
            print(f"❌ {query_date} 錯誤: {e}")

    return all_data


def parse_html_for_tx(html, date):
    tx_pattern = r'臺股期貨.*?外資.*?(\d+(?:,\d+)?).*?(\d+(?:,\d+)?).*?(-?\d+(?:,\d+)?)'
    match = re.search(tx_pattern, html, re.DOTALL | re.IGNORECASE)

    if not match:
        return None

    try:
        long_val = int(match.group(1).replace(',', ''))
        short_val = int(match.group(2).replace(',', ''))
        net_val = int(match.group(3).replace(',', ''))

        return {
            'date': date,
            'inst_tx': {
                'long': long_val,
                'short': short_val,
                'net': net_val
            }
        }
    except Exception:
        return None


def load_existing():
    if not os.path.exists(JS_FILE):
        return []

    try:
        with open(JS_FILE, 'r', encoding='utf-8') as f:
            content = f.read()

        m = re.search(r'var\s+TAIFEX_DATA\s*=\s*(\[.*\]);?', content, re.DOTALL)
        if not m:
            return []

        data = json.loads(m.group(1))
        print(f"📂 舊資料: {len(data)} 筆")
        return data
    except Exception as e:
        print(f"📂 解析舊資料失敗: {e}")
        return []


def save_js(data):
    updated_at = datetime.now(TW_TZ).strftime('%Y/%m/%d %H:%M TW')

    meta = {
        'updated_at': updated_at
    }

    with open(JS_FILE, 'w', encoding='utf-8') as f:
        f.write(f"var TAIFEX_META = {json.dumps(meta, ensure_ascii=False)};\n")
        f.write(f"var TAIFEX_DATA = {json.dumps(data, ensure_ascii=False, separators=(',', ':'))};\n")


def main():
    print("=== TX 三大法人更新 ===")

    old_data = load_existing()
    new_data = fetch_latest_tx_data(days=7)

    if old_data:
        merged = {d['date']: d for d in old_data}
    else:
        merged = {}

    for d in new_data:
        merged[d['date']] = d

    final = sorted(
        merged.values(),
        key=lambda x: datetime.strptime(x['date'], '%Y/%m/%d'),
        reverse=True
    )[:MAX_DAYS]

    save_js(final)

    print(f"\n🎉 完成！共 {len(final)} 筆")
    if final:
        print("最新資料：")
        for d in final[:3]:
            tx = d.get('inst_tx', {})
            print(f" {d['date']} TX 外資：多={tx.get('long',0)} 空={tx.get('short',0)} 淨={tx.get('net',0)}")


if __name__ == '__main__':
    main()

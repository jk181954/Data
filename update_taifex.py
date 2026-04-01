import requests
import json
import os
import time
from datetime import datetime, timedelta

JSON_FILE = 'taifex_data.js'
JS_FILE = 'taifex_data.js'
MAX_DAYS = 2500

def fetch_large_trader(start_date, end_date):
    all_rows = []
    cursor = datetime.strptime(start_date, '%Y/%m/%d')
    end_dt = datetime.strptime(end_date, '%Y/%m/%d')

    while cursor <= end_dt:
        chunk_end = min(cursor + timedelta(days=88), end_dt)
        s_str = cursor.strftime('%Y/%m/%d')
        e_str = chunk_end.strftime('%Y/%m/%d')
        print(f" [期交所] 抓取區間: {s_str} ~ {e_str}")

        try:
            res = requests.post(
                'https://www.taifex.com.tw/cht/3/largeTraderFutDown',
                data={'queryStartDate': s_str, 'queryEndDate': e_str},
                timeout=30
            )
            text = res.content.decode('ms950')
            lines = text.strip().split('\n')

            if len(lines) > 1 and '查詢區間' not in text:
                header = [h.strip() for h in lines[0].split(',')]
                for line in lines[1:]:
                    vals = [v.strip() for v in line.split(',')]
                    if len(vals) == len(header):
                        all_rows.append(dict(zip(header, vals)))
        except Exception as e:
            print(f" [期交所] 錯誤: {e}")

        cursor = chunk_end + timedelta(days=1)
        time.sleep(1)

    return all_rows

def fetch_inst(symbol, start_date, end_date):
    s_str = start_date.replace('/', '-')
    e_str = end_date.replace('/', '-')
    print(f" [FinMind] 抓取 {symbol}: {s_str} ~ {e_str}")

    url = f"https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFuturesInstitutionalInvestors&data_id={symbol}&start_date={s_str}&end_date={e_str}"
    try:
        res = requests.get(url, timeout=30)
        data = res.json().get('data', [])
        return [d for d in data if '外資' in d.get('institutional_investors', '')]
    except Exception as e:
        print(f" [FinMind] 錯誤: {e}")
        return []

def process_data(start_date, end_date):
    large_raw = fetch_large_trader(start_date, end_date)
    inst_tx_raw = fetch_inst('TX', start_date, end_date)
    inst_mtx_raw = fetch_inst('MTX', start_date, end_date)

    daily_data = {}

    for d in inst_tx_raw:
        date = d['date'].replace('-', '/')
        l = d.get('long_open_interest_balance_volume', 0)
        s = d.get('short_open_interest_balance_volume', 0)
        if date not in daily_data:
            daily_data[date] = {"date": date}
        daily_data[date]["inst_tx"] = {"long": l, "short": s, "net": l - s}

    for d in inst_mtx_raw:
        date = d['date'].replace('-', '/')
        l = d.get('long_open_interest_balance_volume', 0)
        s = d.get('short_open_interest_balance_volume', 0)
        if date not in daily_data:
            daily_data[date] = {"date": date}
        daily_data[date]["inst_mtx"] = {"long": l, "short": s, "net": l - s}

    tx_large = [r for r in large_raw if r.get('商品(契約)','').strip()=='TX' and r.get('交易人類別','').strip()=='0']

    for r in tx_large:
        date = r['日期']
        month = r.get('到期月份(週別)','').strip()
        if date not in daily_data:
            daily_data[date] = {"date": date}

        t5l = int(r.get('前五大交易人買方', 0))
        t5s = int(r.get('前五大交易人賣方', 0))
        t10l = int(r.get('前十大交易人買方', 0))
        t10s = int(r.get('前十大交易人賣方', 0))

        payload = {"top5_l": t5l, "top5_s": t5s, "top5_net": t5l-t5s, "top10_l": t10l, "top10_s": t10s, "top10_net": t10l-t10s}

        if month == '999999':
            daily_data[date]["allm"] = payload
        elif len(month) == 6 and month not in ('666666', '999912'):
            if "near" not in daily_data[date] or month < daily_data[date].get("_near_month", "999999"):
                daily_data[date]["near"] = payload
                daily_data[date]["_near_month"] = month

    for date in daily_data:
        daily_data[date].pop("_near_month", None)

    return list(daily_data.values())

def main():
    print("=== 啟動台指期資料更新 ===")

    old_data = []
    
    # 1. 優先嘗試讀取 JSON 緩存檔
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            old_data = json.load(f)
        print(f"已讀取 JSON 歷史資料: 共 {len(old_data)} 筆")
        
    # 2. 如果沒有 JSON，但有原本的 JS 檔，就從 JS 檔提取
    elif os.path.exists(JS_FILE):
        print(f"找不到 JSON，嘗試從 {JS_FILE} 提取歷史資料...")
        try:
            with open(JS_FILE, 'r', encoding='utf-8') as f:
                content = f.read()
                start_idx = content.find('=')
                if start_idx != -1:
                    json_str = content[start_idx+1:].strip()
                    if json_str.endswith(';'):
                        json_str = json_str[:-1]
                    
                    parsed = json.loads(json_str)
                    if isinstance(parsed, dict) and "data" in parsed:
                        old_data = parsed["data"]
                    elif isinstance(parsed, list):
                        old_data = parsed
                    print(f"成功從 JS 檔提取歷史資料: 共 {len(old_data)} 筆")
        except Exception as e:
            print(f"解析 JS 檔失敗: {e}")

    end_date = datetime.now().strftime('%Y/%m/%d')

    if len(old_data) == 0:
        start_date = (datetime.now() - timedelta(days=3650)).strftime('%Y/%m/%d')
        print(f"無歷史資料，準備抓取 10 年資料: {start_date} ~ {end_date}")
    else:
        start_date = (datetime.now() - timedelta(days=5)).strftime('%Y/%m/%d')
        print(f"執行增量更新: {start_date} ~ {end_date}")

    new_data = process_data(start_date, end_date)
    print(f"本次成功抓取 {len(new_data)} 個交易日")

    data_dict = {d['date']: d for d in old_data}
    for d in new_data:
        data_dict[d['date']] = d

    final_data = sorted(list(data_dict.values()), key=lambda x: x['date'], reverse=True)

    if len(final_data) > MAX_DAYS:
        final_data = final_data[:MAX_DAYS]

    # 輸出 1: 保存 json 檔案 (後端純淨資料庫)
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        # 把資料包進物件，並加上 update_time
        output_data = {
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data": final_data
        }
        # 轉成字串
        json_str = json.dumps(output_data, ensure_ascii=False, separators=(',', ':'))
        # 寫入成 JavaScript 變數
        f.write(f"var myData = {json_str};")

    # 輸出 2: 保存包含時間戳的 JS 檔案 (前端載入用)
    output_payload = {
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data": final_data
    }
    with open(JS_FILE, 'w', encoding='utf-8') as f:
        f.write("var myData = ")
        json.dump(output_payload, f, ensure_ascii=False, separators=(',', ':'))
        f.write(";")

    print(f"=== 更新完成！總筆數: {len(final_data)} 筆 ===")

if __name__ == '__main__':
    main()

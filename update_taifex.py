import requests
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

JS_FILE = "taifex_data.js"
MAX_DAYS = 2500
CALENDAR_URL = "https://www.taifex.com.tw/cht/4/calendar"


def fetch_large_trader(start_date, end_date):
    all_rows = []
    cursor = datetime.strptime(start_date, "%Y/%m/%d")
    end_dt = datetime.strptime(end_date, "%Y/%m/%d")

    while cursor <= end_dt:
        chunk_end = min(cursor + timedelta(days=88), end_dt)
        s_str = cursor.strftime("%Y/%m/%d")
        e_str = chunk_end.strftime("%Y/%m/%d")
        print(f"[TAIFEX] 抓取區間: {s_str} ~ {e_str}")

        try:
            res = requests.post(
                "https://www.taifex.com.tw/cht/3/largeTraderFutDown",
                data={"queryStartDate": s_str, "queryEndDate": e_str},
                timeout=30,
            )
            text = res.content.decode("ms950", errors="ignore")
            lines = text.strip().splitlines()

            if len(lines) > 1 and "," in text:
                header = [h.strip() for h in lines[0].split(",")]
                for line in lines[1:]:
                    vals = [v.strip() for v in line.split(",")]
                    if len(vals) == len(header):
                        all_rows.append(dict(zip(header, vals)))
        except Exception as e:
            print(f"[TAIFEX] 錯誤: {e}")

        cursor = chunk_end + timedelta(days=1)
        time.sleep(1)

    return all_rows


def fetch_inst(symbol, start_date, end_date):
    s_str = start_date.replace("/", "-")
    e_str = end_date.replace("/", "-")
    print(f"[FinMind] 抓取 {symbol}: {s_str} ~ {e_str}")

    url = (
        "https://api.finmindtrade.com/api/v4/data"
        f"?dataset=TaiwanFuturesInstitutionalInvestors"
        f"&data_id={symbol}"
        f"&start_date={s_str}"
        f"&end_date={e_str}"
    )

    try:
        res = requests.get(url, timeout=30)
        data = res.json().get("data", [])
        return [d for d in data if "外資" in d.get("institutional_investors", "")]
    except Exception as e:
        print(f"[FinMind] 錯誤: {e}")
        return []


def to_int(value):
    try:
        return int(str(value).replace(",", "").strip())
    except Exception:
        return 0


def is_meaningful_trading_record(record):
    return any(key in record for key in ("inst_tx", "inst_mtx", "near", "allm"))


def fetch_official_trading_days(year):
    print(f"[Calendar] 讀取官方行事曆: {year}")
    try:
        res = requests.get(CALENDAR_URL, timeout=30)
        res.raise_for_status()
        text = res.text
    except Exception as e:
        print(f"[Calendar] 讀取失敗: {e}")
        return None

    if str(year) not in text and str(year - 1911) not in text:
        print(f"[Calendar] 頁面中未找到年份 {year}")

    holiday_matches = set(re.findall(rf"{year}/\d{{2}}/\d{{2}}", text))
    trading_days = set()

    dt = datetime(year, 1, 1)
    end_dt = datetime(year, 12, 31)
    while dt <= end_dt:
        d = dt.strftime("%Y/%m/%d")
        if dt.weekday() < 5 and d not in holiday_matches:
            trading_days.add(d)
        dt += timedelta(days=1)

    print(f"[Calendar] 推得交易日 {len(trading_days)} 天，休市日 {len(holiday_matches)} 天")
    return trading_days


def process_data(start_date, end_date, official_days=None):
    large_raw = fetch_large_trader(start_date, end_date)
    inst_tx_raw = fetch_inst("TX", start_date, end_date)
    inst_mtx_raw = fetch_inst("MTX", start_date, end_date)

    daily_data = {}

    for d in inst_tx_raw:
        date = d["date"].replace("-", "/")
        l = to_int(d.get("long_open_interest_balance_volume", 0))
        s = to_int(d.get("short_open_interest_balance_volume", 0))
        if date not in daily_data:
            daily_data[date] = {"date": date}
        daily_data[date]["inst_tx"] = {
            "long": l,
            "short": s,
            "net": l - s,
        }

    for d in inst_mtx_raw:
        date = d["date"].replace("-", "/")
        l = to_int(d.get("long_open_interest_balance_volume", 0))
        s = to_int(d.get("short_open_interest_balance_volume", 0))
        if date not in daily_data:
            daily_data[date] = {"date": date}
        daily_data[date]["inst_mtx"] = {
            "long": l,
            "short": s,
            "net": l - s,
        }

    tx_large = [
        r for r in large_raw
        if r.get("商品名稱", r.get("商品(契約)", "")).strip() == "TX"
        and r.get("身份別", r.get("交易人類別", "")).strip() == "0"
    ]

    for r in tx_large:
        date = r.get("日期", "").strip()
        month = r.get("到期月份(週別)", "").strip()

        if not date:
            continue

        if date not in daily_data:
            daily_data[date] = {"date": date}

        t5l = to_int(r.get("前五大交易人買方", 0))
        t5s = to_int(r.get("前五大交易人賣方", 0))
        t10l = to_int(r.get("前十大交易人買方", 0))
        t10s = to_int(r.get("前十大交易人賣方", 0))

        payload = {
            "top5_l": t5l,
            "top5_s": t5s,
            "top5_net": t5l - t5s,
            "top10_l": t10l,
            "top10_s": t10s,
            "top10_net": t10l - t10s,
        }

        if month == "999999":
            daily_data[date]["allm"] = payload
        elif len(month) == 6 and month not in ("666666", "999912"):
            if "near" not in daily_data[date] or month < daily_data[date].get("_near_month", "999999"):
                daily_data[date]["near"] = payload
                daily_data[date]["_near_month"] = month

    for date in daily_data:
        daily_data[date].pop("_near_month", None)

    rows = [d for d in daily_data.values() if is_meaningful_trading_record(d)]
    if official_days is not None:
        rows = [d for d in rows if d.get("date") in official_days]
    return rows


def load_old_data():
    if not os.path.exists(JS_FILE):
        return []

    try:
        with open(JS_FILE, "r", encoding="utf-8") as f:
            content = f.read().strip()

        if content.startswith("window.TAIFEX_META"):
            parts = content.split("window.TAIFEX_DATA = ", 1)
            if len(parts) == 2:
                content = parts[1].strip()
        else:
            prefixes = [
                "window.TAIFEX_DATA = ",
                "window.TAIFEXDATA = ",
                "var myData = ",
            ]
            for prefix in prefixes:
                if content.startswith(prefix):
                    content = content[len(prefix):]
                    break

        if content.endswith(";"):
            content = content[:-1]

        return json.loads(content)
    except Exception as e:
        print(f"[LOAD] 舊資料讀取失敗: {e}")
        return []


def save_js(data):
    tw_tz = timezone(timedelta(hours=8))
    updated_at = datetime.now(tw_tz).strftime("%Y/%m/%d %H:%M:%S")

    js_content = (
        "window.TAIFEX_META = "
        + json.dumps(
            {"updated_at": updated_at},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + ";\n"
        + "window.TAIFEX_DATA = "
        + json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + ";\n"
    )

    with open(JS_FILE, "w", encoding="utf-8") as f:
        f.write(js_content)


def main():
    print("=== 啟動台指期資料更新（官方交易日版）===")

    old_data = load_old_data()
    print(f"已讀取本地端資料: 共 {len(old_data)} 筆")

    tw_tz = timezone(timedelta(hours=8))
    now_tw = datetime.now(tw_tz)
    today = now_tw.strftime("%Y/%m/%d")
    end_date = today
    official_days = fetch_official_trading_days(now_tw.year)

    if official_days is None:
        print("無法取得官方行事曆，停止更新。")
        return

    if today not in official_days:
        print(f"今天 {today} 不在官方交易日名單中，跳過更新。")
        return

    if len(old_data) == 0:
        start_date = (now_tw - timedelta(days=3650)).strftime("%Y/%m/%d")
        print(f"首次執行，準備抓取 10 年資料: {start_date} ~ {end_date}")
    else:
        start_date = (now_tw - timedelta(days=5)).strftime("%Y/%m/%d")
        print(f"執行增量更新: {start_date} ~ {end_date}")

    new_data = process_data(start_date, end_date, official_days=official_days)
    print(f"本次成功抓取 {len(new_data)} 個官方交易日資料")

    if old_data:
        old_dates = {d["date"] for d in old_data}
        today_rows = [d for d in new_data if d.get("date") == today]
        truly_new_today_rows = [
            d for d in today_rows
            if d.get("date") not in old_dates
            or d != next((o for o in old_data if o.get("date") == d.get("date")), None)
        ]
        if not truly_new_today_rows:
            print(f"今天 {today} 沒有確認到新的交易資料，跳過寫入。")
            return

    data_dict = {d["date"]: d for d in old_data}
    for d in new_data:
        data_dict[d["date"]] = d

    final_data = sorted(list(data_dict.values()), key=lambda x: x["date"], reverse=True)

    if len(final_data) > MAX_DAYS:
        final_data = final_data[:MAX_DAYS]

    save_js(final_data)
    print(f"=== 更新完成！總筆數: {len(final_data)} 筆，輸出檔案: {JS_FILE} ===")


if __name__ == "__main__":
    main()

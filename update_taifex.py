import requests
import json
import os
import time
from datetime import datetime, timedelta

JSFILE = "taifex_data.js"
MAX_DAYS = 2500


def fetch_large_trader(start_date, end_date):
    all_rows = []
    cursor = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while cursor <= end_dt:
        chunk_end = min(cursor + timedelta(days=88), end_dt)
        sstr = cursor.strftime("%Y/%m/%d")
        estr = chunk_end.strftime("%Y/%m/%d")
        print("TAIFEX:", sstr, estr)

        try:
            res = requests.post(
                "https://www.taifex.com.tw/cht/3/largeTraderFutDown",
                data={
                    "queryStartDate": sstr,
                    "queryEndDate": estr
                },
                timeout=30
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
            print("TAIFEX ERROR:", e)

        cursor = chunk_end + timedelta(days=1)
        time.sleep(1)

    return all_rows


def fetch_inst(symbol, start_date, end_date):
    sstr = start_date.replace("-", "")
    estr = end_date.replace("-", "")
    print("FinMind:", symbol, sstr, estr)

    url = (
        f"https://api.finmindtrade.com/api/v4/data?"
        f"dataset=TaiwanFuturesInstitutionalInvestors&data_id={symbol}"
        f"&start_date={sstr}&end_date={estr}"
    )

    try:
        res = requests.get(url, timeout=30)
        data = res.json().get("data", [])
        return [
            d for d in data
            if "InstitutionalInvestors" not in d.get("institutional_investors", "")
        ]
    except Exception as e:
        print("FinMind ERROR:", e)
        return []


def process_data(start_date, end_date):
    large_raw = fetch_large_trader(start_date, end_date)
    inst_tx_raw = fetch_inst("TX", start_date, end_date)
    inst_mtx_raw = fetch_inst("MTX", start_date, end_date)

    daily_data = {}

    for d in inst_tx_raw:
        date = d["date"].replace("-", "")
        l = d.get("long_open_interest_balance_volume", 0)
        s = d.get("short_open_interest_balance_volume", 0)
        if date not in daily_data:
            daily_data[date] = {"date": date}
        daily_data[date]["insttx"] = {
            "long": l,
            "short": s,
            "net": l - s
        }

    for d in inst_mtx_raw:
        date = d["date"].replace("-", "")
        l = d.get("long_open_interest_balance_volume", 0)
        s = d.get("short_open_interest_balance_volume", 0)
        if date not in daily_data:
            daily_data[date] = {"date": date}
        daily_data[date]["instmtx"] = {
            "long": l,
            "short": s,
            "net": l - s
        }

    tx_large = [
        r for r in large_raw
        if r.get("商品名稱", "").strip() == "TX" and r.get("身份別", "").strip() == "0"
    ]

    for r in tx_large:
        date = r.get("日期", "").replace("/", "")
        month = r.get("到期月份(週別)", "").strip()

        if not date:
            continue

        if date not in daily_data:
            daily_data[date] = {"date": date}

        t5l = int(r.get("前五大交易人買方", 0) or 0)
        t5s = int(r.get("前五大交易人賣方", 0) or 0)
        t10l = int(r.get("前十大交易人買方", 0) or 0)
        t10s = int(r.get("前十大交易人賣方", 0) or 0)

        payload = {
            "top5l": t5l,
            "top5s": t5s,
            "top5net": t5l - t5s,
            "top10l": t10l,
            "top10s": t10s,
            "top10net": t10l - t10s
        }

        if month == "999999":
            daily_data[date]["allm"] = payload
        elif len(month) == 6 and month not in ("666666", "999912"):
            if (
                "near" not in daily_data[date]
                or month < daily_data[date].get("_near_month", "999999")
            ):
                daily_data[date]["near"] = payload
                daily_data[date]["_near_month"] = month

    for date in daily_data:
        daily_data[date].pop("_near_month", None)

    return list(daily_data.values())


def load_old_data():
    if not os.path.exists(JSFILE):
        return []

    try:
        with open(JSFILE, "r", encoding="utf-8") as f:
            content = f.read().strip()

        prefix = "window.TAIFEXDATA = "
        suffix = ";"

        if content.startswith(prefix):
            content = content[len(prefix):]
        if content.endswith(suffix):
            content = content[:-1]

        return json.loads(content)
    except Exception as e:
        print("LOAD OLD DATA ERROR:", e)
        return []


def save_js(data):
    js_content = "window.TAIFEXDATA = " + json.dumps(
        data,
        ensure_ascii=False,
        separators=(",", ":")
    ) + ";\n"

    with open(JSFILE, "w", encoding="utf-8") as f:
        f.write(js_content)


def main():
    old_data = load_old_data()
    print("OLD:", len(old_data))

    end_date = datetime.now().strftime("%Y-%m-%d")

    if len(old_data) == 0:
        start_date = (datetime.now() - timedelta(days=3650)).strftime("%Y-%m-%d")
        print("FULL UPDATE:", start_date, end_date)
    else:
        start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        print("INCREMENTAL UPDATE:", start_date, end_date)

    new_data = process_data(start_date, end_date)
    print("NEW:", len(new_data))

    data_dict = {d["date"]: d for d in old_data}
    for d in new_data:
        data_dict[d["date"]] = d

    final_data = sorted(
        list(data_dict.values()),
        key=lambda x: x["date"],
        reverse=True
    )

    if len(final_data) > MAX_DAYS:
        final_data = final_data[:MAX_DAYS]

    save_js(final_data)
    print("DONE:", len(final_data), "->", JSFILE)


if __name__ == "__main__":
    main()

import pandas as pd
import sqlite3
import os
from config import ORDERBOOK_DIR, KLINE_DIR, SQLITE_DB_PATH, MERGED_KLINE_PATH


def convert_csv_to_1m_kline(input_path, out_path, chunksize=200_000):
    kline_dict = {}

    for chunk in pd.read_csv(input_path, chunksize=chunksize):
        chunk["datetime"] = pd.to_datetime(chunk["timestamp"], unit="s", utc=True)
        chunk["utc_time"] = chunk["datetime"].dt.floor("1min")

        for (asset_id, utc_time), group in chunk.groupby(["asset_id", "utc_time"]):
            o = group["price"].iloc[0]
            h = group["price"].max()
            l = group["price"].min()
            c = group["price"].iloc[-1]
            v = group["token_amount"].sum()

            utc_time_str = utc_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            key = (asset_id, utc_time_str)

            if key not in kline_dict:
                kline_dict[key] = [o, h, l, c, v]
            else:
                kline_dict[key][1] = max(kline_dict[key][1], h)
                kline_dict[key][2] = min(kline_dict[key][2], l)
                kline_dict[key][3] = c
                kline_dict[key][4] += v

    rows = [[aid, t, *vals] for (aid, t), vals in kline_dict.items()]
    df_out = pd.DataFrame(rows, columns=["asset_id", "utc_time", "open", "high", "low", "close", "volume"])

    if not df_out.empty:
        df_out = df_out.sort_values(["asset_id", "utc_time"])
        df_out.to_csv(out_path, index=False, encoding="utf-8")


def batch_convert_to_kline():
    """ 批量將 Orderbook CSV 轉為 K 線 CSV """
    print("\n=== 開始轉換 K 線 ===")
    files = [f for f in os.listdir(ORDERBOOK_DIR) if f.endswith(".csv")]
    for file in files:
        input_path = ORDERBOOK_DIR / file
        output_path = KLINE_DIR / file
        convert_csv_to_1m_kline(input_path, output_path)
    print("K 線轉換完成。")


def merge_to_sqlite():
    """ 將 Orderbook CSV 匯入 SQLite """
    print("\n=== 匯入 SQLite ===")
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            asset_id TEXT, timestamp INTEGER, price REAL, 
            token_amount REAL, maker TEXT, taker TEXT
        );
    """)

    for file in os.listdir(ORDERBOOK_DIR):
        if not file.endswith(".csv"): continue

        path = ORDERBOOK_DIR / file
        try:
            df = pd.read_csv(path)
            if not df.empty:
                df.to_sql("trades", conn, if_exists="append", index=False)
        except Exception as e:
            print(f"SQLite Import Error {file}: {e}")

    conn.commit()
    conn.close()
    print(f"SQLite DB Ready: {SQLITE_DB_PATH}")


def merge_kline_csv():
    """ 合併所有 K 線 CSV 為一個大檔 """
    print("\n=== 合併 K 線 CSV ===")
    dfs = []
    for file in os.listdir(KLINE_DIR):
        if file.endswith(".csv"):
            dfs.append(pd.read_csv(KLINE_DIR / file))

    if dfs:
        merged_df = pd.concat(dfs, ignore_index=True).sort_values("utc_time")
        merged_df.to_csv(MERGED_KLINE_PATH, index=False)
        print(f"合併完成: {MERGED_KLINE_PATH}")
    else:
        print("沒有 K 線檔案可合併。")
import pandas as pd
import os
from config import SLUG_CSV_PATH, ORDERBOOK_DIR
from market_fetcher import fetch_market_ids
from orderbook_fetcher import fetch_and_convert_orderbook
from data_processor import batch_convert_to_kline, merge_to_sqlite, merge_kline_csv


def step_1_fetch_slugs():
    """ 若 csv 不存在則抓取 """
    if not os.path.exists(SLUG_CSV_PATH):
        fetch_market_ids()
    else:
        print(f"Slugs CSV 已存在，跳過抓取: {SLUG_CSV_PATH}")


def step_2_download_orderbooks():
    """ 讀取 Slugs CSV 並下載對應的 Orderbook """
    if not os.path.exists(SLUG_CSV_PATH):
        print("找不到 Slugs CSV，請先執行 Step 1")
        return

    df_tokens = pd.read_csv(SLUG_CSV_PATH)
    total = len(df_tokens)

    print(f"\n=== 開始下載 Orderbooks (共 {total} 筆紀錄) ===")

    for idx, row in df_tokens.iterrows():
        # 處理 Yes/No token
        for token_col in ["yes_token", "no_token"]:
            token = str(row.get(token_col, ""))
            if not token or token.lower() == "nan":
                continue

            out_path = ORDERBOOK_DIR / f"{token}.csv"

            # 若檔案已存在且不為空，可選擇跳過
            if out_path.exists() and out_path.stat().st_size > 0:
                print(f"[{idx + 1}/{total}] {token} 已存在，跳過。")
                continue

            print(f"[{idx + 1}/{total}] 下載中: {token} ...")
            try:
                df = fetch_and_convert_orderbook(token)
                if not df.empty:
                    df.to_csv(out_path, index=False)
                else:
                    print(f"  ⚠ 無數據: {token}")
            except Exception as e:
                print(f"  ❌ 錯誤: {e}")


def run_pipeline():
    # 1. 獲取市場 ID
    step_1_fetch_slugs()

    # 2. 下載交易數據
    step_2_download_orderbooks()

    # 3. 轉 K 線
    batch_convert_to_kline()

    # 4. (可選) 匯入資料庫
    merge_to_sqlite()

    # 5. (可選) 合併 K 線
    merge_kline_csv()


if __name__ == "__main__":
    run_pipeline()
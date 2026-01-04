import requests
import pandas as pd
import numpy as np
from flatten_json import flatten
import time
from config import GOLDSKY_API_URL


def fetch_by_role(asset_id, role="makerAssetId", limit=500):
    """
    功能：根據指定的角色（Maker 或 Taker）抓取該資產的所有歷史成交事件。
    機制：使用 timestamp 作為游標 (Cursor) 進行分頁 (Pagination)，直到抓完所有資料。
    """
    all_rows = []
    last_timestamp = 0  # 用於記錄最後一筆資料的時間戳，以便抓取下一頁

    while True:
        # 構建 GraphQL 查詢
        # 這裡查詢的是 orderFilledEvents (成交事件)
        # 篩選條件：角色欄位 (makerAssetId 或 takerAssetId) 等於目標 asset_id，且時間大於 last_timestamp
        query = {
            "query": f"""
            query MyQuery {{
              orderFilledEvents(
                orderBy: timestamp
                orderDirection: asc
                first: {limit}
                where: {{
                  {role}: "{asset_id}",
                  timestamp_gt: "{last_timestamp}"
                }}
              ) {{
                maker
                taker
                timestamp
                makerAssetId
                takerAssetId
                makerAmountFilled
                takerAmountFilled
              }}
            }}
            """
        }

        # 重試機制 (Retry Logic)
        # 如果請求失敗，最多重試 3 次，每次間隔 0.5 秒
        for _ in range(3):
            try:
                r = requests.post(GOLDSKY_API_URL, json=query, timeout=30)
                if r.status_code == 200:
                    break
            except:
                pass
            time.sleep(0.5)

        # 如果重試後請求仍然失敗，跳出迴圈
        if 'r' not in locals() or r.status_code != 200:
            break

        try:
            result = r.json()
        except:
            break

        # 提取資料
        rows = result.get("data", {}).get("orderFilledEvents", [])

        # 如果沒有資料了，代表已經抓取完畢，跳出迴圈
        if not rows:
            break

        # 將 JSON 資料展平並轉為 DataFrame
        df = pd.DataFrame([flatten(x) for x in rows])
        all_rows.append(df)

        # 更新 last_timestamp，為下一輪迴圈做準備
        if "timestamp" in df.columns:
            last_timestamp = df["timestamp"].astype(int).max()
        else:
            break

    # 如果完全沒有抓到資料，回傳空的 DataFrame
    if not all_rows:
        return pd.DataFrame()

    # 合併所有分頁的資料
    return pd.concat(all_rows, ignore_index=True)


def fetch_and_convert_orderbook(asset_id):
    """
    主要邏輯函數：
    1. 分別抓取該資產作為 Maker 和 Taker 的成交紀錄。
    2. 識別哪一方是用 USDC (Asset '0') 交易，哪一方是交易該 Token。
    3. 計算每一筆成交的實際價格 (Price)。
    """

    # Step 1：抓取資料
    # 分別抓取「該資產是 Maker 掛單資產」和「該資產是 Taker 吃單資產」的情況
    df_maker = fetch_by_role(asset_id, role="makerAssetId")
    df_taker = fetch_by_role(asset_id, role="takerAssetId")

    # 合併兩邊的資料
    df = pd.concat([df_maker, df_taker], ignore_index=True)
    if df.empty:
        return df

    # Step 2：數值轉換
    # Polymarket 或多數區塊鏈數據通常有 6 位小數精度 (1e6)，需除以 1,000,000 轉為正常數值
    df["makerAmountFilled"] = df["makerAmountFilled"].astype(float) / 1e6
    df["takerAmountFilled"] = df["takerAmountFilled"].astype(float) / 1e6
    df["timestamp"] = df["timestamp"].astype(int)

    # Step 3：識別 Token 數量 (計算 Token Amount)
    # 邏輯核心：判斷哪一個 ID 是 USDC (通常 ID 為 "0")
    # 如果 takerAssetId 是 "0" (USDC)，代表 Taker 用錢買 Token -> Maker 提供的就是 Token
    is_taker_usdc = df["takerAssetId"] == "0"

    # np.where(條件, 條件成立時的值, 條件不成立時的值)
    df["token_amount"] = np.where(
        is_taker_usdc,
        df["makerAmountFilled"],  # Taker 付錢，Maker 給 Token
        df["takerAmountFilled"]  # Maker 付錢 (掛買單)，Taker 給 Token
    )

    # Step 4：計算價格 (Calculate Price)
    # 找出哪邊是 USDC 金額
    df["usdc_amount"] = np.where(
        is_taker_usdc,
        df["takerAmountFilled"],  # Taker 付的是 USDC
        df["makerAmountFilled"]  # Maker 付的是 USDC
    )

    # 價格 = USDC 總額 / Token 總數
    # 加入防呆機制避免除以零
    df["price"] = np.where(
        df["token_amount"] != 0,
        df["usdc_amount"] / df["token_amount"],
        0
    )

    # 標記目前的 Asset ID
    df["asset_id"] = asset_id

    # Step 5：輸出清理後的資料
    # 只保留重要欄位，並按時間排序
    df_final = df[[
        "asset_id", "timestamp", "price", "token_amount", "maker", "taker"
    ]].sort_values("timestamp").reset_index(drop=True)

    return df_final
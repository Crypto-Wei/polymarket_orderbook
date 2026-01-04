import requests
import json
import csv
import pandas as pd
import os
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
# 載入設定檔變數
from config import GAMMA_API_URL, SLUG_CSV_PATH, START_DATE, END_DATE


# --- 輔助函式：時間格式轉換 ---

def hour_to_ampm(h: int) -> str:
    """
    將 24 小時制的整數 (0-23) 轉換為 12 小時制字串 (例如: 12am, 5pm)。
    用於構建 API 的查詢 slug。
    """
    if h == 0:
        return "12am"
    elif h == 12:
        return "12pm"
    elif h < 12:
        return f"{h}am"
    else:
        return f"{h - 12}pm"


def ampm_to_24h(hour_str: str) -> int:
    """
    將 12 小時制字串 (例如: 5pm) 轉回 24 小時制整數 (例如: 17)。
    用於從 CSV 讀取舊資料時還原時間。
    """
    hour_str = hour_str.lower().strip()
    if hour_str.endswith("am"):
        h = int(hour_str.replace("am", ""))
        return 0 if h == 12 else h
    elif hour_str.endswith("pm"):
        h = int(hour_str.replace("pm", ""))
        return 12 if h == 12 else h + 12
    else:
        raise ValueError(f"Invalid hour format: {hour_str}")


def et_to_utc_iso(d: date, hour_str: str) -> str:
    """
    將美東時間 (ET) 轉換為世界協調時間 (UTC) 的 ISO 格式字串。

    關鍵邏輯：
    1. 建立美東時間的 Timestamp。
    2. 使用 tz_localize 處理時區，並設定 ambiguous=True 以處理日光節約時間切換時的模糊時間點。
    3. 轉換為 UTC 時區。
    """
    hour_24 = ampm_to_24h(hour_str)
    # 建立無時區的時間戳
    dt_et = pd.Timestamp(d) + pd.Timedelta(hours=hour_24)
    # 定位為美東時間，處理 DST
    dt_et = dt_et.tz_localize(
        ZoneInfo("America/New_York"),
        ambiguous=True,
        nonexistent="shift_forward"
    )
    # 轉為 UTC 並輸出字串
    return dt_et.tz_convert(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- 核心功能：斷點續傳邏輯 ---

def get_last_processed_info():
    """
    讀取現有 CSV 的最後一行，以判斷上次執行到哪裡。

    回傳:
        tuple (last_date_obj, last_hour_int) 若檔案存在且有資料。
        None 若檔案不存在或為空。
    """
    if not os.path.exists(SLUG_CSV_PATH):
        return None

    try:
        with open(SLUG_CSV_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()

            # 如果只有標頭或空檔，視為新檔案
            if len(lines) < 2:
                return None

            last_line = lines[-1].strip()
            if not last_line:
                return None

            # CSV 欄位順序: date, hour, slug, yes_token, no_token, utc_time
            parts = last_line.split(",")
            last_date_str = parts[0]
            last_hour_str = parts[1]

            # 解析日期與時間
            last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
            last_hour = ampm_to_24h(last_hour_str)

            return last_date, last_hour
    except Exception as e:
        print(f"讀取舊檔案發生錯誤，將重新開始: {e}")
        return None


# --- 主程式 ---

def fetch_market_ids():
    # 從 config 讀取設定的起訖日期
    config_start = date(*START_DATE)
    config_end = date(*END_DATE)

    # 步驟 1: 檢查是否有舊進度 (斷點續傳)
    last_info = get_last_processed_info()

    if last_info:
        last_date, last_h = last_info
        print(f"發現舊資料，最後記錄為: {last_date} {hour_to_ampm(last_h)}")

        # 計算「接續點」：如果是當天最後一小時 (23點)，則從隔天 0 點開始
        if last_h == 23:
            current_date = last_date + timedelta(days=1)
            start_hour_idx = 0
        else:
            # 否則從當天的下一小時開始
            current_date = last_date
            start_hour_idx = last_h + 1

        file_mode = "a"  # 設定為追加模式 (Append)
        print(f"將從 {current_date} 的 {hour_to_ampm(start_hour_idx)} 繼續抓取...")
    else:
        # 無舊資料，從頭開始
        current_date = config_start
        start_hour_idx = 0
        file_mode = "w"  # 設定為寫入模式 (Write/Overwrite)
        print(f"從頭開始抓取市場 ID，從 {config_start} 到 {config_end} ...")

    # 防呆機制：若接續日期已超過設定結束日期，則直接結束
    if current_date > config_end:
        print("所有日期已抓取完畢，無須執行。")
        return

    # 步驟 2: 開啟檔案進行寫入
    # 使用 newline='' 避免在 Windows 上產生多餘空行
    with open(SLUG_CSV_PATH, file_mode, newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # 只有在新檔案模式下才寫入標頭，避免在檔案中間重複插入
        if file_mode == "w":
            writer.writerow(["date", "hour", "slug", "yes_token", "no_token", "utc_time"])

        count = 0

        # 步驟 3: 日期迴圈
        while current_date <= config_end:
            month = current_date.strftime("%B").lower()  # 例如: october
            day = current_date.day

            # 判斷當天從幾點開始抓取
            if count == 0 and file_mode == "a":
                # 這是「接續執行」的第一天，使用計算出的接續點
                range_start = start_hour_idx
            else:
                # 其他所有日期 (或是全新執行)，都從 0 點開始
                range_start = 0

            # 步驟 4: 小時迴圈
            for h in range(range_start, 24):
                hour_str = hour_to_ampm(h)
                # 拼湊 API 查詢用的 slug
                slug = f"bitcoin-up-or-down-{month}-{day}-{hour_str}-et"

                yes_token, no_token = None, None
                utc_time = ""

                try:
                    # 發送 API 請求
                    resp = requests.get(GAMMA_API_URL, params={"slug": slug}, timeout=20)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data:
                            # 解析回傳的 JSON 取得 Token IDs
                            market = data[0]
                            ids = json.loads(market.get("clobTokenIds", "[]"))
                            if len(ids) > 0: yes_token = ids[0]  # Yes Token
                            if len(ids) > 1: no_token = ids[1]  # No Token

                    # 計算 UTC 時間 (用於資料庫或分析)
                    utc_time = et_to_utc_iso(current_date, hour_str)

                    # 寫入 CSV
                    writer.writerow([str(current_date), hour_str, slug, yes_token, no_token, utc_time])

                    count += 1

                except Exception as e:
                    # 簡單的錯誤捕捉，避免單一請求失敗中斷整個程序
                    print(f"ERROR fetching {slug}: {e}")

            print(f"已處理日期: {current_date} (當日起始小時: {range_start})")

            # 日期推進
            current_date += timedelta(days=1)
            # 重置起始小時標記 (確保下一天從 0 點開始)
            start_hour_idx = 0

    print(f"\n抓取完成。本次執行共新增 {count} 筆。路徑: {SLUG_CSV_PATH}")


if __name__ == "__main__":
    fetch_market_ids()
import os
from pathlib import Path

# --- 路徑設定 ---
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "Data"
SLUG_CSV_PATH = DATA_DIR / "slugs_tokens_24h.csv"
ORDERBOOK_DIR = DATA_DIR / "orderbook_converted"
KLINE_DIR = DATA_DIR / "kline_1m"
SQLITE_DB_PATH = DATA_DIR / "orderbook_all.sqlite"
MERGED_KLINE_PATH = DATA_DIR / "kline_1m_merged.csv"

# 確保資料夾存在
for p in [DATA_DIR, ORDERBOOK_DIR, KLINE_DIR]:
    p.mkdir(parents=True, exist_ok=True)

# --- API 設定 ---
GAMMA_API_URL = "https://gamma-api.polymarket.com/markets"
GOLDSKY_API_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"

# --- 參數設定 ---
# 抓取 token 的時間範圍
START_DATE = (2025, 7, 1)  # (Year, Month, Day)
END_DATE = (2025, 12, 1)
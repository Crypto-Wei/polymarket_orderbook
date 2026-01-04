# Polymarket Historical Data Pipeline

這是一個自動化的數據管道工具，專門設計用於抓取、處理並分析 **Polymarket** 預測市場（以 Bitcoin 漲跌預測為例）的歷史交易數據。

本專案串接 Gamma API 與 Goldsky Subgraph (GraphQL)，將鏈上原始交易紀錄清洗並轉換為金融分析常用的 **1 分鐘 K 線 (OHLCV)** 格式。

---

## 📂 程式碼檔案詳細說明 (File Descriptions)

本專案由 5 個主要 Python 檔案組成，各檔案功能與邏輯如下：

### 1. `market_fetcher.py` (市場列表抓取器)
**功能**：負責從 Polymarket Gamma API 獲取所有目標市場的識別碼（Token IDs）。
* **自動化 Slugs 生成**：依據日期與小時（例如 `october-10-12pm-et`）自動拼湊查詢網址。
* **斷點續傳 (Resumable)**：程式會檢查 `slugs_tokens_24h.csv` 的最後一行。若程式中斷，下次執行時會自動從「中斷的那一天與小時」繼續抓取，無需重新開始。
* **時區處理**：將 API 預設的美東時間 (ET) 轉換為標準 UTC ISO 格式，方便後續資料庫存儲。

### 2. `orderbook_fetcher.py` (交易數據下載與清洗)
**功能**：核心爬蟲，負責從區塊鏈數據源 (Goldsky GraphQL) 下載原始成交紀錄並進行數值還原。
* **GraphQL 分頁機制**：透過 `timestamp` 作為游標 (Cursor)，支援下載超過單次 API 限制的大量歷史交易數據。
* **Maker/Taker 雙向抓取**：同時抓取資產作為 Maker (掛單) 與 Taker (吃單) 的事件。
* **智能價格計算**：
    * 自動識別 Asset ID `"0"` 為 **USDC**。
    * 判斷交易方向：若 Taker 付出 USDC，則為買入 Token；若 Taker 付出 Token，則為賣出。
    * **公式**：`Price = USDC Amount / Token Amount`。
* **精度修正**：自動將鏈上數據 (通常為 1e6) 除以 $1,000,000$ 轉換為人類可讀數值。

### 3. `data_processor.py` (數據加工與轉換)
**功能**：將清洗後的 Tick 級別交易數據，聚合為時間序列數據 (K線)。
* **K 線合成 (OHLCV)**：將原始交易按「每 1 分鐘」進行分組，計算開盤價 (Open)、最高價 (High)、最低價 (Low)、收盤價 (Close) 與成交量 (Volume)。
* **批量處理**：`batch_convert_to_kline` 函式會遍歷目錄下所有 CSV 進行轉換。
* **SQLite 整合**：提供 `merge_to_sqlite` 功能，可將所有散落的 CSV 匯入單一 SQLite 資料庫 (`orderbook_all.sqlite`)，便於使用 SQL 進行複雜查詢。
* **資料合併**：提供 `merge_kline_csv` 功能，將所有單檔 K 線合併為一個大檔 (`kline_1m_merged.csv`)。

### 4. `main.py` (主程式入口)
**功能**：專案的指揮中心，負責串聯上述所有模組。
* **Pipeline 流程控制**：
    1.  **Step 1**: 呼叫 `market_fetcher` 獲取市場列表。
    2.  **Step 2**: 讀取列表，呼叫 `orderbook_fetcher` 下載每個 Token 的交易數據（支援跳過已下載檔案）。
    3.  **Step 3**: 呼叫 `data_processor` 將數據轉為 K 線圖。
    4.  **Step 4**: (可選) 執行資料合併或資料庫匯入。

### 5. `config.py` (全域設定檔)
**功能**：集中管理專案的所有參數與路徑，方便調整。
* **路徑設定**：定義數據輸出資料夾 (`Data/`) 及其子目錄結構。
* **API 端點**：Gamma API 與 Goldsky Subgraph 的 URL。
* **抓取範圍**：透過 `START_DATE` 與 `END_DATE` 控制要抓取的歷史數據時間範圍。

---

## 🚀 安裝與使用 (Quick Start)

### 1. 安裝相依套件
請確保已安裝 Python 3.9+，並執行以下指令安裝所需套件：

```bash
pip install pandas numpy requests flatten-json

```

### 2. 調整設定 (可選)

打開 `config.py` 修改抓取日期範圍：

```python
# config.py
START_DATE = (2025, 7, 1)  # 開始日期
END_DATE = (2025, 12, 1)   # 結束日期

```

### 3. 執行程式

執行主程式即可啟動完整的數據流水線：

```bash
python main.py

```

---

## 📊 輸出資料結構

程式執行後，會在 `Data/` 目錄下產生以下結構：

```text
Data/
├── slugs_tokens_24h.csv     # 市場列表 (包含 Token IDs)
├── orderbook_converted/     # 原始交易數據 CSV (清洗後的 Tick Data)
├── kline_1m/                # 1 分鐘 K 線數據 CSV (OHLCV)
└── kline_1m_merged.csv      # 合併後的大檔 (所有市場的 K 線)

```

### K 線欄位說明

| 欄位 | 說明 |
| --- | --- |
| `asset_id` | Token 的唯一識別碼 |
| `utc_time` | 時間 (UTC ISO 格式) |
| `open` | 開盤價 |
| `high` | 最高價 |
| `low` | 最低價 |
| `close` | 收盤價 |
| `volume` | 成交量 (Token 數量) |

```

```
# Polymarket Crypto Mispricing Trader

目前只保留單一主線版本，正式入口只有兩個：

- `run_research_pipeline.py`：研究掃描模式
- `run_auto_trading.py`：研究 / 自動交易模式

舊版單體入口、舊 dashboard、SDK 腳本與一次性診斷腳本都不再屬於正式版本。

## 策略主線

系統固定走這條路徑：

1. 用 Gamma / CLOB 發現加密市場
2. scanner 仍可解析多種市場，但 v1 研究與交易只保留 `UP / DOWN` 且週期屬於 `5m / 15m / 1h / 4h / 1d`
3. 依市場結算來源抓取開盤錨點，並用即時現貨價格與短窗波動率估算尾盤勝率
4. 用 CLOB 最佳買賣價、費用、滑價與成交懲罰計算 `net_edge`
5. 研究模式寫入 SQLite `observations` 與 `opening_anchors`
6. `live` 模式依 timeframe 採 maker-first 或 aggressive taker 策略後交由官方 CLOB SDK 下單
7. 啟用自動領取時，系統會掃描 redeemable positions，並透過官方 relayer 送出 `redeemPositions`

## 生產版範圍

目前正式版只支援：

- 單機 VPS 常駐部署
- `Proxy/Funder` 帳戶模型
- 檔案日誌觀測
- `UP / DOWN` 尾盤單策略

不支援：

- `EOA` / `Safe` 多帳戶泛化交易主線
- Docker-first 部署
- 外部告警整合
- 盈利保證

## 安裝

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 環境變數

研究模式通常不需要私鑰；正式版 `live` 模式至少需要以下變數：

```bash
POLYMARKET_API_KEY=your_api_key
POLYMARKET_API_SECRET=your_api_secret
POLYMARKET_API_PASSPHRASE=your_passphrase
WALLET_PRIVATE_KEY=0xyour_private_key
FUNDER_ADDRESS=0xyour_funder
WALLET_ADDRESS=0xyour_wallet_optional

# 正式版檔案日誌
POLY_LOG_DIR=./logs
POLY_LOG_LEVEL=INFO
POLY_PREFLIGHT_TIMEOUT=20

# 自動領取：擇一配置 Builder 憑證或 Relayer API Key
POLY_BUILDER_API_KEY=your_builder_key
POLY_BUILDER_API_SECRET=your_builder_secret
POLY_BUILDER_API_PASSPHRASE=your_builder_passphrase

RELAYER_API_KEY=your_relayer_api_key
RELAYER_API_KEY_ADDRESS=0xyour_relayer_key_owner

# 可選 host 覆寫
POLY_RELAYER_HOST=https://relayer-v2.polymarket.com
POLY_DATA_API_HOST=https://data-api.polymarket.com
POLY_RPC_URL=https://polygon-rpc.example
```

約束：

- `FUNDER_ADDRESS` 必須等於 signer 推導出的 proxy wallet
- `WALLET_ADDRESS` 改為可選顯示值，不再是交易必填
- `live` 啟動前一定要先跑 `--preflight-only`

## 使用方式

研究模式：

```bash
python3 run_research_pipeline.py --styles up_down --timeframes 5m,15m,1h,4h,1d --tail-mode adaptive --min-edge 0.03 --top 20
```

自動交易單輪：

```bash
python3 run_auto_trading.py --mode live --styles up_down --timeframes 5m,15m,1h,4h,1d --tail-mode adaptive --max-candidates 3
```

自動交易 + 自動領取：

```bash
python3 run_auto_trading.py --mode live --styles up_down --continuous --scan-interval 60 --enable-auto-claim --claim-interval 300
```

低風險常駐範例：

```bash
python3 run_auto_trading.py --mode live --continuous --styles up_down --timeframes 5m,15m,1h,4h,1d --assets BTC,ETH,SOL --limit-events 30 --min-position-usdc 1 --max-position-usdc 3 --min-marketable-buy-usdc 1 --max-candidates 1 --scan-interval 10 --enable-auto-claim --claim-interval 300
```

只掃描可領取倉位，不提交交易：

```bash
python3 run_auto_trading.py --mode live --claim-dry-run
```

正式版啟動前檢查：

```bash
python3 run_auto_trading.py --mode live --preflight-only --log-dir ./logs
```

持續輪詢：

```bash
python3 run_auto_trading.py --mode research --styles up_down --continuous --scan-interval 60
```

## 主要輸出

- 終端掃描摘要
- `logs/lifecycle.log`
- `logs/preflight.log`
- `logs/candidate.log`
- `logs/order.log`
- `logs/fill.log`
- `logs/claim.log`
- `logs/error.log`
- 可選 JSON 機會清單
- `research_signals_YYYYMMDD.db` SQLite 觀測資料
- `opening_anchors` 開盤錨點資料
- `live` 模式下的訂單執行摘要
- 啟用自動領取時的 `settlement_claims` 領取追蹤資料

## 正式啟動流程

1. 複製 `.env.example` 為 `.env`，填入正式憑證
2. 先執行 `python3 run_auto_trading.py --mode live --preflight-only`
3. 確認 `preflight status=ready`
4. 再以 `systemd` 啟動常駐服務

## systemd 部署

已提供範本：

- `deploy/systemd/polymarket-arbitrage.service`

部署時請調整：

- `User`
- `WorkingDirectory`
- `EnvironmentFile`
- Python 虛擬環境路徑

常用指令：

```bash
sudo cp deploy/systemd/polymarket-arbitrage.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable polymarket-arbitrage
sudo systemctl start polymarket-arbitrage
sudo systemctl status polymarket-arbitrage
sudo systemctl restart polymarket-arbitrage
sudo systemctl stop polymarket-arbitrage
```

## 測試

安裝 `requirements.txt` 後，可直接執行：

```bash
python3 -m pytest -q
```

## 風險提示

`--mode live` 會實際下單。本專案僅供研究與工程驗證使用，不保證獲利，也不構成投資建議。

# 內部介面文件

## 說明

本專案目前正式交易主線仍只保留兩個 CLI 入口與其內部資料模型，不提供對外交易 REST API。
v1 正式主線為 `UP / DOWN` 尾盤錯價策略；`ABOVE / BELOW` 既有能力仍保留於底層模組，但不參與本輪新定價與新執行邏輯。
另可提供一個本地只讀監控 Web，僅用於查看執行狀態與日誌，不提供任何交易寫操作。

## 依賴基線

- 執行主線依賴需至少包含 `scipy`、`py-clob-client`
- 測試基線需包含 `pytest`
- 自動領取基線需包含 `py-builder-relayer-client`、`py-builder-signing-sdk`

## 環境變數

### 交易主線

- `POLYMARKET_API_KEY`
- `POLYMARKET_API_SECRET`
- `POLYMARKET_API_PASSPHRASE`
- `WALLET_PRIVATE_KEY`
- `FUNDER_ADDRESS`
- `WALLET_ADDRESS`
- `POLY_LOG_DIR`
- `POLY_LOG_LEVEL`
- `POLY_PREFLIGHT_TIMEOUT`

補充：

- 正式版只支援 `Proxy/Funder` 帳戶模型
- `FUNDER_ADDRESS` 必須等於 signer 推導出的 proxy wallet
- `WALLET_ADDRESS` 為可選覆寫或顯示用途，不再是交易必填
- `POLY_LOG_DIR` 用於正式版檔案日誌輸出目錄

### 自動領取主線

- `POLY_BUILDER_API_KEY`
- `POLY_BUILDER_API_SECRET`
- `POLY_BUILDER_API_PASSPHRASE`
- `RELAYER_API_KEY`
- `RELAYER_API_KEY_ADDRESS`
- `POLY_CLAIM_RELAYER_TYPE`
- `POLY_RELAYER_HOST`
- `POLY_DATA_API_HOST`
- `POLY_RPC_URL`

補充：

- `POLY_CLAIM_RELAYER_TYPE`
  - 可選：`auto`、`safe`、`proxy`
  - 預設：`auto`
  - 說明：自動領取提交策略；`auto` 會依 signer 推導出的 safe / proxy wallet 與 `claim_account` 自動判斷
- claim 提交認證順序固定為：
  - 先使用 `RELAYER_API_KEY` + `RELAYER_API_KEY_ADDRESS`
  - 若 relayer API key 缺失，才退回 `POLY_BUILDER_API_*`
  - 若兩者都缺失，preflight / submit 必須直接失敗
- `RELAYER_API_KEY_ADDRESS`
  - 必須是 relayer key 綁定的 signer / key owner 地址
  - 不能填 `FUNDER_ADDRESS`、proxy wallet 或 safe 地址
  - 若 `/submit` 回 `401 {"error":"invalid authorization"}`，需分開檢查 relayer API key header 與 Builder header，避免把兩組授權問題混在一起

### Live 訂單終態兼容

- `poll_order_status()` 需兼容交易所狀態別名：
  - `FILLED`、`MATCHED`：視為成交終態
  - `CANCELED`、`CANCELLED`：視為取消終態
  - `EXPIRED`：視為過期終態
- `OPEN`、`LIVE`、`UNMATCHED`、未知非終態：視為仍在掛單中的 pending 狀態
- 成交量解析優先順序：
  - `size_matched`
  - `takerAmount`
- 成交價格沿用 `price` 欄位；若手續費欄位缺失，可先記 `0`
- 若 pending 訂單自 `created_at` 起超過 `order_timeout_seconds` 仍未進入終態，`poll_order_status()` 需主動呼叫取消流程，將本地狀態改為 `CANCELLED`，並釋放持久化的 pending / directional exposure
- `AutoTradingPipeline.run_cycle()` 在 live 模式送出新單後，需於短延遲（預設約 `2` 秒）後立即再呼叫一次 `poll_order_status(order_id)`；若已有遠端終態或部分成交，應以最新輪詢結果覆蓋本輪 execution summary
- `5m / up_down` v1 固定止盈基線：
  - 僅對 BUY 已成交後建立的 managed position 生效
  - 固定 `take_profit_roi = 1.0`
  - v1 不提供 CLI / env 覆寫，先以內部常數實作
- `poll_order_status()` 的 entry / exit 語義：
  - `order_intent=entry`
    - `FILLED`：建立 managed position，保留 directional exposure
    - `CANCELLED / EXPIRED / timeout`：釋放 directional exposure
  - `order_intent=exit`
    - `FILLED`：關閉 managed position，釋放 directional exposure
    - `CANCELLED / EXPIRED / timeout`：managed position 回到 `open`，directional exposure 保留

### Managed Position 介面約束

- `LiveExecutor` 需新增 `monitor_take_profit_positions() -> List[LiveExecutionResult]`
  - 讀取所有 `status=open` 的 managed positions
  - 用最新 `best_bid` 計算當前浮盈 ROI
  - 若 `roi >= take_profit_roi`，提交 SELL exit 訂單
  - 回傳本輪新建立的 exit executions
- `LiveExecutor` 需新增 `get_managed_positions()` 供主迴圈與測試讀取目前受管持倉
- `AutoTradingPipeline.run_cycle()` 在 live 模式下，完成 pending order 輪詢後，需先呼叫一次 `monitor_take_profit_positions()`，再進入 research 掃描

### SQLite Runtime State 契約

- `live_pending_orders` 需兼容以下欄位：
  - `order_id TEXT PRIMARY KEY`
  - `market_id TEXT NOT NULL`
  - `observation_id TEXT NOT NULL`
  - `asset TEXT NOT NULL`
  - `token_id TEXT`
  - `side TEXT NOT NULL`
  - `size REAL NOT NULL`
  - `price REAL NOT NULL`
  - `status TEXT NOT NULL`
  - `created_at TEXT NOT NULL`
  - `filled_at TEXT`
  - `exposure_key TEXT`
  - `raw_response_json TEXT`
  - `order_intent TEXT NOT NULL DEFAULT 'entry'`
  - `position_id TEXT`
  - `submitted_shares REAL NOT NULL DEFAULT 0`
- `live_managed_positions` 需新增：
  - `position_id TEXT PRIMARY KEY`
  - `market_id TEXT NOT NULL`
  - `observation_id TEXT NOT NULL`
  - `asset TEXT NOT NULL`
  - `side TEXT NOT NULL`
  - `token_id TEXT NOT NULL`
  - `shares REAL NOT NULL`
  - `entry_price REAL NOT NULL`
  - `entry_fee_paid REAL NOT NULL`
  - `entry_cost REAL NOT NULL`
  - `take_profit_roi REAL NOT NULL`
  - `exposure_key TEXT NOT NULL`
  - `status TEXT NOT NULL`
  - `opened_at TEXT NOT NULL`
  - `updated_at TEXT NOT NULL`
  - `exit_order_id TEXT`
  - `closed_at TEXT`
- 舊資料庫若缺少上述新增欄位，`LiveExecutor._init_runtime_state_tables()` 需用 additive migration 補齊，不得要求人工重建 SQLite
- `token_id` 必須落在 pending order runtime state 中，避免程序重啟後 entry order 成交時無法建立可賣出的 managed position

### 止盈估值與 SELL 下單契約

- 止盈估值來源：
  - 先讀 `RealtimeOrderBookCache.get_cached_orderbook(token_id, max_age_seconds=3.0)`
  - 若無新鮮快取，再 fallback `py-clob-client.get_order_book(token_id)`
  - SELL 價格一律使用最新 `best_bid`
- 浮盈 ROI：
  - `mark_value = shares * best_bid`
  - `roi = (mark_value - entry_cost) / entry_cost`
- EXIT 訂單：
  - `OrderArgs.side` 必須為 `SELL`
  - `OrderArgs.size` 必須直接使用持有股數 `shares`
  - `position_id` 需回寫到 pending order context，供後續 `poll_order_status()` 在 FILLED 後關閉對應持倉

### 進場最小金額 Fallback 契約

- `LiveExecutor` 需統一提供「最小金額 fallback」能力，覆蓋：
  - 一般 `should_execute()` 路徑
  - `UP_DOWN` 的 `_execute_tail_candidate()` 路徑
- 規則如下：
  - 先算策略原始目標金額 `raw_amount`
  - 若 `raw_amount < min_position_per_trade`，則將下單金額提升為 `min_position_per_trade`
  - 若提升後仍低於 `5 shares * order_price`，則再提升至 `5 shares` 對應的最低金額
  - 若提升後仍低於 `min_marketable_buy_notional` 且屬可立即成交 BUY，則再提升至 `min_marketable_buy_notional`
  - 提升後再做 `check_risk_limits()` 與餘額/最大單筆檢查
- `UP_DOWN` 的 `bucket_amount` 不能再作為「低於 min_position 就直接 reject」的硬門檻
  - 它只能限制策略原始金額的上限
  - 最終下單金額可因 fallback 被提升到 `min_position_per_trade`
- 若 fallback 後的目標金額超過：
  - `account.available_capital`
  - `max_position_per_trade`
  - 或其他硬風控限制
  - 才可回傳拒絕

### Live 最小成交額約束

- CLOB 對可立即成交的 `BUY` 單存在最小 notional 限制
- 2026-03-29 實測：`$0.4` 的 marketable `BUY` 與 `limit + FOK BUY` 都被拒絕，錯誤為 `invalid amount for a marketable BUY order ($0.4), min size: $1`
- 因此 micro smoke 若要求驗證 taker / FOK 成交，實際 notional 不得低於 `$1`
- 執行層需在本地顯式檢查此限制，並回傳結構化拒絕原因，避免重複向交易所送出必敗請求
- 若產品側仍要求 `<=0.5 USDC`，需改為：
  - 接受此 smoke 無法做即時成交驗證
  - 或改走非 marketable maker 掛單驗證，但不保證當輪成交

### 市場識別約束

- discovery 主來源需優先改為官方 Gamma `GET /markets`
- 建議查詢參數基線為：
  - `active=true`
  - `closed=false`
  - `archived=false`
  - `enableOrderBook=true`
  - `liquidity_num_min=1`
  - `order=volume`
  - `ascending=false`
- 若 CLI 的 `--limit-events` 偏小，scanner 可在內部放大 `/markets` 拉取窗口，再以本地 parse / asset / style 規則收斂，避免小樣本剛好沒有 `BTC / ETH / SOL up_down`
- 官方 Gamma `GET /events` 查詢改為 fallback，用於 `/markets` discovery 不可用或回傳空集合時的保守退路
- `GET /events` fallback 參數基線為：
  - `tag_slug=crypto`
  - `related_tags=true`
  - `active=true`
  - `closed=false`
  - `archived=false`
  - `order=endDate`
  - `ascending=true`
- 若 CLI / runtime 只允許 `up_down`，需在 events 展開為 markets 時先以前置 style 檢查過濾，只保留 `UP_DOWN` 題型，不再讓 `ABOVE_BELOW` 或 `UNKNOWN` 進入 parse
- 若 CLI / runtime 只允許 `up_down`，discovery 需合併兩個來源：
  - `/markets`：保留 `order=volume&ascending=false`，提供可交易熱門樣本
  - `/events`：使用 `tag_slug=crypto&related_tags=true&order=endDate&ascending=true`，補足近端即將到期市場
- 合併後需按 `market_id` 去重，且優先保留 `/markets` 來源的 market payload
- `get_all_events()` 在合併前必須先做 discovery-time UTC 時間清洗：
  - `market.endDate` 為主，`event.endDate` 為 fallback
  - 若 payload 解析出的 `expiry <= datetime.now(timezone.utc)`，即使遠端仍標記 `active=true` / `closed=false`，也必須直接剔除
  - 過期 payload 不得進入 `expand_markets()`、`parse_market()`、`check_tradability()` 與後續研究拒絕漏斗
- 若 CLI / runtime 只允許 `up_down`，research 主線在 scanner / research 交界只應前置過濾已過期市場；`window_state=observe` 的開盤市場仍需進入 `_analyze_market()`
- 若 `UP_DOWN` 市場存在 `market_start_timestamp` 且其值大於目前時間，需以前置拒絕 `market_not_open_yet` 記錄，不得進入 anchor 抓取階段並混入 `anchor_unavailable`
- 研究層的資產識別需優先使用完整詞、價格語義與常見 ticker 邊界匹配
- 不得用無邊界子字串將一般英文單字誤判成加密資產
- 市場描述中的 oracle / provider 文字不得優先於題目主體做資產識別，避免把 `Bitcoin / Ethereum / Solana Up or Down` 題目誤判成 `LINK`
- `up/down` 題型若以顯式時間區間呈現（如 `11:30AM-11:35AM ET`），需能映射回 `5m / 15m / 30m / 1h / 4h`
- 典型誤判案例：
  - `Netherlands` 不得識別為 `ETH`
  - 一般 `up / down` 問句若缺少加密資產語義，不得進入加密交易候選
- 若 CLI 明確指定 `--styles above_below`，固定 strike 市場可不帶 `timeframe`

### Order Book 抓取約束

- 研究層不得再依賴舊的 `GET /book/{token_id}` 路徑
- 即時深度主來源需優先改為官方 market WebSocket，而不是把 SDK `get_order_book()` 當成唯一即時資料
- 官方 market WebSocket 契約：
  - 端點：`wss://ws-subscriptions-clob.polymarket.com/ws/market`
  - 初始訂閱：
    - `{"assets_ids": [...], "type": "market", "custom_feature_enabled": true}`
  - 動態追加訂閱：
    - `{"operation": "subscribe", "assets_ids": [...], "custom_feature_enabled": true}`
  - heartbeat：
    - client 每 `10` 秒送 `PING`
    - server 回 `PONG`
- market WebSocket 需至少處理兩種訊息：
  - `book`：全量 order book 快照
  - `price_change`：價位增量更新，內含 `asset_id`、`price`、`size`、`side`、`best_bid`、`best_ask`
- 本地需新增 `RealtimeOrderBookCache` 類型，對外提供：
  - `ensure_assets(asset_ids)`：確保 token 已訂閱
  - `get_cached_orderbook(token_id, max_age_seconds)`：同步讀取新鮮快取
  - `get_orderbook(token_id, rest_fallback, max_wait_seconds)`：優先等候 WebSocket 快取，必要時 fallback REST
  - `close()`：關閉 WebSocket、reader task、heartbeat task
- order book 標準化輸出需維持既有 dict 契約：
  - `bids: [{"price": "...", "size": "..."}]`
  - `asks: [{"price": "...", "size": "..."}]`
  - `_fetched_at: "<iso timestamp>"`
- 需改用 `py-clob-client` 的 `get_order_book(token_id)` 公開方法
- scanner / research / 執行層需對齊同一份 CLOB 深度語義：`bids` / `asks` / `price` / `size`
- scanner / research / live 的 order book 讀取順序必須一致：
  - 先讀 `RealtimeOrderBookCache`
  - 只有在快取缺失或過期時才 fallback `py-clob-client.get_order_book(token_id)`
- scanner 的 tradability 驗證必須同時驗證 `yes_token` 與 `no_token`：
  - `has_token_ids=true` 的前提是 YES / NO 兩邊 token 都存在
  - `book_available=true` 的前提是 YES / NO 兩邊 order book 都成功取得
  - 任一邊缺失時不得先計入 `clob_eligible` 或 `pricing_verified`
- `ResearchPipeline.run()` 需改為兩段式：
  - 第一段：`check_tradability(..., verify_depth=False)`，只做 status、雙邊 token 與輕量 quote 檢查
  - 第二段：在排序與 `filter_live_markets_for_analysis()` 後，才對剩餘市場補做雙邊 order book 驗證
- `tradable_markets` 在第一段只代表「值得進一步檢查」；只有第二段 `tradability.book_available=true` 的市場才可進入 `_analyze_market()`
- scanner 若已取得第二段雙邊 order book，需把標準化後的 `yes_orderbook` / `no_orderbook` 置於 `MarketTradability` 內供 research 重用；research 僅在快照缺失時才允許 fallback 重抓
- `MarketTradability` 內的 `yes_orderbook` / `no_orderbook` 若已來自 WebSocket 新鮮快取，research 不得再無條件強制 REST 重抓
- `UP_DOWN` 研究層需改用單邊有效成交成本模型：
  - 先根據選定方向的 ask 檔位估算固定 notional 的加權平均成交價
  - 再以該有效成交價相對最佳 ask 的偏離，作為 `spread_pct` / friction 門檻
  - 不再用 YES / NO 雙邊最大相對 spread 直接淘汰整個市場
- `UP_DOWN` tail 候選的正式 `selected_edge` 需改為 maker 視角：
  - `gross_edge_up_maker = p_up - yes_bid`
  - `gross_edge_down_maker = p_down - no_bid`
  - `selected_side` 與候選排序預設以 maker 淨 edge 為準
- `UP_DOWN` 若需保留 taker 診斷值，需另外輸出 taker 視角欄位，不得再把 taker edge 直接覆寫成候選主排序值
- `UP_DOWN` 研究層的拒絕順序需固定：
  - `timeframe_missing`
  - `missing_token_ids`
  - `window_not_open`
  - `volume_too_low`
  - `orderbook_unavailable`
  - `ask_quote_missing`
  - `spot_price_unavailable`
  - `anchor_unavailable`
  - `spread_too_wide`
  - `lead_z_too_low` / `edge_too_low`
- `observe` 市場不需完成研究層定價與打分；它只需在 `_analyze_up_down_market()` 內統一產出 `window_not_open`，且在使用者文案上明確顯示為「已開盤未進尾盤」
- `_analyze_up_down_market()` 在算出 `window_state` 後，若市場仍為 `observe`，需立即以 `window_not_open` 拒絕並早退，不得繼續進行 order book、spot、anchor、volatility 抓取
- `observe` 市場的拒絕原因不得被後續深度或 edge 拒絕覆蓋；只要 `window_state=observe`，就必須固定回傳 `window_not_open`
- `window_not_open` reject detail 至少需帶出：
  - `window_state`
  - `window_label`
  - `tau_seconds`
  - `seconds_to_armed`
  - `seconds_to_attack`
- `UP_DOWN` fee 模型需對齊官方：
  - maker fee = `0`
  - taker fee 使用 `fee = C * feeRate * p * (1-p)`
  - 研究層若以相對名義金額表示 taker fee cost，可換算為 `fee_rate * (1 - execution_price)`
  - 對 crypto 市場，`feesEnabled=true` 時的預設 `feeRate` 為 `0.072`
- `ResearchOpportunity` / `TailStrategyEstimate` 至少需補出：
  - `selected_execution_mode`
  - `maker_net_edge_up`
  - `maker_net_edge_down`
  - `taker_net_edge_up`
  - `taker_net_edge_down`
- 執行層 `_select_tail_order_price()` 不得再單靠 timeframe 推導 maker / taker；應優先尊重 research 產出的 `selected_execution_mode`
- 研究門檻基線：
  - `15m`：`minimum_lead_z=1.5`、`minimum_net_edge=0.04`
  - `4h`：`minimum_lead_z=1.4`、`minimum_net_edge=0.03`
  - 其他 timeframe 維持既有平衡預設
- `market_not_open_yet` reject detail 至少需帶出：
  - `window_state`
  - `market_start_timestamp`
  - `seconds_to_open`
  - `tau_seconds`
- `ask_quote_missing` 需前移為早期拒絕：
  - 若兩邊皆無法提供足量 ask 深度，直接拒絕
  - 若僅單邊缺價，保留另一邊進入定價，但若最終選定方向缺價，需以 `ask_quote_missing` 拒絕
- `/markets` 遠端查詢仍保留 `order=volume&ascending=false`，但研究層在只跑 `up_down` 或處理 `UP_DOWN` 候選集合時，需於本地重排為 `time_to_armed asc, expiry asc, volume desc`
- `time_to_armed` 定義為 `max(tau_seconds - armed_window_seconds, 0)`；已進入 `armed / attack` 的市場其 `time_to_armed` 視為 `0`
- `UP_DOWN` 在 scanner / research 交界只允許前置擋掉 `parsed.expiry <= now` 的市場；`observe` 市場需進入 `_analyze_market()`，由研究層統一回 `window_not_open`，不得以研究成功候選往後傳
- `prefiltered_rejects` 不得只進 `reject_summary`；同一批前置拒絕樣本也需依 `reject_samples` 上限寫入結果，供監控頁顯示具體樣本
- 對來自 `/events` 補充來源的 `UP_DOWN` 市場，若 `parsed.expiry <= now`，應在 research 早期直接排除，不得進入 tradability 或 live 窗口過濾
- 執行層對 `UP_DOWN` 候選正式送單前，需重新抓取選定方向對應 token 的最新 order book
- 執行層 `_refresh_tail_side_quote()` 需先讀 `RealtimeOrderBookCache.get_cached_orderbook(token_id, max_age_seconds=...)`；只有快取不存在或過期時才允許 fallback `get_order_book(token_id)`
- maker / taker 價格選擇需以最新 order book 為準，不得直接沿用 research candidate 內的歷史 `yes_bid / yes_ask / no_bid / no_ask`
- 若送單前最新 quote 缺失、價格不合法或與研究時刻相比已失去可成交性，執行層需回傳結構化拒絕，不得送出舊價格訂單

## 研究層執行期約束

- `ResearchPipeline.run()` 需重用同一個 scanner session；同一個 pipeline 實例在連續多輪 `run()` 間不得重建 `aiohttp.ClientSession`
- `ResearchPipeline.close()` 需作為公開關閉方法，供 CLI 在程序結束時釋放 scanner session
- `spot_price` 快取鍵為 `oracle_symbol`
- `volatility` 快取鍵為 `(oracle_symbol, lookback_minutes)`
- 上述快取需為短 TTL，本質僅是降低重複讀取，不得跨太長時間保留陳舊行情

## CLI 入口

### `run_research_pipeline.py`

用途：

- 執行研究主線掃描
- 匯出 edge 機會
- 將觀測資料寫入 SQLite

參數：

- `--limit-events`
  - 說明：Gamma events 掃描上限
  - 預設：`200`
  - 補充：短週期常駐若面向極短窗口市場，建議收斂到 `30`
- `--min-edge`
  - 說明：最小 edge 門檻
  - 預設：`0.03`
- `--min-confidence`
  - 說明：最小模型信心門檻
  - 預設：`0.30`
- `--max-spread`
  - 說明：最大允許單邊有效成交成本比率
  - 預設：`0.10`
- `--min-volume`
  - 說明：最小市場成交量
  - 預設：`0.0`
- `--timeframes`
  - 說明：允許週期，逗號分隔
  - 預設：`5m,15m,1h,4h,1d`
  - 補充：正式常駐預設不得納入 `1m`
- `--styles`
  - 說明：允許市場風格，逗號分隔；v1 預設只跑 `up_down`
  - 預設：`up_down`
  - 補充：live 短週期常駐配置需與主線一致，不得再覆寫為 `above_below`
- `--assets`
  - 說明：允許資產，逗號分隔，空值代表全部
  - 預設：空
- `--anchor-source`
  - 說明：開盤錨點來源策略；v1 只接受與市場結算來源一致的 `settlement_oracle`
  - 預設：`settlement_oracle`
- `--tail-mode`
  - 說明：尾盤狀態機模式；v1 採自適應窗口
  - 預設：`adaptive`
- `--top`
  - 說明：輸出前幾筆最佳機會
  - 預設：`20`
- `--db-path`
  - 說明：SQLite 輸出路徑
  - 預設：當日 `research_signals_YYYYMMDD.db`

## 本地監控 Web

### `run_monitor_web.py`

用途：

- 啟動本地只讀監控頁
- 提供目前機器人執行狀態、最近循環摘要、關鍵日誌與未買入原因

參數：

- `--host`
  - 說明：監控頁綁定主機
  - 預設：`127.0.0.1`
- `--port`
  - 說明：監控頁綁定埠號
  - 預設：`8787`
- `--db-path`
  - 說明：讀取研究資料的 SQLite 路徑
  - 預設：當日 `research_signals_YYYYMMDD.db`
- `--log-dir`
  - 說明：讀取檔案日誌的目錄
  - 預設：`logs`
- `--refresh-seconds`
  - 說明：前端自動刷新秒數
  - 預設：`5`
  - 補充：首頁需以倒數方式顯示剩餘秒數，不能只固定顯示設定值

### 只讀監控端點

- `GET /`
  - 說明：監控首頁 HTML
- `GET /api/status`
  - 說明：回傳進程狀態、最近循環摘要與主要日誌尾部
- `GET /api/reasons`
  - 說明：回傳最近一輪未買入原因彙總與近期樣本

回傳原則：

- 僅回傳只讀資訊
- 不暴露 `.env` 機密值
- 對日誌只回傳尾部片段，避免一次傳回完整檔案
- 監控首頁文案應以中文為主；生命週期日誌區需以最新紀錄在最上方顯示
- 監控首頁應以摘要卡與分區方式呈現同一批資料，不新增任何寫入型端點
- `GET /api/status` 的資料需足以支撐：
  - 本輪不買四層占比
  - 最近 30 分鐘趨勢
  - 熱門拒絕市場樣本
  - 目前 live 參數快照
  - 可疑異常提醒
- `alerts` 需依嚴重度與優先度排序輸出，優先讓使用者看到會直接阻塞研判或執行的訊號
- 每筆 `alert` 除 `severity`、`title`、`detail` 外，應可額外提供 `action`，用一句話指示優先排查方向
- `--export-json`
  - 說明：JSON 匯出路徑
  - 預設：不輸出

輸出：

- 終端摘要
- SQLite `observations` 資料
- SQLite `opening_anchors` 資料
- 可選 JSON 機會清單

### `run_auto_trading.py`

用途：

- 掃描指定 timeframe 的加密市場
- 產出排序後的可執行候選機會
- 視模式決定只研究或實際下單

參數：

- `--mode`
  - 說明：執行模式
  - 可選：`research`、`live`
  - 預設：`research`
- `--preflight-only`
  - 說明：只執行正式版啟動前檢查，不進入研究或 live loop
  - 預設：`false`
- `--limit-events`
  - 說明：Gamma events 掃描上限
  - 預設：`200`
  - 補充：短週期常駐若面向極短窗口市場，建議收斂到 `30`
- `--timeframes`
  - 說明：允許週期，逗號分隔
  - 預設：`5m,15m,1h,4h,1d`
- `--styles`
  - 說明：允許市場風格；v1 預設只跑 `up_down`
  - 預設：`up_down`
  - 補充：部署用 live 常駐命令需顯式維持 `up_down`
- `--assets`
  - 說明：允許資產，逗號分隔，空值代表全部支援資產
  - 預設：空
- `--min-edge`
  - 說明：最小 edge 門檻
  - 預設：`0.03`
- `--min-confidence`
  - 說明：最小模型信心門檻
  - 預設：`0.30`
- `--max-spread`
  - 說明：最大允許單邊有效成交成本比率
  - 預設：`0.10`
- `--min-volume`
  - 說明：最小市場成交量
  - 預設：`0.0`
- `--max-candidates`
  - 說明：每輪最多執行幾筆機會
  - 預設：平衡風控預設值
- `--tail-mode`
  - 說明：尾盤狀態機模式
  - 預設：`adaptive`
- `--allow-taker-fallback`
  - 說明：是否允許 `1h / 4h / 1d` 在 maker-first 超時後改走 taker fallback
  - 預設：`false`
- `--scan-interval`
  - 說明：輪詢間隔秒數
  - 預設：`10`
  - 補充：此值需與正式短週期常駐基線一致，不得保留 `60` 秒舊預設
- `--log-dir`
  - 說明：正式版檔案日誌輸出目錄；未指定時回退環境變數
  - 預設：`POLY_LOG_DIR` 或 `logs`
- `--log-level`
  - 說明：日誌等級
  - 預設：`INFO`
- `--enable-auto-claim`
  - 說明：`live` 模式下是否啟用結算後自動領取
  - 預設：`false`
- `--claim-interval`
  - 說明：自動領取掃描間隔秒數
  - 預設：`300`
- `--claim-dry-run`
  - 說明：只掃描可領取倉位，不提交 relayer 領取交易
  - 預設：`false`
- `--continuous`
  - 說明：是否持續輪詢；未指定時只執行一輪
  - 預設：`false`
- `--db-path`
  - 說明：SQLite 輸出路徑
  - 預設：當日 `research_signals_YYYYMMDD.db`

輸出：

- 終端摘要
- preflight 摘要
- 分類檔案日誌
- SQLite `observations` 資料
- SQLite `opening_anchors` 資料
- `live` 模式下的訂單執行摘要
- 啟用自動領取時的 `settlement_claims` 資料

### `run_auto_trading.py --preflight-only`

用途：

- 正式啟動前檢查生產環境是否可用

檢查項目：

1. CLOB API 憑證
2. signer / proxy wallet 推導與 `FUNDER_ADDRESS` 一致性
3. CLOB 餘額查詢
4. Data API 可領取倉位掃描
5. claim relayer payload 組裝
6. SQLite 可寫性

結果：

- 任一檢查失敗以非零狀態結束
- 全部通過才允許正式啟動 live 模式

## 自動領取提交策略

### SAFE 路徑

適用條件：

- `claim_account` 等於 signer 對應的 safe 地址
- 或 `POLY_CLAIM_RELAYER_TYPE=safe`

流程：

1. 呼叫 `GET /deployed?address=<safe>`
2. 呼叫 `GET /nonce?address=<signer>&type=SAFE`
3. 建立 `type=SAFE` 的 `/submit` payload

### PROXY 路徑

適用條件：

- `claim_account` 等於 signer 對應的 proxy wallet
- 或 `POLY_CLAIM_RELAYER_TYPE=proxy`

流程：

1. 呼叫 `GET /relay-payload?address=<signer>&type=PROXY`
2. 以官方 `proxy((uint8,address,uint256,bytes)[])` 格式編碼交易
3. 建立 `type=PROXY` 的 `/submit` payload，帶入 `relayerFee`、`gasLimit`、`relayHub`、`relay`

安全約束：

- 若 `claim_account` 與 signer 推導出的 wallet 類型不一致，必須拒絕提交
- `dry run` 仍只掃描，不做任何 relayer 檢查或提交
- 若 gas estimate 失敗改用預設 `gasLimit`，必須寫入 claim 類日誌

## 生產運行內部狀態

正式版需額外持久化以下運行狀態：

- pending orders
- 同資產同方向暴露鍵
- claim pending 狀態與 relayer 回補結果

設計要求：

- 重啟後必須先恢復持久化狀態，再開始新一輪候選執行
- 同一市場 / 同一資產方向不得因程序重啟而重複下單

## 內部資料模型

### `ResearchOpportunity`

欄位：

- `market_id`
- `slug`
- `asset`
- `timeframe`
- `question`
- `market_style`
- `selected_side`
- `selected_edge`
- `fair_yes`
- `fair_no`
- `anchor_price`
- `anchor_timestamp`
- `yes_bid`
- `yes_ask`
- `no_bid`
- `no_ask`
- `spot_price`
- `strike_price`
- `tau_seconds`
- `sigma_tail`
- `lead_z`
- `window_state`
- `time_to_expiry_sec`
- `confidence_score`
- `spread_pct`
- `volume`
- `yes_token_id`
- `no_token_id`
- `observation_id`

### `TradingCandidate`

欄位：

- `opportunity`
- `market_definition`
- `reference_price`
- `fair_probability`
- `observation`
- `raw_market`
- `parsed_market`
- `tradability`

衍生欄位：

- `tick_size`
- `neg_risk`
- `selected_net_edge`
- `selected_window_state`

### `OpeningAnchorRecord`

欄位：

- `market_id`
- `asset`
- `timeframe`
- `anchor_timestamp`
- `anchor_price`
- `source`
- `source_trade_id`
- `quality_score`
- `captured_at`

### `MarketRuntimeSnapshot`

欄位：

- `market_id`
- `asset`
- `timeframe`
- `anchor_price`
- `spot_price`
- `tau_seconds`
- `sigma_tail`
- `yes_bid`
- `yes_ask`
- `no_bid`
- `no_ask`
- `best_depth`
- `fees_enabled`
- `window_state`

### `TailStrategyEstimate`

欄位：

- `p_up`
- `p_down`
- `lead_z`
- `gross_edge_up`
- `gross_edge_down`
- `fee_cost`
- `slippage_cost`
- `fill_penalty`
- `net_edge_up`
- `net_edge_down`
- `selected_side`
- `selected_net_edge`
- `window_state`

### `ResearchScanResult`

欄位：

- `scanned_event_count`
- `discovered_market_count`
- `parsed_market_count`
- `pricing_verified_count`
- `analyzed_market_count`
- `opportunity_count`
- `opportunities`
- `candidates`

### `AutoTradingCycleResult`

欄位：

- `started_at`
- `finished_at`
- `mode`
- `scanned_event_count`
- `candidate_count`
- `selected_count`
- `executed_count`
- `rejected_count`
- `failed_count`
- `claim_submitted_count`
- `claim_failed_count`
- `claim_dry_run_count`
- `scan_result`
- `selected_candidates`
- `executions`
- `claim_results`

### `RedeemablePosition`

欄位：

- `condition_id`
- `market_id`
- `question`
- `token_id`
- `proxy_wallet`
- `size`
- `redeemable`
- `raw_payload`

### `SettlementClaimResult`

欄位：

- `claim_id`
- `condition_id`
- `market_id`
- `claim_account`
- `question`
- `proxy_wallet`
- `status`
- `submitted_at`
- `completed_at`
- `transaction_id`
- `transaction_hash`
- `safe_nonce`
- `error_message`
- `raw_response`

狀態補充：

- `submitted`
- `mined`
- `confirmed`
- `failed`
- `skipped`
- `dry_run`

### `LiveExecutionResult`

欄位：

- `order_id`
- `market_id`
- `observation_id`
- `side`
- `size`
- `price`
- `filled_size`
- `avg_fill_price`
- `fee_paid`
- `status`
- `created_at`
- `filled_at`
- `error_message`
- `raw_response`

## SQLite 資料契約

### `opening_anchors`

欄位：

- `market_id`
- `asset`
- `timeframe`
- `anchor_timestamp`
- `anchor_price`
- `source`
- `source_trade_id`
- `quality_score`
- `captured_at`

### `observations`

新增欄位：

- `market_style`
- `anchor_price`
- `anchor_timestamp`
- `lead_z`
- `sigma_tail`
- `window_state`
- `net_edge_selected`

### `settlement_claims`

欄位：

- `claim_id`
- `condition_id`
- `market_id`
- `claim_account`
- `question`
- `proxy_wallet`
- `status`
- `submitted_at`
- `completed_at`
- `transaction_id`
- `transaction_hash`
- `safe_nonce`
- `error_message`
- `raw_response_json`

## 不再保留的舊介面

以下舊介面已不屬於正式版本：

- 舊版單體入口
- 舊 dashboard 啟動與讀庫介面
- 舊 dashboard 專用資料表與舊獨立交易資料庫
- 舊版 `sdk_*` 腳本輸出格式

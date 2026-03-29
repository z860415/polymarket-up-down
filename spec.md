# Polymarket 尾盤主線規格

## 目標

將專案收斂為單一主線，v1 唯一正式策略改為：

1. `run_research_pipeline.py` 研究掃描入口
2. `run_auto_trading.py` 自動交易入口
3. `discover -> score -> risk -> execute` 單一路徑
4. 研究與下單共用同一份候選機會資料模型
5. `UP / DOWN` 尾盤錯價作為本輪唯一主線
6. 結算後可追蹤 redeemable 倉位並自動送出領取交易
7. 正式上線版只支援目前這組 `Proxy/Funder` 帳戶模型，不做 `EOA` / `Safe` 多帳戶泛化
8. 正式上線前必須先通過單一 preflight 與最小 smoke gate，否則不得進入 live loop

## 官方約束

本輪主線需遵循 Polymarket 官方文檔的公開與交易模型：

1. 市場發現使用 Gamma API `events` / `markets`
2. 公開價格與深度使用 CLOB 公開端點或官方 SDK 公開方法
3. 實盤下單使用官方 CLOB SDK 與 L2 認證流程
4. 代理錢包帳戶需正確配置 `signatureType` 與 `funder`
5. 結算後領取需遵循官方 Data API `positions?redeemable=true` 與官方 relayer / CTF `redeemPositions` 路徑
6. 結算後領取需同時相容官方 `SAFE` 與 `POLY_PROXY` 兩種 relayer 提交模型

## 生產版範圍

本輪正式上線版鎖定以下前提：

1. 只支援單機 VPS 常駐部署
2. 只支援目前的 `Proxy/Funder` 帳戶模型
3. 只保留檔案日誌，不接外部告警系統
4. 風控預設採平衡，不以激進擴倉為目標
5. 目標是穩定執行、可恢復、可觀測，不承諾保證盈利
6. 可額外提供本地只讀監控 Web，用於人工查看當前循環狀態與最近未成交原因，但不得反向控制交易主流程

## 保留範圍

### CLI 入口

- `run_research_pipeline.py`
- `run_auto_trading.py`

### 核心模組

- `polymarket_arbitrage/market_definition.py`
- `polymarket_arbitrage/reference_builder.py`
- `polymarket_arbitrage/fair_prob_model.py`
- `polymarket_arbitrage/signal_logger.py`
- `polymarket_arbitrage/integrated_scanner_v2.py`
- `polymarket_arbitrage/research_pipeline.py`
- `polymarket_arbitrage/live_executor.py`
- `polymarket_arbitrage/auto_trading.py`
- `binance_client.py`

### 保留測試

- `tests/test_market_definition.py`
- `tests/test_reference_builder.py`
- `tests/test_fair_prob_model.py`
- `tests/test_signal_logger.py`
- `tests/test_live_executor.py`

## 主架構

### 1. 市場發現層

輸入：

- Gamma `events`
- Gamma `markets`

職責：

- 只保留加密市場
- discovery 主來源需優先改用官方 Gamma `GET /markets`，直接抓可交易 market，而不是先抓大量 event 再在 tradability 層淘汰
- `GET /markets` discovery 至少需帶入 `active=true`、`closed=false`、`archived=false`、`enableOrderBook=true`，並以 `liquidity_num_min>0` 或等價條件排除無 order book 模板市場
- `GET /markets` discovery 需優先按成交活躍度排序，例如 `order=volume` 且 `ascending=false`，避免短週期可交易市場被低活躍模板市場擠掉
- 若 CLI 的 `limit-events` 偏小，scanner 可在內部對 `/markets` 查詢做放大量抓取，再以本地 parse / style / asset 規則收斂，避免前 30 筆全站市場剛好沒有 `BTC / ETH / SOL up_down`
- 官方 Gamma `GET /events` 查詢保留為 fallback；僅在 `/markets` discovery 不可用或回傳空集合時，才退回 `tag_slug=crypto` 與 `related_tags=true`
- 若運行參數只允許 `up_down`，discovery 階段需先以問題語意做前置 style 過濾，只展開 `UP / DOWN` 市場，避免 `ABOVE / BELOW` 與未知型別進入 parse
- 若運行參數只允許 `up_down`，discovery 不得只依賴 `/markets order=volume desc` 單一視窗；需額外合併 `tag_slug=crypto`、`order=endDate`、`ascending=true` 的近端 `events` 視窗，避免低成交但即將進入 live 窗口的市場被熱門排序完全漏掉
- 合併後的 `UP_DOWN` 候選需以 `market_id` 去重；同一市場若同時來自 `/markets` 與 `/events`，應優先保留 `/markets` 版本
- 已過期 `UP_DOWN` 市場不得因 `events` 回傳殘留資料再度進入研究主線
- 若運行參數只允許 `up_down`，research 主線在 scanner / research 交界只允許擋掉「已過期 / 已不可推進」的市場；只要市場仍開盤且未過期，即使 `window_state=observe` 也必須進入 `_analyze_market()`
- crypto `up_down` discovery 需優先按 `endDate` 近端排序，確保短週期市場不被長週期事件淹沒
- 資產識別不得使用無邊界的子字串命中；`eth`、`sol`、`dot` 等縮寫必須以單詞邊界或明確價格語義匹配，避免 `Netherlands`、`soldier` 之類誤判
- `up/down` 題型的資產識別不得被市場描述中的 oracle / provider 文案干擾；像 `Chainlink` 來源描述不得覆蓋 `Bitcoin / Ethereum / Solana` 題目主體
- scanner 保留 `ABOVE / BELOW` 與 `UP / DOWN` 解析能力
- 研究與交易 v1 只納入 `UP / DOWN`
- live 短週期常駐配置不得再覆寫為 `above_below`，需與 v1 主線一致只跑 `up_down`
- `up/down` 題目若使用顯式時間窗字串（如 `11:30AM-11:35AM ET`），需能推導為對應的短週期 timeframe
- 若運行參數顯式允許 `above_below`，固定 strike 市場不得因 `timeframe=None` 在入口過濾層被提前丟棄
- 只保留 `5m / 15m / 1h / 4h / 1d`

### 2. 價格與深度層

輸入：

- YES / NO token id
- 市場結算來源對應之開盤錨點
- 即時現貨價格

職責：

- 從 CLOB 抓取 best bid / ask / spread / midpoint / tick size / min order size
- 研究層抓取 order book 時不得依賴已失效的 `/book/{token}` 舊路徑，需改用官方 SDK 公開方法或當前有效公開端點
- 嚴格依市場結算來源抓取開盤錨點，不允許自定義替代值
- 依 timeframe 估算尾盤剩餘波動率
- 建立研究與執行共用的市場狀態快照
- `UP / DOWN` 市場不得再以 YES / NO 雙邊對稱 spread 當唯一流動性門檻；需改為以單邊有效成交成本作為研究層主要 friction 指標
- 單邊有效成交成本需基於選定方向的 ask 深度估算，例如以固定 notional 小單向 order book 逐檔吃單後的加權平均成交價，衡量實際可成交性
- 若單一方向缺少足夠 ask 深度，應視為該方向 quote 不可用；若兩邊皆不可用，需在研究早期直接拒絕，避免進入後續定價與 anchor 抓取
- `UP / DOWN` 研究主線需允許開盤中的 `observe` 市場先進入 `_analyze_market()`，但只用於統一產生 `window_not_open` 拒絕，不得在 `observe` 狀態繼續做重成本定價流程
- `window_not_open` 在 `window_state=observe` 時，對使用者文案需明確表達為「已開盤未進尾盤」，不得再只顯示模糊的「未進入窗口」
- `UP / DOWN` discovery 在保留官方 `/markets` 熱門排序的前提下，研究入口需新增本地窗口距離重排：同題型候選先按「距離 `armed` 窗口還差多久」排序，再按 `expiry` 近端優先，最後以 `volume` 高者優先，避免不同 timeframe 下僅靠到期時間排序而錯過更快進窗的市場
- `UP / DOWN` 主研究線需區分「市場是否開盤」與「策略窗口是否開啟」：開盤且未過期的市場必須進入 `_analyze_market()`；其中 `observe` 僅用於統一回傳 `window_not_open`，`armed / attack` 才進入完整研究打分
- `UP / DOWN` 在 research 前置過濾需額外區分「尚未開盤」市場；若 `market_start_timestamp > now`，應以前置拒絕 `market_not_open_yet` 記錄，避免把尚未生成開盤 K 線的市場混入 `anchor_unavailable`
- `UP / DOWN` 市場雖可在 scanner / research 交界保留 `observe` 狀態進入 `_analyze_market()`，但 `_analyze_up_down_market()` 在算出 `window_state` 後必須立即以前置拒絕 `window_not_open` 早退，不得對 `observe` 市場繼續做 order book、spot、anchor、波動率等重成本分析
- `window_not_open` reject detail 至少需帶出 `window_state`、`window_label`、`tau_seconds`、`seconds_to_armed`、`seconds_to_attack`，讓監控頁能正確顯示「已開盤未進尾盤」

### 3. 定價與打分層

輸出欄位至少包含：

- `asset`
- `timeframe`
- `market_id`
- `question`
- `market_style`
- `anchor_price`
- `anchor_timestamp`
- `spot_price`
- `tau_seconds`
- `sigma_tail`
- `lead_z`
- `p_up`
- `p_down`
- `yes_bid`
- `yes_ask`
- `no_bid`
- `no_ask`
- `spread_pct`
- `gross_edge_up`
- `gross_edge_down`
- `fee_cost`
- `slippage_cost`
- `fill_penalty`
- `net_edge_up`
- `net_edge_down`
- `selected_side`
- `selected_edge`
- `window_state`
- `confidence_score`

補充：

- `UP / DOWN` 的 `spread_pct` 在 v1 主線中應表示「選定方向的有效成交成本比率」，不再表示 YES / NO 雙邊最大對稱 spread
- `UP / DOWN` 的研究拒絕語義需固定為：不得在 scanner / research 交界提前吞掉開盤中的 `observe` 市場；但 `_analyze_up_down_market()` 在確認 `window_state=observe` 後，需立即以 `window_not_open` 早退，只有 `armed / attack` 市場才進入深度、anchor、spread、edge、confidence 等完整研究判斷
- `window_state` 應保留在 observation / candidate metadata 中；監控頁需能區分 `observe / armed / attack`，其中 `observe` 對使用者文案顯示為「已開盤未進尾盤」
- 生產版平衡預設下，研究門檻改為：
  - `15m`: `minimum_lead_z=1.5`、`minimum_net_edge=0.04`
  - `4h`: `minimum_lead_z=1.4`、`minimum_net_edge=0.03`
- 其餘 timeframe 維持原門檻不變，避免一次性擴張過多短週期噪音與低品質候選

定價核心：

- `tau_years = tau_seconds / 31536000`
- `lead_z = ln(spot_price / anchor_price) / (sigma_tail * sqrt(tau_years))`
- `p_up = N(lead_z)`
- `p_down = 1 - p_up`

尾盤狀態機：

- `observe`
- `armed`
- `attack`
- `expired`

預設動態窗口：

- `5m`: `armed<=90s`, `attack<=35s`
- `15m`: `armed<=180s`, `attack<=75s`
- `1h`: `armed<=720s`, `attack<=240s`
- `4h`: `armed<=2400s`, `attack<=900s`
- `1d`: `armed<=5400s`, `attack<=1800s`

### 4. 風控與執行層

職責：

- 過濾低信心、低流動性、過寬 spread、未進尾盤窗口之機會
- 只以 `net_edge` 作為排序與進場核心依據
- 檢查帳戶可用餘額與持倉上限
- 依 timeframe 採取 maker-first 或 aggressive taker 策略
- 同一資產同方向只允許一個尾盤攻擊倉位
- 保存訂單與觀測關聯
- 啟動前先完成 proxy-only preflight，失敗即拒絕 live 啟動
- 將 pending orders / directional exposure 持久化，確保重啟後可恢復
- 對市場掃描、下單、訂單輪詢、claim 分類記錄錯誤與退避
- `UP / DOWN` 候選在正式送單前必須重抓一次最新 order book，重新解析選定方向的 maker / taker 價格；若最新 best ask / bid 缺失，需在執行層本地拒絕，不得沿用研究時刻的舊 quote 直接送單
- 送單層需保留最小滑價保護基線：若研究時刻 quote 與送單前最新 quote 偏離超過允許範圍，應以最新 quote 重算下單價格；若最新 quote 已不可成交，需直接拒絕
- pending order 輪詢需支援超時治理：若訂單在 `order_timeout_seconds` 內仍未進入 `filled / cancelled / expired` 終態，執行層需主動取消，並釋放對應的 directional exposure，避免後續同資產同方向機會被永遠阻塞
- live 主迴圈送出新單後，需在短延遲（約 `2` 秒）後立即做至少一次 `poll_order_status()`，避免新送出的 maker / taker 單一定要等到下一輪掃描才更新成交或取消狀態

預設執行規則：

- `5m / 15m`：以 taker 為主，但必須在 `attack` 狀態且 `net_edge` 高於基準門檻兩倍
- `1h / 4h / 1d`：預設 maker-first，超過 `attack_window` 三分之一仍未成交才允許 taker fallback

預設倉位 bucket：

- `5m`: `1.0%`
- `15m`: `1.25%`
- `1h`: `1.5%`
- `4h`: `2.0%`
- `1d`: `1.5%`

生產版平衡預設：

- `max_candidates_per_cycle` 預設收斂為較低值，避免單輪過度併發
- `retry_attempts`、`order_timeout_seconds`、`scan_interval` 以常駐穩定性優先
- 極短週期或 1 分鐘級市場不得沿用過大的事件掃描批次；常駐預設需將 `limit-events` 收斂到約 `30`，避免單輪掃描時間吃掉可交易窗口
- 極短週期或 1 分鐘級市場不得使用 `300` 秒級輪詢；常駐預設需將 `scan-interval` 收斂到約 `10` 秒，避免整輪錯過短窗口
- `run_auto_trading.py` 的 CLI 預設 `scan-interval` 需與正式常駐基線一致，預設值固定為 `10` 秒，不得保留 `60` 秒舊預設
- `1m` 市場不得列入正式常駐預設 timeframes；目前單輪 research / live 掃描成本無法穩定匹配 `armed=20s`、`attack=8s` 的窗口長度
- 單資產單方向暴露必須可跨重啟恢復，不能只靠記憶體暫存
- 對會立即成交的 `BUY` 單，執行前需先檢查最小 marketable notional；不滿足交易所限制時必須在本地直接拒絕，不得把無效請求送到 CLOB

### 4.3 研究層傳輸優化

- `ResearchPipeline` 在同一個程序生命週期內需重用 scanner 的 `aiohttp.ClientSession`，不得每次 `run()` 都重新 `__aenter__ / __aexit__`
- CLI 入口在程序結束前需顯式呼叫研究層關閉方法，確保常駐 session 被正確釋放
- 研究層對同一資產的 `spot_price` 與同一資產 / 視窗的 `volatility` 需提供短 TTL 快取，避免 `observe` 市場在同一輪或緊鄰輪次中重複發出等價 HTTP 查詢
- 上述快取必須維持最小侵入：僅作讀取優化，不得改變原有 edge / risk / execute 的決策邏輯

### 4.1 Proxy-only 帳戶約束

交易主線必須遵守：

- `FUNDER_ADDRESS` 是唯一資金歸屬地址
- `WALLET_PRIVATE_KEY` 用於推導 signer
- `WALLET_ADDRESS` 改為可選顯示值，不再是交易必填
- 啟動時必須驗證 `FUNDER_ADDRESS` 等於 signer 推導出的 proxy wallet
- CLOB 初始化、餘額查詢、持倉查詢與下單都必須統一使用同一組 proxy/funder 驗證結果

### 4.2 Preflight Gate

`run_auto_trading.py` 在正式 live 啟動前必須支援單次 preflight，至少驗證：

1. CLOB API 憑證可用
2. signer / proxy wallet 推導成功，且與 `FUNDER_ADDRESS` 一致
3. CLOB 餘額查詢可用
4. Data API redeemable positions 掃描可用
5. relayer claim 路徑可組裝有效 payload
6. SQLite 資料庫可寫入

任何一項失敗都必須阻止 live loop 啟動。

### 5. 主迴圈

提供兩種模式：

- `research`：只掃描與落庫，不下單
- `live`：掃描、風控、下單、輪詢訂單

### 5.1 本地監控 Web

用途：

- 提供本機瀏覽器可打開的只讀監控頁
- 顯示機器人目前是否在執行、最近循環摘要、關鍵日誌與未買入原因

約束：

- 只允許本機或使用者顯式指定的 host/port 啟動
- 不得提供下單、撤單、修改風控等寫操作
- 只讀既有 SQLite、檔案日誌與進程資訊，不得改寫交易資料

頁面最少需展示：

- 交易主進程是否存活
- 最近一輪 `scanned / candidates / selected / executed / failed`
- 最近候選 / 訂單 / 錯誤日誌
- 最近一輪未買入原因彙總，例如 `spread_too_wide`、`edge_too_low`、`confidence_too_low`
- 監控頁文案需以中文呈現；生命週期日誌至少需以最新紀錄在最上方顯示，方便人工追看最近異常
- 監控頁資訊層次需明確區分為摘要卡、判斷結論、拒絕原因、觀測樣本與日誌區，避免所有資訊平鋪造成人工判讀成本過高
- 監控頁若提供自動刷新秒數，需顯示即時倒數，而不是固定文案；倒數需隨前端計時更新，讓使用者可預期下一次刷新時間
- 需提供「本輪不買結論卡」，將本輪阻塞路徑拆成市場過濾、研究拒絕、風控拒絕、執行拒絕四層，並展示占比
- 需提供最近 30 分鐘短趨勢，至少覆蓋 `scanned / candidates / executed / failed`
- 需提供熱門拒絕市場樣本，聚合最近常見題目與拒絕原因
- 需提供目前 live 進程的關鍵參數快照，避免人工誤看舊進程或錯誤配置
- 需提供可疑異常提醒，例如連續零候選、連續錯誤與觀測長時間停滯
- 可疑異常提醒需主動挑出「優先處理」訊號，而非僅平鋪顯示；至少需依嚴重度排序，讓 `連續零候選`、`缺少觀測資料` 這類會直接阻塞研判的問題排在前面
- 每條異常提醒除標題與說明外，還需附上簡短建議動作，避免使用者仍需自行翻日誌判斷下一步

### 6. 結算領取層

輸入：

- 使用者 funder / wallet 地址
- Data API redeemable positions
- 官方 relayer 認證資料

職責：

- 週期性掃描已結算且可領取的持倉
- 將可領取倉位轉為 CTF `redeemPositions` 交易
- 透過官方 relayer 送出 gasless 領取
- 記錄掃描結果、送單狀態與鏈上交易摘要
- 避免同一個 condition 重複提交領取
- 可回補 pending claim，避免重啟後遺失狀態
- 將 nonce、submission type、transaction id、最後狀態完整落庫
- 當 proxy gas estimate 失敗退回預設值時，必須有可搜尋日誌

設計約束：

- 自動領取必須是獨立模組，不能侵入既有下單決策主線
- 領取地址優先使用 `FUNDER_ADDRESS`，缺省時退回 `WALLET_ADDRESS`
- v1 只支援 Polygon 主網與官方 relayer
- v1 只支援 binary market 的 `indexSets=[1,2]` 領取策略
- 必須提供 `claim dry run` 模式，只掃描可領取倉位、不提交 relayer 交易
- 當 `.env` 同時存在 `RELAYER_API_KEY / RELAYER_API_KEY_ADDRESS` 與 Builder 憑證時，claim 提交必須優先使用 relayer API key header；只有 relayer API key 缺失時才允許退回 Builder 簽名標頭
- 當領取地址等於 signer 推導出的 proxy wallet 時，必須改走官方 `PROXY` relayer 路徑，不得誤用 `SAFE`
- 當領取地址等於 signer 推導出的 safe 或明確指定 `safe` 模式時，沿用既有 `SAFE` 路徑
- 提交前必須驗證 `claim_account` 與 signer 推導出的 proxy / safe 地址是否一致，避免替錯帳戶領取
- `POLY_PROXY` 路徑需使用官方 `relay-payload`、`proxy((uint8,address,uint256,bytes)[])` 編碼與對應簽名欄位
- 正式上線版雖保留 `SAFE/PROXY` 相容實作，但 live 自動交易入口只允許 `Proxy/Funder` 帳戶進入

## 可觀測性與部署

正式版至少需要以下可搜尋日誌分類：

1. 啟動 / 關閉
2. preflight
3. 候選機會
4. 下單
5. 成交 / 輪詢
6. claim
7. 錯誤

成交輪詢補充規則：

- 正式版需兼容 CLOB 訂單終態別名；`MATCHED` 必須視為成交終態，`CANCELED` / `CANCELLED` 必須視為同一取消終態
- 成交欄位解析不得只依賴 `takerAmount`；若訂單查詢回傳 `size_matched`，需優先或等價納入 filled size 判定
- market order / FOK smoke 驗證必須以交易所終態與成交欄位為準，不能只看本地 `submitted`
- 正式 smoke 若採可立即成交的 `BUY` 單，需考慮交易所最小 notional 約束；目前實測 `$1` 以下的 marketable `BUY` 會被 CLOB 拒絕
- 因此若驗證目標要求 `<=0.5 USDC`，不得假設一定能完成 taker/FOK 成交，需先區分為交易所最小額限制阻塞或改採 maker 驗證路徑

部署基線：

- 以 `systemd` 作為唯一正式常駐方式
- 提供 `.env` 範本與服務檔範本
- 需支持異常退出後自動重啟

## 驗收 Gate

功能全通的工程驗收門檻至少包含：

1. `pytest -q` 全綠
2. `run_auto_trading.py --preflight-only` 通過
3. `run_research_pipeline.py` 可正常掃描並寫庫
4. `run_auto_trading.py --mode live` 可完成最小 micro order smoke
5. 訂單輪詢可拿到終態
6. `--claim-dry-run` 成功
7. 程式重啟後不重複下同資產同方向單

## 移除範圍

以下內容不再屬於正式版本：

- 舊版單體入口與平行掃描主線
- 舊版 SDK 自動交易腳本與 shell 啟動腳本
- 舊 dashboard 與其資料庫契約
- 舊研究分析 / 校準 / 模擬報告模組
- 根目錄一次性診斷、測試、監控與手動交易腳本
- 舊版 `ArbitrageDetector` 與平行交易主線

## 資料契約

### 對外正式介面

- `python3 run_research_pipeline.py ...`
- `python3 run_auto_trading.py --mode research|live ...`

### 核心資料模型

- `ParsedMarket`
- `MarketTradability`
- `OpeningAnchorRecord`
- `SettlementSourceDescriptor`
- `MarketRuntimeSnapshot`
- `TailStrategyEstimate`
- `ResearchOpportunity`
- `TradingCandidate`
- `ResearchScanResult`
- `LiveExecutionResult`
- `RedeemablePosition`
- `SettlementClaimResult`
- SQLite `observations` 表
- SQLite `opening_anchors` 表
- SQLite `settlement_claims` 表

### 明確不保留的舊契約

- 舊 dashboard 專用資料表
- 舊獨立交易資料庫
- 舊版 `ArbitrageDetector` 機會格式
- 舊版 `sdk_*` 腳本自定義 JSONL / log 契約

## 非目標

本輪不處理：

- `ABOVE / BELOW` 新定價器重構
- 盤中主動平倉策略
- 舊版主線相容層與資料遷移
- 新的 HTTP / dashboard 服務

## 驗收條件

1. `run_research_pipeline.py` 可輸出 `UP / DOWN` 尾盤候選機會，且結果包含 `anchor_price`、`tau_seconds`、`sigma_tail`、`lead_z`、`window_state`。
2. `run_auto_trading.py` 可依 timeframe 套用不同執行策略與風控 bucket。
3. `observations` 與 `opening_anchors` 可支撐後續 shadow-live 驗證與 settlement 回填。
4. `ABOVE / BELOW` 既有能力仍保留，但不參與 v1 新定價邏輯。
5. `python3 -m compileall polymarket_arbitrage run_research_pipeline.py run_auto_trading.py` 通過。
6. `python3 -m pytest tests/test_market_definition.py tests/test_signal_logger.py tests/test_live_executor.py` 在已安裝依賴環境下可執行。
7. `run_auto_trading.py` 可選擇性啟用自動領取，並在持續輪詢模式下週期性掃描 redeemable positions。
8. `settlement_claims` 可追蹤 condition、提交時間、交易狀態與錯誤，避免重複領取。
9. `--claim-dry-run` 可在不提交交易的前提下列出本輪會被抓到的 redeemable positions。
10. README、API 文件與待辦列表皆反映 `UP / DOWN` 尾盤主線版本與自動領取能力。

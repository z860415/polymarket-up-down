#!/usr/bin/env python3
"""
使用系統的 IntegratedScannerV2 來診斷市場發現
"""
import asyncio
import sys
sys.path.insert(0, '/root/.openclaw/workspace/polymarket-arbitrage')

from polymarket_arbitrage.integrated_scanner_v2 import IntegratedScannerV2

async def main():
    print("=== 使用 IntegratedScannerV2 診斷 ===\n")
    
    # 創建 mock 對象
    class MockSignalLogger:
        pass
    
    class MockLiveExecutor:
        pass
    
    scanner = IntegratedScannerV2(signal_logger=MockSignalLogger(), live_executor=MockLiveExecutor())
    
    # 手動初始化 session
    import aiohttp
    scanner.session = aiohttp.ClientSession()
    
    try:
        # 獲取所有 events
        print("1. 獲取所有 events (limit=200)...")
        events = await scanner.get_all_events(limit=200, allowed_styles={"up_down"})
        print(f"   找到 {len(events)} 個 events")
        
        # 展開 markets
        print("\n2. 展開 markets...")
        found_markets = scanner.expand_markets(events, allowed_styles={"up_down"})
        print(f"   找到 {len(found_markets)} 個 markets")
        
        # 顯示前10個 markets
        print("\n3. 前10個 markets:")
        for i, (event, market) in enumerate(found_markets[:10]):
            print(f"\n   [{i}] {market.get('question', 'N/A')}")
            print(f"       Slug: {market.get('slug', 'N/A')}")
            print(f"       Active: {market.get('active')}, Closed: {market.get('closed')}")
            
            # 檢查 token IDs
            clob_token_ids = market.get("clobTokenIds")
            if clob_token_ids:
                import json
                try:
                    token_ids = json.loads(clob_token_ids)
                    print(f"       Token IDs: {len(token_ids)} 個")
                    if len(token_ids) >= 2:
                        print(f"         YES: {token_ids[0][:30]}...")
                        print(f"         NO:  {token_ids[1][:30]}...")
                except:
                    print(f"       Token IDs: 解析失敗")
            else:
                print(f"       Token IDs: 無")
    
    finally:
        if scanner.session:
            await scanner.session.close()

if __name__ == "__main__":
    asyncio.run(main())

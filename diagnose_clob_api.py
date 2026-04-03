#!/usr/bin/env python3
"""
診斷 CLOB REST API 返回的原始數據
與系統使用相同的方式調用 API
"""
import asyncio
import os
import sys

# 添加項目路徑
sys.path.insert(0, '/root/.openclaw/workspace/polymarket-arbitrage')

from py_clob_client.client import ClobClient

CLOB_API = "https://clob.polymarket.com"

# 從日誌中獲取的 token IDs（Bitcoin Up or Down - April 3, 5:45AM-5:50AM ET）
# 這些是 condition IDs，需要轉換為 token IDs
TEST_CONDITION_IDS = [
    "0x0e92ce8ae27c358eb7d0c4db3135e03d8d862ef790ecbc7c38f5ca4e82e2212c",
    "0xc58106c7e881e08255255d009677ab6d7c15628e0ed465372ad563402c1c6f13",
]

async def main():
    # 使用與系統相同的公開 client
    client = ClobClient(host=CLOB_API)
    
    print("=== 測試 CLOB REST API ===\n")
    
    # 嘗試直接獲取一個已知市場的 order book
    # 我們需要 token_id，而不是 condition_id
    # 讓我們先嘗試獲取一個活躍市場的數據
    
    # 使用一個已知的 token_id（從之前的 Gamma API 調試中）
    # 但由於 Gamma API 沒有返回當前市場，我們無法獲取 token_id
    
    # 替代方案：檢查系統使用的具體 token_id
    print("需要從系統日誌中獲取實際使用的 token_id")
    print("讓我們檢查錯誤日誌中的具體 API 響應...")

if __name__ == "__main__":
    # 同步運行
    import aiohttp
    
    async def test_direct_api():
        CLOB_API = "https://clob.polymarket.com"
        
        # 從系統日誌中看到的 market_id (這可能是 condition_id 或 token_id)
        # 日誌顯示: market_id=0x0e92ce8ae27c358eb7d0c4db3135e03d8d862ef790ecbc7c38f5ca4e82e2212c
        
        test_ids = [
            "0x0e92ce8ae27c358eb7d0c4db3135e03d8d862ef790ecbc7c38f5ca4e82e2212c",
            "0xc58106c7e881e08255255d009677ab6d7c15628e0ed465372ad563402c1c6f13",
        ]
        
        async with aiohttp.ClientSession() as session:
            for test_id in test_ids:
                print(f"\n=== 測試 ID: {test_id[:30]}... ===")
                
                # 1. 嘗試作為 token_id 獲取 order book
                url = f"{CLOB_API}/book"
                params = {"token_id": test_id}
                
                try:
                    async with session.get(url, params=params, timeout=10) as resp:
                        print(f"  /book API status: {resp.status}")
                        if resp.status == 200:
                            data = await resp.json()
                            bids = data.get('bids', [])
                            asks = data.get('asks', [])
                            print(f"  bids: {len(bids)} 個")
                            print(f"  asks: {len(asks)} 個")
                            if bids:
                                print(f"  best_bid: {bids[0]}")
                            if asks:
                                print(f"  best_ask: {asks[0]}")
                        else:
                            text = await resp.text()
                            print(f"  錯誤響應: {text[:200]}")
                except Exception as e:
                    print(f"  錯誤: {e}")
                
                # 2. 嘗試獲取 price
                url = f"{CLOB_API}/price"
                params = {"token_id": test_id, "side": "buy"}
                
                try:
                    async with session.get(url, params=params, timeout=10) as resp:
                        print(f"  /price API status: {resp.status}")
                        if resp.status == 200:
                            data = await resp.json()
                            print(f"  price: {data}")
                except Exception as e:
                    print(f"  錯誤: {e}")
    
    asyncio.run(test_direct_api())

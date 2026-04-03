#!/usr/bin/env python3
"""
診斷 CLOB API vs Gamma API 數據差異
"""
import asyncio
import aiohttp
import json

CLOB_API = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

# 測試市場: Bitcoin Up or Down - April 3, 5:50AM-5:55AM ET
# 從日誌中獲取的 market ID: 1827086
TEST_MARKET_ID = "1827086"

async def fetch_gamma_market(session: aiohttp.ClientSession, market_id: str):
    """從 Gamma API 獲取市場數據"""
    url = f"{GAMMA_API}/markets/{market_id}"
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status == 200:
                return await resp.json()
            return None
    except Exception as e:
        print(f"Gamma API error: {e}")
        return None

async def fetch_clob_orderbook(session: aiohttp.ClientSession, token_id: str):
    """從 CLOB API 獲取 order book"""
    url = f"{CLOB_API}/book"
    params = {"token_id": token_id}
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            if resp.status == 200:
                return await resp.json()
            return None
    except Exception as e:
        print(f"CLOB API error: {e}")
        return None

async def main():
    async with aiohttp.ClientSession() as session:
        print(f"=== 測試市場 ID: {TEST_MARKET_ID} ===\n")
        
        # 1. 獲取 Gamma 數據
        gamma_data = await fetch_gamma_market(session, TEST_MARKET_ID)
        if gamma_data:
            print("=== Gamma API 數據 ===")
            print(f"問題: {gamma_data.get('question', 'N/A')}")
            
            # 獲取 token IDs
            outcomes = gamma_data.get('outcomes', [])
            tokens = gamma_data.get('tokens', [])
            
            print(f"結果數量: {len(outcomes)}")
            print(f"Token 數量: {len(tokens)}")
            
            for i, (outcome, token) in enumerate(zip(outcomes, tokens)):
                print(f"\n  [{i}] {outcome}:")
                print(f"      token_id: {token.get('token_id', 'N/A')[:20]}...")
                print(f"      price: {token.get('price', 'N/A')}")
        else:
            print("無法獲取 Gamma 數據")
            return
        
        # 2. 獲取 CLOB order book
        print("\n=== CLOB API Order Book 數據 ===")
        tokens = gamma_data.get('tokens', [])
        
        for token in tokens:
            token_id = token.get('token_id')
            outcome = token.get('outcome')
            
            if not token_id:
                continue
                
            book = await fetch_clob_orderbook(session, token_id)
            
            print(f"\n  {outcome} (token_id: {token_id[:20]}...):")
            
            if book:
                bids = book.get('bids', [])
                asks = book.get('asks', [])
                
                best_bid = float(bids[0]['price']) if bids else None
                best_ask = float(asks[0]['price']) if asks else None
                
                print(f"    bids 數量: {len(bids)}")
                print(f"    asks 數量: {len(asks)}")
                print(f"    best_bid: {best_bid}")
                print(f"    best_ask: {best_ask}")
                
                if best_bid and best_ask:
                    spread = (best_ask - best_bid) / best_ask
                    print(f"    spread: {spread:.4f} ({spread*100:.2f}%)")
            else:
                print("    無法獲取 order book")
        
        # 3. 比較
        print("\n=== 數據比較 ===")
        if len(tokens) >= 2:
            token1_price = tokens[0].get('price')
            token2_price = tokens[1].get('price')
            
            if token1_price and token2_price:
                gamma_sum = float(token1_price) + float(token2_price)
                print(f"Gamma price sum: {gamma_sum:.4f} (應接近 1.0)")

if __name__ == "__main__":
    asyncio.run(main())

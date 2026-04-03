#!/usr/bin/env python3
"""
診斷 CLOB API vs Gamma API 數據差異
"""
import asyncio
import aiohttp
import json

CLOB_API = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

async def fetch_gamma_markets(session: aiohttp.ClientSession):
    """從 Gamma API 獲取市場列表"""
    url = f"{GAMMA_API}/markets"
    params = {"active": "true", "archived": "false", "closed": "false"}
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            if resp.status == 200:
                return await resp.json()
            print(f"Gamma API status: {resp.status}")
            return []
    except Exception as e:
        print(f"Gamma API error: {e}")
        return []

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
        print("=== 獲取活躍市場 ===\n")
        
        markets = await fetch_gamma_markets(session)
        print(f"找到 {len(markets)} 個市場")
        
        # 找到 Up/Down 類型的市場
        updown_markets = [m for m in markets if 'up-or-down' in m.get('slug', '')]
        print(f"其中 Up/Down 類型: {len(updown_markets)}")
        
        # 顯示前3個
        for market in updown_markets[:3]:
            print(f"\n--- {market.get('question', 'N/A')} ---")
            print(f"Slug: {market.get('slug', 'N/A')}")
            print(f"Active: {market.get('active')}, Closed: {market.get('closed')}")
            
            tokens = market.get('tokens', [])
            print(f"Tokens: {len(tokens)}")
            
            for token in tokens:
                print(f"  {token.get('outcome')}: price={token.get('price')}, token_id={token.get('token_id', 'N/A')[:20]}...")
            
            # 獲取 CLOB order book
            if len(tokens) >= 2:
                print("\n  CLOB Order Book:")
                for token in tokens[:2]:
                    token_id = token.get('token_id')
                    outcome = token.get('outcome')
                    
                    book = await fetch_clob_orderbook(session, token_id)
                    if book:
                        bids = book.get('bids', [])
                        asks = book.get('asks', [])
                        
                        best_bid = float(bids[0]['price']) if bids else None
                        best_ask = float(asks[0]['price']) if asks else None
                        
                        print(f"    {outcome}: bid={best_bid}, ask={best_ask}")
                        
                        if best_bid and best_ask:
                            spread = (best_ask - best_bid) / best_ask
                            print(f"      spread={spread:.4f}")
                    else:
                        print(f"    {outcome}: 無法獲取")

if __name__ == "__main__":
    asyncio.run(main())

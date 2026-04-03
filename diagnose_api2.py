#!/usr/bin/env python3
"""
診斷 CLOB API vs Gamma API 數據差異 - 使用 markets 端點搜索
"""
import asyncio
import aiohttp
import json
from urllib.parse import quote

CLOB_API = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

async def fetch_gamma_markets_by_slug(session: aiohttp.ClientSession, slug: str):
    """從 Gamma API 搜索市場"""
    url = f"{GAMMA_API}/markets"
    params = {"slug": slug}
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get('markets', [])
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
        # 搜索 Bitcoin 5m 市場
        print("=== 搜索 Bitcoin 5m 市場 ===\n")
        
        markets = await fetch_gamma_markets_by_slug(session, "bitcoin-up-or-down")
        
        # 過濾 5:50-5:55 ET 的市場
        target_markets = [m for m in markets if "5:50am-5:55am" in m.get('slug', '').lower()]
        
        if not target_markets:
            print(f"找到 {len(markets)} 個 Bitcoin 市場，但沒有匹配的 5:50-5:55")
            print("可用的 slugs:")
            for m in markets[:5]:
                print(f"  - {m.get('slug', 'N/A')}")
            return
        
        market = target_markets[0]
        print(f"找到市場: {market.get('question', 'N/A')}")
        print(f"Slug: {market.get('slug', 'N/A')}")
        print(f"Condition ID: {market.get('conditionId', 'N/A')}")
        print(f"Active: {market.get('active', 'N/A')}")
        print(f"Closed: {market.get('closed', 'N/A')}")
        
        # 獲取 outcomes 和 tokens
        outcomes = market.get('outcomes', [])
        tokens = market.get('tokens', [])
        
        print(f"\nOutcomes: {outcomes}")
        print(f"Tokens 數量: {len(tokens)}")
        
        for token in tokens:
            print(f"\n  {token.get('outcome', 'N/A')}:")
            print(f"    token_id: {token.get('token_id', 'N/A')}")
            print(f"    price: {token.get('price', 'N/A')}")
        
        # 獲取 CLOB order book
        print("\n=== CLOB API Order Book ===")
        
        for token in tokens:
            token_id = token.get('token_id')
            outcome = token.get('outcome')
            
            if not token_id:
                continue
            
            book = await fetch_clob_orderbook(session, token_id)
            
            print(f"\n  {outcome}:")
            if book:
                bids = book.get('bids', [])
                asks = book.get('asks', [])
                
                best_bid = float(bids[0]['price']) if bids else None
                best_ask = float(asks[0]['price']) if asks else None
                
                print(f"    bids: {len(bids)} 個")
                print(f"    asks: {len(asks)} 個")
                print(f"    best_bid: {best_bid}")
                print(f"    best_ask: {best_ask}")
                
                if bids:
                    print(f"    前3個 bids: {[b['price'] for b in bids[:3]]}")
                if asks:
                    print(f"    前3個 asks: {[a['price'] for a in asks[:3]]}")
                
                if best_bid and best_ask:
                    spread = (best_ask - best_bid) / best_ask
                    print(f"    spread: {spread:.4f} ({spread*100:.2f}%)")
            else:
                print("    無法獲取 order book")
        
        # 比較價格
        if len(tokens) >= 2:
            print("\n=== 價格比較 ===")
            p1 = float(tokens[0].get('price', 0) or 0)
            p2 = float(tokens[1].get('price', 0) or 0)
            print(f"Gamma prices: {p1:.4f} + {p2:.4f} = {p1+p2:.4f}")

if __name__ == "__main__":
    asyncio.run(main())

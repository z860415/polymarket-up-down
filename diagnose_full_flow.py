#!/usr/bin/env python3
"""
診斷完整流程：從 Gamma API 獲取市場 -> 提取 token IDs -> 調用 CLOB API
"""
import asyncio
import aiohttp
import json

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

async def fetch_events(session: aiohttp.ClientSession):
    """獲取 crypto events"""
    url = f"{GAMMA_API}/events"
    params = {
        "tag_slug": "crypto",
        "related_tags": "true",
        "limit": 100,
    }
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            if resp.status == 200:
                return await resp.json()
            return []
    except Exception as e:
        print(f"Events API error: {e}")
        return []

async def fetch_markets(session: aiohttp.ClientSession):
    """獲取 markets"""
    url = f"{GAMMA_API}/markets"
    params = {
        "active": "true",
        "closed": "false",
        "archived": "false",
        "limit": 100,
    }
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            if resp.status == 200:
                return await resp.json()
            return []
    except Exception as e:
        print(f"Markets API error: {e}")
        return []

async def check_clob_token(session: aiohttp.ClientSession, token_id: str):
    """檢查 token ID 是否有效"""
    url = f"{CLOB_API}/book"
    params = {"token_id": token_id}
    try:
        async with session.get(url, params=params, timeout=5) as resp:
            return resp.status == 200
    except:
        return False

async def main():
    async with aiohttp.ClientSession() as session:
        print("=== 診斷 Token ID 獲取流程 ===\n")
        
        # 1. 獲取 Events
        print("1. 獲取 Events...")
        events = await fetch_events(session)
        print(f"   找到 {len(events)} 個 events")
        
        # 2. 獲取 Markets
        print("\n2. 獲取 Markets...")
        markets = await fetch_markets(session)
        print(f"   找到 {len(markets)} 個 markets")
        
        # 3. 尋找 Up/Down markets
        print("\n3. 尋找 Up/Down markets...")
        updown_markets = []
        for market in markets:
            slug = market.get('slug', '')
            if 'up-or-down' in slug.lower():
                updown_markets.append(market)
        
        # 也檢查 events 中的 markets
        for event in events:
            for market in event.get('markets', []):
                slug = market.get('slug', '')
                if 'up-or-down' in slug.lower():
                    updown_markets.append(market)
        
        print(f"   找到 {len(updown_markets)} 個 Up/Down markets")
        
        # 4. 檢查每個 Up/Down market 的 token IDs
        print("\n4. 檢查 Token IDs...")
        for market in updown_markets[:3]:  # 只檢查前3個
            print(f"\n   Market: {market.get('question', 'N/A')}")
            print(f"   Slug: {market.get('slug', 'N/A')}")
            
            # 檢查 clobTokenIds
            clob_token_ids = market.get("clobTokenIds")
            print(f"   clobTokenIds: {clob_token_ids}")
            
            if clob_token_ids:
                try:
                    token_ids = json.loads(clob_token_ids)
                    print(f"   Parsed token_ids: {token_ids}")
                    
                    if len(token_ids) >= 2:
                        yes_token = token_ids[0]
                        no_token = token_ids[1]
                        print(f"   yes_token: {yes_token[:40]}...")
                        print(f"   no_token: {no_token[:40]}...")
                        
                        # 檢查 CLOB API
                        print("   檢查 CLOB API...")
                        yes_valid = await check_clob_token(session, yes_token)
                        no_valid = await check_clob_token(session, no_token)
                        print(f"     yes_token valid: {yes_valid}")
                        print(f"     no_token valid: {no_valid}")
                        
                        if yes_valid and no_valid:
                            # 獲取 order book
                            url = f"{CLOB_API}/book"
                            async with session.get(url, params={"token_id": yes_token}) as resp:
                                if resp.status == 200:
                                    data = await resp.json()
                                    bids = data.get('bids', [])
                                    asks = data.get('asks', [])
                                    print(f"     yes_orderbook: bids={len(bids)}, asks={len(asks)}")
                                    if asks:
                                        print(f"     yes_best_ask: {asks[0]}")
                            
                            async with session.get(url, params={"token_id": no_token}) as resp:
                                if resp.status == 200:
                                    data = await resp.json()
                                    bids = data.get('bids', [])
                                    asks = data.get('asks', [])
                                    print(f"     no_orderbook: bids={len(bids)}, asks={len(asks)}")
                                    if asks:
                                        print(f"     no_best_ask: {asks[0]}")
                except Exception as e:
                    print(f"   Error parsing token_ids: {e}")
            else:
                print("   沒有 clobTokenIds!")
            
            # 也檢查 tokens 字段
            tokens = market.get('tokens', [])
            print(f"   tokens 字段數量: {len(tokens)}")
            for token in tokens:
                print(f"     {token.get('outcome')}: token_id={token.get('token_id', 'N/A')[:40] if token.get('token_id') else 'N/A'}...")

if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
"""
診斷 CLOB API vs Gamma API - 使用系統相同參數
"""
import asyncio
import aiohttp
import json

CLOB_API = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
CRYPTO_TAG_SLUG = "crypto"

async def fetch_events(session: aiohttp.ClientSession):
    """使用系統相同參數獲取 crypto events"""
    url = f"{GAMMA_API}/events"
    params = {
        "tag_slug": CRYPTO_TAG_SLUG,
        "related_tags": "true",
        "active": "true",
        "closed": "false",
        "archived": "false",
        "order": "endDate",
        "ascending": "true",
        "limit": 100,
        "offset": 0,
    }
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            if resp.status == 200:
                return await resp.json()
            print(f"Events API status: {resp.status}")
            return []
    except Exception as e:
        print(f"Events API error: {e}")
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
        print("=== 獲取 Crypto Events ===\n")
        
        events = await fetch_events(session)
        print(f"找到 {len(events)} 個 events\n")
        
        # 找到 Up/Down 類型的 events
        updown_events = []
        for event in events:
            slug = event.get('slug', '')
            if 'up-or-down' in slug:
                updown_events.append(event)
        
        print(f"Up/Down 類型: {len(updown_events)}\n")
        
        # 顯示前3個 Up/Down events
        for event in updown_events[:3]:
            print(f"--- {event.get('title', 'N/A')} ---")
            print(f"Slug: {event.get('slug', 'N/A')}")
            print(f"End Date: {event.get('endDate', 'N/A')}")
            
            markets = event.get('markets', [])
            print(f"Markets: {len(markets)}")
            
            for market in markets:
                print(f"\n  Market: {market.get('question', 'N/A')}")
                print(f"  Active: {market.get('active')}, Closed: {market.get('closed')}")
                
                tokens = market.get('tokens', [])
                print(f"  Tokens: {len(tokens)}")
                
                for token in tokens:
                    print(f"    {token.get('outcome')}: price={token.get('price')}")
                
                # 獲取 CLOB order book
                if len(tokens) >= 2:
                    print("\n  CLOB Order Book:")
                    for token in tokens[:2]:
                        token_id = token.get('token_id')
                        outcome = token.get('outcome')
                        
                        if not token_id:
                            continue
                        
                        book = await fetch_clob_orderbook(session, token_id)
                        if book:
                            bids = book.get('bids', [])
                            asks = book.get('asks', [])
                            
                            best_bid = float(bids[0]['price']) if bids else None
                            best_ask = float(asks[0]['price']) if asks else None
                            
                            print(f"    {outcome}: bid={best_bid}, ask={best_ask}")
                            
                            if best_bid and best_ask:
                                spread = (best_ask - best_bid) / best_ask
                                print(f"      spread={spread:.4f} ({spread*100:.1f}%)")
                        else:
                            print(f"    {outcome}: 無法獲取")
                
                # 只檢查第一個市場
                break
            
            print()

if __name__ == "__main__":
    asyncio.run(main())

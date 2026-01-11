"""Polymarket REST API client for fetching market data."""

import httpx
import orjson
from datetime import datetime, timezone
from typing import Any

from .config import PolymarketConfig


class PolymarketRestClient:
    """REST API client for Polymarket Gamma and CLOB APIs."""
    
    def __init__(self, config: PolymarketConfig):
        self.config = config
        self.http_client = httpx.AsyncClient(timeout=30.0)
    
    async def close(self) -> None:
        """Close HTTP client."""
        await self.http_client.aclose()
    
    async def fetch_active_markets(self, limit: int = 100) -> list[dict]:
        """Fetch active markets from Gamma API with pagination."""
        url = f"{self.config.gamma_api_url}/markets"
        
        # Paginate through all markets
        all_markets_raw = []
        offset = 0
        page_size = min(500, limit)  # API max is 500
        
        while len(all_markets_raw) < limit:
            params = {
                "limit": page_size,
                "offset": offset,
                "active": "true",
                "closed": "false",
            }
            
            response = await self.http_client.get(url, params=params)
            response.raise_for_status()
            
            batch = orjson.loads(response.content)
            if not batch:
                break  # No more markets
            
            all_markets_raw.extend(batch)
            offset += page_size
            
            if len(batch) < page_size:
                break  # Last page
        
        markets_raw = all_markets_raw[:limit]
        print(f"   Fetched {len(markets_raw)} markets from API")
        
        # Transform to our schema
        markets = []
        for m in markets_raw:
            # Parse clobTokenIds (comes as JSON string)
            clob_token_ids = m.get('clobTokenIds', '[]')
            if isinstance(clob_token_ids, str):
                clob_token_ids = orjson.loads(clob_token_ids)
            
            # Parse outcomes
            outcomes = m.get('outcomes', '[]')
            if isinstance(outcomes, str):
                outcomes = orjson.loads(outcomes)
            
            # Parse end date
            end_date = None
            if m.get('endDate'):
                try:
                    end_date = datetime.fromisoformat(m['endDate'].replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    pass
            
            markets.append({
                'market_id': str(m['id']),
                'condition_id': m.get('conditionId', ''),
                'question': m.get('question', ''),
                'slug': m.get('slug', ''),
                'category': m.get('category', 'Unknown'),
                'outcomes': outcomes,
                'clob_token_ids': clob_token_ids,
                'end_date': end_date,
                'active': 1 if m.get('active') else 0,
                'closed': 1 if m.get('closed') else 0,
                'volume_total': float(m.get('volumeNum', 0) or 0),
                'liquidity': float(m.get('liquidityNum', 0) or 0),
                'best_bid': float(m.get('bestBid', 0) or 0),
                'best_ask': float(m.get('bestAsk', 0) or 0),
                'last_trade_price': float(m.get('lastTradePrice', 0) or 0),
            })
        
        return markets
    
    async def fetch_orderbook(self, token_id: str) -> dict | None:
        """Fetch orderbook from CLOB API."""
        url = f"{self.config.clob_api_url}/book"
        params = {"token_id": token_id}
        
        try:
            response = await self.http_client.get(url, params=params)
            response.raise_for_status()
            
            book = orjson.loads(response.content)
            
            # Extract best bid/ask
            bids = book.get('bids', [])
            asks = book.get('asks', [])
            
            # Best bid is highest price, best ask is lowest price
            # bids are typically sorted descending, asks ascending
            best_bid_px = float(bids[0]['price']) if bids else None
            best_bid_sz = float(bids[0]['size']) if bids else None
            best_ask_px = float(asks[0]['price']) if asks else None
            best_ask_sz = float(asks[0]['size']) if asks else None
            
            return {
                'ts': datetime.now(timezone.utc),
                'market_id': book.get('market', ''),
                'condition_id': book.get('market', ''),
                'token_id': token_id,
                'bid_px': best_bid_px,
                'bid_sz': best_bid_sz,
                'ask_px': best_ask_px,
                'ask_sz': best_ask_sz,
                'last_trade_price': float(book.get('last_trade_price', 0) or 0),
                'source': 'clob_rest',
            }
        except Exception as e:
            print(f"⚠️ Error fetching orderbook for {token_id[:20]}...: {e}")
            return None
    
    async def fetch_recent_trades(self, limit: int = 100) -> list[dict]:
        """Fetch recent trades from Data API."""
        url = f"{self.config.data_api_url}/trades"
        params = {"limit": limit}
        
        response = await self.http_client.get(url, params=params)
        response.raise_for_status()
        
        trades_raw = orjson.loads(response.content)
        
        trades = []
        for t in trades_raw:
            # Convert unix timestamp to datetime
            ts = datetime.fromtimestamp(t['timestamp'], tz=timezone.utc)
            
            trades.append({
                'ts': ts,
                'market_id': '',  # Need to lookup from condition_id
                'condition_id': t.get('conditionId', ''),
                'token_id': t.get('asset', ''),
                'side': t.get('side', 'UNKNOWN'),
                'price': float(t.get('price', 0)),
                'size': float(t.get('size', 0)),
                'outcome': t.get('outcome', ''),
                'outcome_index': t.get('outcomeIndex', 0),
                'trade_id': t.get('transactionHash', ''),
                'maker_address': None,
                'taker_address': t.get('proxyWallet', ''),
                'source': 'data_api',
            })
        
        return trades

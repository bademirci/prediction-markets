"""WebSocket client for Polymarket real-time data - Optimized."""

import asyncio
import websockets
import orjson
from datetime import datetime, timezone
from typing import Callable, Any
from websockets.client import WebSocketClientProtocol

from .config import PolymarketConfig


class PolymarketWebSocket:
    """WebSocket client with latency tracking and deep orderbook support."""
    
    def __init__(
        self,
        config: PolymarketConfig,
        on_trade: Callable[[dict], Any] | None = None,
        on_price_change: Callable[[dict], Any] | None = None,
        on_book: Callable[[dict], Any] | None = None,
    ):
        self.config = config
        self.on_trade = on_trade
        self.on_price_change = on_price_change
        self.on_book = on_book
        
        self._ws: WebSocketClientProtocol | None = None
        self._running = False
        self._subscribed_tokens: set[str] = set()
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 30.0
    
    async def connect(self) -> None:
        """Connect to WebSocket."""
        self._running = True
        await self._connect_with_retry()
    
    async def _connect_with_retry(self) -> None:
        """Connect with exponential backoff retry."""
        while self._running:
            try:
                if not self._ws or not self._ws.open:
                    print(f"🔌 Connecting to {self.config.websocket_url}...")
                    self._ws = await websockets.connect(
                        self.config.websocket_url,
                        ping_interval=20,
                        ping_timeout=10,
                        max_size=None,
                    )
                    print("✅ WebSocket connected!")
                    self._reconnect_delay = 1.0
                    
                    # Send any queued subscriptions
                    if self._subscribed_tokens:
                        await self._send_subscribe(list(self._subscribed_tokens))
                
                await self._listen()
                
            except Exception as e:
                print(f"❌ WebSocket error: {e}")
                if self._running:
                    print(f"🔄 Reconnecting in {self._reconnect_delay}s...")
                    await asyncio.sleep(self._reconnect_delay)
                    self._reconnect_delay = min(
                        self._reconnect_delay * 2,
                        self._max_reconnect_delay
                    )
    
    async def subscribe(self, token_ids: list[str]) -> None:
        """Subscribe to market updates for given tokens."""
        self._subscribed_tokens.update(token_ids)
        
        if self._ws and self._ws.open:
            await self._send_subscribe(token_ids)
        else:
            # Store for later when connection is established
            print(f"📝 Queued {len(token_ids)} tokens for subscription (WebSocket not connected yet)")
    
    async def _send_subscribe(self, token_ids: list[str]) -> None:
        """Send subscribe message."""
        if not self._ws:
            return
        
        if not token_ids:
            return
        
        try:
            message = {
                "type": "subscribe",
                "channel": "market",
                "assets_ids": token_ids,
            }
            
            await self._ws.send(orjson.dumps(message).decode())
            print(f"📡 ✅ Subscribed to {len(token_ids)} tokens")
        except Exception as e:
            print(f"❌ Subscription error for {len(token_ids)} tokens: {e}")
            raise
    
    async def _listen(self) -> None:
        """Listen for messages with immediate timestamping."""
        if not self._ws:
            return
        
        async for message in self._ws:
            # HFT CRITICAL: Capture Receipt Time immediately
            receipt_ts = datetime.now(timezone.utc)
            
            try:
                data = orjson.loads(message)
                if isinstance(data, list):
                    for event in data:
                        await self._handle_event(event, receipt_ts)
                else:
                    await self._handle_event(data, receipt_ts)
            except Exception as e:
                print(f"⚠️ Error handling message: {e}")
    
    async def _handle_event(self, event: dict, receipt_ts: datetime) -> None:
        """Handle single event."""
        event_type = event.get('event_type') or event.get('type')
        
        # Debug: Log unknown event types (first 10 only)
        if event_type not in ('trade', 'last_trade_price', 'book', 'TRADE', 'BOOK', 'price_change') and not hasattr(self, '_unknown_events_logged'):
            if not hasattr(self, '_unknown_event_count'):
                self._unknown_event_count = 0
            if self._unknown_event_count < 10:
                print(f"🔍 Unknown event type: {event_type}, keys: {list(event.keys())[:10]}")
                self._unknown_event_count += 1
                if self._unknown_event_count >= 10:
                    self._unknown_events_logged = True
        
        # Handle trade events - Polymarket market channel uses 'last_trade_price' for trades
        # Also check for trade-like data (has price, size, side but no bids/asks)
        is_trade = (
            event_type in ('trade', 'last_trade_price', 'TRADE', 'trade_executed', 'execution') or
            ('price' in event and 'size' in event and 'side' in event and 'bids' not in event and 'asks' not in event)
        )
        
        if is_trade:
            await self._handle_trade(event, receipt_ts)
        elif event_type in ('book', 'BOOK') or ('bids' in event or 'asks' in event):
            # Book update - check if it has bids/asks even if event_type is missing
            await self._handle_book(event, receipt_ts)
    
    async def _handle_trade(self, event: dict, receipt_ts: datetime) -> None:
        """Handle trade event with precise timing."""
        if not self.on_trade:
            return
        
        ts_val = event.get('timestamp') or event.get('ts')
        if isinstance(ts_val, (int, float)):
             exchange_ts = datetime.fromtimestamp(ts_val / 1000.0, tz=timezone.utc)
        elif isinstance(ts_val, str) and ts_val.isdigit():
             exchange_ts = datetime.fromtimestamp(int(ts_val) / 1000.0, tz=timezone.utc)
        else:
             exchange_ts = receipt_ts

        trade = {
            'exchange_ts': exchange_ts,
            'local_ts': receipt_ts,
            'market_id': event.get('market', event.get('condition_id', '')),
            'condition_id': event.get('condition_id', event.get('market', '')),
            'token_id': event.get('asset_id', event.get('asset', '')),
            'side': event.get('side', 'UNKNOWN'),
            'price': float(event.get('price', 0)),
            'size': float(event.get('size', 0)),
            'outcome': event.get('outcome', ''),
            'outcome_index': event.get('outcome_index', 0),
            'trade_id': event.get('id', event.get('trade_id', '')),
            'maker_address': event.get('maker', ''),
            'taker_address': event.get('taker', ''),
            'source': 'websocket',
        }
        
        await self.on_trade(trade)
    
    async def _handle_book(self, event: dict, receipt_ts: datetime) -> None:
        """Handle book update with full depth extraction."""
        if not self.on_book:
            return

        ts_val = event.get('timestamp') or event.get('ts')
        if isinstance(ts_val, (int, float)):
             exchange_ts = datetime.fromtimestamp(ts_val / 1000.0, tz=timezone.utc)
        else:
             exchange_ts = receipt_ts

        raw_bids = event.get('bids', [])
        raw_asks = event.get('asks', [])

        bids = [{'p': float(b['price']), 's': float(b['size'])} for b in raw_bids if b.get('price') and b.get('size')]
        asks = [{'p': float(a['price']), 's': float(a['size'])} for a in raw_asks if a.get('price') and a.get('size')]

        bids.sort(key=lambda x: x['p'], reverse=True)
        asks.sort(key=lambda x: x['p'])
        
        levels = []
        max_depth = 10 
        
        for i in range(max_depth):
            bid_px = bids[i]['p'] if i < len(bids) else None
            bid_sz = bids[i]['s'] if i < len(bids) else None
            ask_px = asks[i]['p'] if i < len(asks) else None
            ask_sz = asks[i]['s'] if i < len(asks) else None
            
            if bid_px is None and ask_px is None:
                continue

            levels.append({
                'exchange_ts': exchange_ts,
                'local_ts': receipt_ts,
                'market_id': event.get('market', ''),
                'condition_id': event.get('market', ''),
                'token_id': event.get('asset_id', ''),
                'level': i + 1,
                'bid_px': bid_px,
                'bid_sz': bid_sz,
                'ask_px': ask_px,
                'ask_sz': ask_sz,
                'source': 'websocket',
            })
            
        await self.on_book({'levels': levels})
    
    async def close(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
            print("🔌 WebSocket closed")

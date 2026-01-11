"""WebSocket client for Polymarket real-time data."""

import asyncio
import websockets
import orjson
from datetime import datetime, timezone
from typing import Callable, Any
from websockets.client import WebSocketClientProtocol

from .config import PolymarketConfig


class PolymarketWebSocket:
    """WebSocket client for Polymarket market channel."""
    
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
        self._max_reconnect_delay = 60.0
    
    async def connect(self) -> None:
        """Connect to WebSocket."""
        self._running = True
        await self._connect_with_retry()
    
    async def _connect_with_retry(self) -> None:
        """Connect with exponential backoff retry."""
        while self._running:
            try:
                print(f"🔌 Connecting to {self.config.websocket_url}...")
                self._ws = await websockets.connect(
                    self.config.websocket_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                )
                print("✅ WebSocket connected!")
                self._reconnect_delay = 1.0  # Reset on successful connect
                
                # Re-subscribe if we have tokens
                if self._subscribed_tokens:
                    await self._send_subscribe(list(self._subscribed_tokens))
                
                # Start listening
                await self._listen()
                
            except websockets.ConnectionClosed as e:
                print(f"⚠️ WebSocket connection closed: {e}")
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
    
    async def _send_subscribe(self, token_ids: list[str]) -> None:
        """Send subscribe message."""
        if not self._ws:
            return
        
        message = {
            "type": "subscribe",
            "channel": "market",
            "assets_ids": token_ids,
        }
        
        await self._ws.send(orjson.dumps(message).decode())
        print(f"📡 Subscribed to {len(token_ids)} tokens")
    
    async def _listen(self) -> None:
        """Listen for messages."""
        if not self._ws:
            return
        
        async for message in self._ws:
            try:
                data = orjson.loads(message)
                await self._handle_message(data)
            except orjson.JSONDecodeError as e:
                print(f"⚠️ Failed to parse message: {e}")
            except Exception as e:
                print(f"⚠️ Error handling message: {e}")
    
    async def _handle_message(self, data: dict | list) -> None:
        """Handle incoming WebSocket message."""
        # Handle array of events
        if isinstance(data, list):
            for event in data:
                await self._handle_event(event)
        else:
            await self._handle_event(data)
    
    async def _handle_event(self, event: dict) -> None:
        """Handle single event."""
        event_type = event.get('event_type') or event.get('type')
        
        if event_type == 'trade' or event_type == 'last_trade_price':
            await self._handle_trade(event)
        elif event_type == 'price_change':
            await self._handle_price_change(event)
        elif event_type == 'book':
            await self._handle_book(event)
        elif event_type in ('subscribed', 'connected'):
            print(f"ℹ️ {event_type}: {event}")
        else:
            # Unknown event type - log for debugging
            pass
    
    async def _handle_trade(self, event: dict) -> None:
        """Handle trade event."""
        if not self.on_trade:
            return
        
        # Parse timestamp
        ts_str = event.get('timestamp') or event.get('ts')
        if ts_str:
            # Could be milliseconds or ISO string
            if isinstance(ts_str, (int, float)):
                ts = datetime.fromtimestamp(ts_str / 1000, tz=timezone.utc)
            elif isinstance(ts_str, str):
                if ts_str.isdigit():
                    ts = datetime.fromtimestamp(int(ts_str) / 1000, tz=timezone.utc)
                else:
                    ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            else:
                ts = datetime.now(timezone.utc)
        else:
            ts = datetime.now(timezone.utc)
        
        trade = {
            'ts': ts,
            'market_id': event.get('market', event.get('condition_id', '')),
            'condition_id': event.get('condition_id', event.get('market', '')),
            'token_id': event.get('asset_id', event.get('asset', '')),
            'side': event.get('side', 'UNKNOWN'),
            'price': float(event.get('price', 0)),
            'size': float(event.get('size', 0)),
            'outcome': event.get('outcome', ''),
            'outcome_index': event.get('outcome_index', 0),
            'trade_id': event.get('id', event.get('trade_id', str(ts.timestamp()))),
            'maker_address': event.get('maker'),
            'taker_address': event.get('taker', ''),
            'source': 'websocket',
        }
        
        await self.on_trade(trade)
    
    async def _handle_price_change(self, event: dict) -> None:
        """Handle price change event."""
        if not self.on_price_change:
            return
        
        await self.on_price_change(event)
    
    async def _handle_book(self, event: dict) -> None:
        """Handle book update event."""
        if not self.on_book:
            return
        
        ts = datetime.now(timezone.utc)
        
        bids = event.get('bids', [])
        asks = event.get('asks', [])
        
        bbo = {
            'ts': ts,
            'market_id': event.get('market', ''),
            'condition_id': event.get('market', ''),
            'token_id': event.get('asset_id', ''),
            'bid_px': float(bids[0]['price']) if bids else None,
            'bid_sz': float(bids[0]['size']) if bids else None,
            'ask_px': float(asks[0]['price']) if asks else None,
            'ask_sz': float(asks[0]['size']) if asks else None,
            'source': 'websocket',
        }
        
        await self.on_book(bbo)
    
    async def close(self) -> None:
        """Close WebSocket connection."""
        self._running = False
        if self._ws:
            await self._ws.close()
            print("🔌 WebSocket closed")

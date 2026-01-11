"""ClickHouse writer for Polymarket data."""

import asyncio
from datetime import datetime, timezone
from typing import Any
import clickhouse_connect
from clickhouse_connect.driver.client import Client

from .config import ClickHouseConfig


class ClickHouseWriter:
    """Handles batch writes to ClickHouse."""
    
    def __init__(self, config: ClickHouseConfig):
        self.config = config
        self.client: Client | None = None
        
        # Buffers for batch inserts
        self._trade_buffer: list[dict] = []
        self._bbo_buffer: list[dict] = []
        self._market_buffer: list[dict] = []
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
    
    def connect(self) -> None:
        """Connect to ClickHouse."""
        self.client = clickhouse_connect.get_client(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password,
        )
        print(f"✅ Connected to ClickHouse at {self.config.host}:{self.config.port}")
    
    def close(self) -> None:
        """Close connection."""
        if self.client:
            self.client.close()
            print("🔌 ClickHouse connection closed")
    
    async def buffer_trade(self, trade: dict) -> None:
        """Add trade to buffer."""
        async with self._lock:
            self._trade_buffer.append(trade)
    
    async def buffer_bbo(self, bbo: dict) -> None:
        """Add BBO snapshot to buffer."""
        async with self._lock:
            self._bbo_buffer.append(bbo)
    
    async def buffer_market(self, market: dict) -> None:
        """Add market to buffer."""
        async with self._lock:
            self._market_buffer.append(market)
    
    async def flush_trades(self) -> int:
        """Flush trade buffer to ClickHouse."""
        async with self._lock:
            if not self._trade_buffer:
                return 0
            
            trades = self._trade_buffer.copy()
            self._trade_buffer.clear()
        
        # Convert to column format for clickhouse-connect
        columns = [
            'ts', 'market_id', 'condition_id', 'token_id', 'side',
            'price', 'size', 'outcome', 'outcome_index', 'trade_id',
            'maker_address', 'taker_address', 'source'
        ]
        
        data = []
        for t in trades:
            data.append([
                t['ts'],
                t['market_id'],
                t['condition_id'],
                t['token_id'],
                t['side'],
                t['price'],
                t['size'],
                t['outcome'],
                t['outcome_index'],
                t['trade_id'],
                t.get('maker_address'),
                t['taker_address'],
                t.get('source', 'websocket'),
            ])
        
        self.client.insert(
            'trades_raw',
            data,
            column_names=columns,
        )
        
        return len(trades)
    
    async def flush_bbo(self) -> int:
        """Flush BBO buffer to ClickHouse."""
        async with self._lock:
            if not self._bbo_buffer:
                return 0
            
            bbos = self._bbo_buffer.copy()
            self._bbo_buffer.clear()
        
        columns = [
            'ts', 'market_id', 'condition_id', 'token_id',
            'bid_px', 'bid_sz', 'ask_px', 'ask_sz', 'last_trade_price', 'source'
        ]
        
        data = []
        for b in bbos:
            data.append([
                b['ts'],
                b['market_id'],
                b['condition_id'],
                b['token_id'],
                b.get('bid_px'),
                b.get('bid_sz'),
                b.get('ask_px'),
                b.get('ask_sz'),
                b.get('last_trade_price'),
                b.get('source', 'websocket'),
            ])
        
        self.client.insert(
            'bbo_raw',
            data,
            column_names=columns,
        )
        
        return len(bbos)
    
    async def flush_markets(self) -> int:
        """Flush market buffer to ClickHouse."""
        async with self._lock:
            if not self._market_buffer:
                return 0
            
            markets = self._market_buffer.copy()
            self._market_buffer.clear()
        
        columns = [
            'market_id', 'condition_id', 'question', 'slug', 'category',
            'computed_category', 'outcomes', 'clob_token_ids', 'end_date', 'active', 'closed',
            'volume_total', 'liquidity', 'best_bid', 'best_ask', 'last_trade_price',
            'updated_at'
        ]
        
        data = []
        now = datetime.now(timezone.utc)
        for m in markets:
            data.append([
                m['market_id'],
                m['condition_id'],
                m['question'],
                m['slug'],
                m['category'],
                m.get('computed_category', 'Other'),
                m['outcomes'],
                m['clob_token_ids'],
                m.get('end_date'),
                m.get('active', 1),
                m.get('closed', 0),
                m.get('volume_total', 0),
                m.get('liquidity', 0),
                m.get('best_bid', 0),
                m.get('best_ask', 0),
                m.get('last_trade_price', 0),
                now,
            ])
        
        self.client.insert(
            'markets_dim',
            data,
            column_names=columns,
        )
        
        return len(markets)
    
    async def flush_all(self) -> dict[str, int]:
        """Flush all buffers."""
        trades = await self.flush_trades()
        bbos = await self.flush_bbo()
        markets = await self.flush_markets()
        
        return {
            'trades': trades,
            'bbo': bbos,
            'markets': markets,
        }

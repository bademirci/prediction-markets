"""ClickHouse writer for Polymarket data - HFT & Research Optimized."""

import asyncio
from datetime import datetime, timezone
import clickhouse_connect
from clickhouse_connect.driver.client import Client

from .config import ClickHouseConfig


class ClickHouseWriter:
    """Handles batch writes to ClickHouse with HFT precision schemas."""
    
    def __init__(self, config: ClickHouseConfig):
        self.config = config
        self.client: Client | None = None
        
        # Buffers for batch inserts
        self._trade_buffer: list[dict] = []
        self._bbo_buffer: list[dict] = []
        self._orderbook_levels_buffer: list[dict] = []
        self._market_buffer: list[dict] = []
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
    
    def connect(self) -> None:
        """Connect to ClickHouse and initialize optimized HFT tables."""
        self.client = clickhouse_connect.get_client(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password,
        )
        print(f"✅ Connected to ClickHouse at {self.config.host}:{self.config.port}")
        self._init_schema()

    def _init_schema(self) -> None:
        """Initializes HFT-ready database schema."""
        if not self.client:
            return

        # 1. TRADES: Latency tracking ve Taker/Maker analizi için optimize edildi
        self.client.command("""
            CREATE TABLE IF NOT EXISTS trades_raw
            (
                exchange_ts DateTime64(6, 'UTC'),
                local_ts DateTime64(6, 'UTC') DEFAULT now64(),
                delta_t_ms Float32 MATERIALIZED dateDiff('ms', exchange_ts, local_ts),
                
                market_id LowCardinality(String),
                condition_id String,
                token_id String,
                side Enum8('BUY' = 1, 'SELL' = -1, 'UNKNOWN' = 0),
                price Float64,
                size Float64,
                outcome LowCardinality(String),
                outcome_index UInt8,
                maker_address String,
                taker_address String,
                trade_id String,
                source LowCardinality(String)
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(exchange_ts)
            ORDER BY (market_id, exchange_ts, token_id)
        """)

        # 2. ORDERBOOK: Heatmap ve Derinlik analizi için optimize edildi
        self.client.command("""
            CREATE TABLE IF NOT EXISTS orderbook_levels
            (
                exchange_ts DateTime64(6, 'UTC'),
                local_ts DateTime64(6, 'UTC') DEFAULT now64(),
                
                market_id LowCardinality(String),
                condition_id String,
                token_id String,
                level UInt8,
                
                bid_px Nullable(Float64),
                bid_sz Nullable(Float64),
                ask_px Nullable(Float64),
                ask_sz Nullable(Float64),
                source LowCardinality(String)
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(exchange_ts)
            ORDER BY (market_id, token_id, exchange_ts, level)
        """)
        
        # 3. MARKETS DIM (Metadata)
        self.client.command("""
            CREATE TABLE IF NOT EXISTS markets_dim
            (
                market_id String,
                condition_id String,
                event_id String,
                event_slug String,
                event_title String,
                event_start_date Nullable(DateTime),
                event_end_date Nullable(DateTime),
                event_tags Array(String),
                question String,
                slug String,
                category LowCardinality(String),
                computed_category LowCardinality(String),
                outcomes String,
                clob_token_ids Array(String),
                end_date Nullable(DateTime),
                active UInt8,
                closed UInt8,
                volume_total Float64,
                liquidity Float64,
                best_bid Float64,
                best_ask Float64,
                last_trade_price Float64,
                updated_at DateTime
            )
            ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (condition_id, market_id)
        """)
        self._ensure_markets_dim_schema()
        print("🛠️ HFT Schemas Initialized (Trades, Orderbook, Markets)")
    
    def _ensure_markets_dim_schema(self) -> None:
        """Ensure markets_dim has expected category columns."""
        if not self.client:
            return
        
        try:
            result = self.client.query("DESCRIBE TABLE markets_dim")
        except Exception as e:
            print(f"⚠️ markets_dim schema check failed: {e}")
            return
        
        existing = {row[0] for row in result.result_rows}
        if "event_id" not in existing:
            self.client.command(
                "ALTER TABLE markets_dim ADD COLUMN IF NOT EXISTS event_id String AFTER condition_id"
            )
        if "event_slug" not in existing:
            self.client.command(
                "ALTER TABLE markets_dim ADD COLUMN IF NOT EXISTS event_slug String AFTER event_id"
            )
        if "event_title" not in existing:
            self.client.command(
                "ALTER TABLE markets_dim ADD COLUMN IF NOT EXISTS event_title String AFTER event_slug"
            )
        if "event_start_date" not in existing:
            self.client.command(
                "ALTER TABLE markets_dim ADD COLUMN IF NOT EXISTS event_start_date Nullable(DateTime) AFTER event_title"
            )
        if "event_end_date" not in existing:
            self.client.command(
                "ALTER TABLE markets_dim ADD COLUMN IF NOT EXISTS event_end_date Nullable(DateTime) AFTER event_start_date"
            )
        if "event_tags" not in existing:
            self.client.command(
                "ALTER TABLE markets_dim ADD COLUMN IF NOT EXISTS event_tags Array(String) AFTER event_end_date"
            )
        if "category" not in existing:
            self.client.command(
                "ALTER TABLE markets_dim ADD COLUMN IF NOT EXISTS category LowCardinality(String) AFTER slug"
            )
        if "computed_category" not in existing:
            self.client.command(
                "ALTER TABLE markets_dim ADD COLUMN IF NOT EXISTS computed_category LowCardinality(String) AFTER category"
            )
    
    def close(self) -> None:
        """Close connection."""
        if self.client:
            self.client.close()
            print("🔌 ClickHouse connection closed")
    
    async def buffer_trade(self, trade: dict) -> None:
        async with self._lock:
            self._trade_buffer.append(trade)
    
    async def buffer_bbo(self, bbo: dict) -> None:
        async with self._lock:
            self._bbo_buffer.append(bbo)
    
    async def buffer_orderbook_levels(self, levels: list[dict]) -> None:
        async with self._lock:
            self._orderbook_levels_buffer.extend(levels)
    
    async def buffer_market(self, market: dict) -> None:
        async with self._lock:
            self._market_buffer.append(market)
    
    async def flush_trades(self) -> int:
        """Flush trade buffer to ClickHouse with HFT columns."""
        async with self._lock:
            if not self._trade_buffer:
                return 0
            trades = self._trade_buffer.copy()
            self._trade_buffer.clear()
        
        columns = [
            'exchange_ts', 'local_ts', 'market_id', 'condition_id', 'token_id', 
            'side', 'price', 'size', 'outcome', 'outcome_index', 
            'trade_id', 'maker_address', 'taker_address', 'source'
        ]
        
        data = []
        for t in trades:
            data.append([
                t['exchange_ts'],
                t['local_ts'],
                t['market_id'],
                t['condition_id'],
                t['token_id'],
                t['side'],
                t['price'],
                t['size'],
                t['outcome'],
                t['outcome_index'],
                t['trade_id'],
                t.get('maker_address', ''),
                t.get('taker_address', ''),
                t.get('source', 'websocket'),
            ])
        
        try:
            self.client.insert('trades_raw', data, column_names=columns)
            return len(trades)
        except Exception as e:
            print(f"⚠️ Insert Error (Trades): {e}")
            return 0
    
    async def flush_bbo(self) -> int:
        """Flush BBO buffer (Legacy support, mostly unused in HFT mode)."""
        async with self._lock:
            count = len(self._bbo_buffer)
            self._bbo_buffer.clear() 
            return count
    
    async def flush_orderbook_levels(self) -> int:
        """Flush orderbook levels buffer to ClickHouse."""
        async with self._lock:
            if not self._orderbook_levels_buffer:
                return 0
            levels = self._orderbook_levels_buffer.copy()
            self._orderbook_levels_buffer.clear()
        
        columns = [
            'exchange_ts', 'local_ts', 'market_id', 'condition_id', 
            'token_id', 'level', 'bid_px', 'bid_sz', 'ask_px', 'ask_sz', 'source'
        ]
        
        data = []
        for l in levels:
            # Handle None values - convert to None (which is now allowed with Nullable)
            bid_px = l.get('bid_px')
            bid_sz = l.get('bid_sz')
            ask_px = l.get('ask_px')
            ask_sz = l.get('ask_sz')
            
            # Skip levels with no data at all
            if bid_px is None and bid_sz is None and ask_px is None and ask_sz is None:
                continue
            
            data.append([
                l['exchange_ts'],
                l['local_ts'],
                l['market_id'],
                l['condition_id'],
                l['token_id'],
                l['level'],
                bid_px,  # Can be None (Nullable)
                bid_sz,  # Can be None (Nullable)
                ask_px,  # Can be None (Nullable)
                ask_sz,  # Can be None (Nullable)
                l.get('source', 'websocket'),
            ])
        
        try:
            self.client.insert('orderbook_levels', data, column_names=columns)
            return len(levels)
        except Exception as e:
            print(f"⚠️ Insert Error (Levels): {e}")
            return 0
    
    async def flush_markets(self) -> int:
        """Flush market buffer to ClickHouse."""
        async with self._lock:
            if not self._market_buffer:
                return 0
            markets = self._market_buffer.copy()
            self._market_buffer.clear()
        
        columns = [
            'market_id', 'condition_id', 'event_id', 'event_slug', 'event_title',
            'event_start_date', 'event_end_date', 'event_tags',
            'question', 'slug', 'category',
            'computed_category', 'outcomes', 'clob_token_ids', 'end_date', 'active', 'closed',
            'volume_total', 'liquidity', 'best_bid', 'best_ask', 'last_trade_price',
            'updated_at'
        ]
        
        data = []
        now = datetime.now(timezone.utc)
        for m in markets:
            outcomes_str = str(m['outcomes']) if isinstance(m['outcomes'], (list, dict)) else m['outcomes']
            
            data.append([
                m['market_id'],
                m['condition_id'],
                m.get('event_id', ''),
                m.get('event_slug', ''),
                m.get('event_title', ''),
                m.get('event_start_date'),
                m.get('event_end_date'),
                m.get('event_tags', []),
                m['question'],
                m['slug'],
                m['category'],
                m.get('computed_category', 'Other'),
                outcomes_str,
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
        
        try:
            self.client.insert('markets_dim', data, column_names=columns)
            return len(markets)
        except Exception as e:
            print(f"⚠️ Insert Error (Markets): {e}")
            return 0
    
    async def flush_all(self) -> dict[str, int]:
        """Flush all buffers."""
        trades = await self.flush_trades()
        levels = await self.flush_orderbook_levels()
        markets = await self.flush_markets()
        
        return {
            'trades': trades,
            'bbo': 0,
            'orderbook_levels': levels,
            'markets': markets,
        }

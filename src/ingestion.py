"""Main ingestion orchestrator for Polymarket data."""

import asyncio
import json
import signal
from datetime import datetime, timezone
from pathlib import Path

from .config import Config, get_config
from .clickhouse_writer import ClickHouseWriter
from .polymarket_rest import PolymarketRestClient
from .websocket_client import PolymarketWebSocket


# Load computed categories
CATEGORIES_FILE = Path(__file__).parent.parent / "market_categories.json"
COMPUTED_CATEGORIES: dict[str, str] = {}

if CATEGORIES_FILE.exists():
    with open(CATEGORIES_FILE) as f:
        COMPUTED_CATEGORIES = json.load(f)
    print(f"📂 Loaded {len(COMPUTED_CATEGORIES)} computed categories")


class PolymarketIngestion:
    """Orchestrates data ingestion from Polymarket to ClickHouse."""
    
    def __init__(self, config: Config | None = None):
        self.config = config or get_config()
        
        # Initialize components
        self.writer = ClickHouseWriter(self.config.clickhouse)
        self.rest_client = PolymarketRestClient(self.config.polymarket)
        self.ws_client: PolymarketWebSocket | None = None
        
        # State
        self._running = False
        self._markets: dict[str, dict] = {}  # condition_id -> market
        self._token_to_market: dict[str, str] = {}  # token_id -> condition_id
        
        # Stats
        self._stats = {
            'trades_received': 0,
            'bbo_received': 0,
            'trades_written': 0,
            'bbo_written': 0,
            'markets_synced': 0,
        }
    
    async def start(self) -> None:
        """Start ingestion."""
        print("🚀 Starting Polymarket Ingestion")
        print(f"   ClickHouse: {self.config.clickhouse.host}:{self.config.clickhouse.port}")
        print(f"   Max markets: {self.config.max_markets}")
        if self.config.category_filter:
            print(f"   🎯 Category filter: {self.config.category_filter}")
        
        # Connect to ClickHouse
        self.writer.connect()
        
        self._running = True
        
        # Initial market sync
        await self._sync_markets()
        
        # Setup WebSocket client
        self.ws_client = PolymarketWebSocket(
            config=self.config.polymarket,
            on_trade=self._on_trade,
            on_book=self._on_book,
        )
        
        # Get token IDs to subscribe (optionally filtered by category)
        token_ids = list(self._token_to_market.keys())
        
        # If category filter is set, get tokens directly from ClickHouse
        if self.config.category_filter:
            import clickhouse_connect
            ch = clickhouse_connect.get_client(
                host=self.config.clickhouse.host,
                port=self.config.clickhouse.port,
                database=self.config.clickhouse.database,
            )
            result = ch.query(f"""
                SELECT 
                    condition_id,
                    market_id,
                    arrayJoin(clob_token_ids) as token_id 
                FROM markets_dim 
                WHERE computed_category = '{self.config.category_filter}'
                  AND length(clob_token_ids) > 0
            """)
            # Build token -> market mapping from ClickHouse
            token_ids = []
            for row in result.result_rows:
                condition_id, market_id, token_id = row
                if token_id:
                    token_ids.append(token_id)
                    self._token_to_market[token_id] = condition_id
                    if condition_id not in self._markets:
                        self._markets[condition_id] = {'market_id': market_id, 'condition_id': condition_id}
            print(f"🎯 Loaded {len(token_ids)} {self.config.category_filter} tokens from ClickHouse")
        
        if not token_ids:
            print("⚠️ No tokens to subscribe to. Exiting.")
            return
        
        print(f"📊 Subscribing to {len(token_ids)} tokens from {len(self._markets)} markets")
        
        # Start background tasks
        tasks = [
            asyncio.create_task(self._run_websocket(token_ids)),
            asyncio.create_task(self._run_flusher()),
            asyncio.create_task(self._run_market_sync()),
            asyncio.create_task(self._run_stats_reporter()),
        ]
        
        # Wait for all tasks
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("⏹️ Shutting down...")
    
    async def stop(self) -> None:
        """Stop ingestion."""
        print("⏹️ Stopping ingestion...")
        self._running = False
        
        if self.ws_client:
            await self.ws_client.close()
        
        # Final flush
        results = await self.writer.flush_all()
        print(f"📤 Final flush: {results}")
        
        await self.rest_client.close()
        self.writer.close()
        
        print("✅ Ingestion stopped")
    
    async def _sync_markets(self) -> None:
        """Sync markets from Gamma API."""
        print("📥 Syncing markets...")
        
        try:
            markets = await self.rest_client.fetch_active_markets(
                limit=self.config.max_markets
            )

            # Keywords to auto-detect Sports/Esports if not in our manual list
            sports_keywords = [
                " vs ", "League", "Cup", "Tournament", "Championship", "NBA", "NFL", 
                "Premier League", "Champions League", "UFC", "F1", "Grand Prix",
                "Dota", "Counter-Strike", "CS2", "LoL", "Valorant", "Overwatch"
            ]
            
            for market in markets:
                condition_id = market['condition_id']
                market_id_str = str(market.get('market_id', ''))
                
                # 1. Try to get from our curated list
                computed_cat = COMPUTED_CATEGORIES.get(market_id_str)

                # 2. If not in list (NEW MARKET), try to infer
                if not computed_cat:
                    api_category = market.get('category', 'Unknown')
                    question_text = market.get('question', '')
                    
                    # Trust API if it says Sports
                    if api_category == 'Sports':
                        computed_cat = 'Sports'
                    # Check keywords for new Esports/Sports markets
                    elif any(k in question_text for k in sports_keywords):
                        computed_cat = 'Sports'
                    else:
                        computed_cat = api_category

                # Add computed category
                market['computed_category'] = computed_cat or 'Other'
                
                # Apply category filter if set
                if self.config.category_filter and market['computed_category'] != self.config.category_filter:
                    continue

                self._markets[condition_id] = market
                
                # Map token IDs to condition ID
                for token_id in market.get('clob_token_ids', []):
                    self._token_to_market[token_id] = condition_id
                
                # Buffer for ClickHouse
                await self.writer.buffer_market(market)
            
            # Flush markets
            count = await self.writer.flush_markets()
            self._stats['markets_synced'] = len(markets)
            
            print(f"✅ Synced {count} markets, {len(self._token_to_market)} tokens (filtered: {self.config.category_filter})")
            
        except Exception as e:
            print(f"❌ Error syncing markets: {e}")
    
    async def _on_trade(self, trade: dict) -> None:
        """Handle incoming trade from WebSocket."""
        self._stats['trades_received'] += 1
        
        # Enrich with market_id if missing
        if not trade.get('market_id') and trade.get('token_id'):
            condition_id = self._token_to_market.get(trade['token_id'])
            if condition_id:
                trade['market_id'] = self._markets.get(condition_id, {}).get('market_id', '')
                trade['condition_id'] = condition_id
        
        await self.writer.buffer_trade(trade)
    
    async def _on_book(self, book_data: dict) -> None:
        """Handle incoming BBO and orderbook levels update from WebSocket."""
        self._stats['bbo_received'] += 1
        
        # Handle new format: {'bbo': {...}, 'levels': [...]}
        if 'bbo' in book_data and 'levels' in book_data:
            bbo = book_data['bbo']
            levels = book_data['levels']
        else:
            # Backward compatibility: old format (just BBO)
            bbo = book_data
            levels = []
        
        # Enrich BBO with market_id if missing
        if not bbo.get('market_id') and bbo.get('token_id'):
            condition_id = self._token_to_market.get(bbo['token_id'])
            if condition_id:
                bbo['market_id'] = self._markets.get(condition_id, {}).get('market_id', '')
                bbo['condition_id'] = condition_id
        
        # Enrich levels with market_id if missing
        for level in levels:
            if not level.get('market_id') and level.get('token_id'):
                condition_id = self._token_to_market.get(level['token_id'])
                if condition_id:
                    level['market_id'] = self._markets.get(condition_id, {}).get('market_id', '')
                    level['condition_id'] = condition_id
        
        # Buffer both BBO (for backward compatibility) and levels
        await self.writer.buffer_bbo(bbo)
        if levels:
            await self.writer.buffer_orderbook_levels(levels)
    
    async def _run_websocket(self, token_ids: list[str]) -> None:
        """Run WebSocket connection."""
        if not self.ws_client:
            return
        
        await self.ws_client.subscribe(token_ids)
        await self.ws_client.connect()
    
    async def _run_flusher(self) -> None:
        """Periodically flush buffers to ClickHouse."""
        while self._running:
            await asyncio.sleep(self.config.flush_interval)
            
            try:
                results = await self.writer.flush_all()
                
                self._stats['trades_written'] += results['trades']
                self._stats['bbo_written'] += results['bbo']
                
            except Exception as e:
                print(f"❌ Error flushing: {e}")
    
    async def _run_market_sync(self) -> None:
        """Periodically sync markets."""
        while self._running:
            await asyncio.sleep(self.config.polymarket.market_sync_interval)
            await self._sync_markets()
    
    async def _run_stats_reporter(self) -> None:
        """Report stats periodically."""
        while self._running:
            await asyncio.sleep(10)  # Every 10 seconds
            
            print(
                f"📊 Stats | "
                f"Trades: {self._stats['trades_received']} recv / {self._stats['trades_written']} written | "
                f"BBO: {self._stats['bbo_received']} recv / {self._stats['bbo_written']} written | "
                f"Markets: {self._stats['markets_synced']}"
            )


async def main():
    """Main entry point."""
    config = get_config()
    ingestion = PolymarketIngestion(config)
    
    # Handle signals
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        asyncio.create_task(ingestion.stop())
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        await ingestion.start()
    except KeyboardInterrupt:
        await ingestion.stop()


if __name__ == "__main__":
    asyncio.run(main())

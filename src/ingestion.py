"""Main ingestion orchestrator for Polymarket data - HFT Optimized."""

import asyncio
import json
import signal
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
    """Orchestrates HFT data ingestion from Polymarket to ClickHouse."""
    
    def __init__(self, config: Config | None = None):
        self.config = config or get_config()
        
        # Initialize components
        self.writer = ClickHouseWriter(self.config.clickhouse)
        self.rest_client = PolymarketRestClient(self.config.polymarket)
        self.ws_client: PolymarketWebSocket | None = None
        
        # State
        self._running = False
        self._markets: dict[str, dict] = {}
        self._token_to_market: dict[str, str] = {}
        
        # Stats
        self._stats = {
            'trades_received': 0,
            'levels_received': 0, 
            'trades_written': 0,
            'levels_written': 0,
            'markets_synced': 0,
        }
    
    async def start(self) -> None:
        """Start ingestion."""
        print("🚀 Starting Polymarket HFT Ingestion")
        print(f"   ClickHouse: {self.config.clickhouse.host}:{self.config.clickhouse.port}")
        
        self.writer.connect()
        self._running = True
        
        await self._sync_markets()
        
        self.ws_client = PolymarketWebSocket(
            config=self.config.polymarket,
            on_trade=self._on_trade,
            on_book=self._on_book,
        )
        
        token_ids = list(self._token_to_market.keys())
        
        if self.config.category_filter:
            print(f"🎯 Applying category filter: {self.config.category_filter}")
            import clickhouse_connect
            try:
                ch = clickhouse_connect.get_client(
                    host=self.config.clickhouse.host,
                    port=self.config.clickhouse.port,
                    database=self.config.clickhouse.database,
                )
                result = ch.query(f"""
                    SELECT condition_id, market_id, arrayJoin(clob_token_ids) as token_id 
                    FROM markets_dim 
                    WHERE computed_category = '{self.config.category_filter}'
                      AND length(clob_token_ids) > 0
                """)
                
                token_ids = []
                for row in result.result_rows:
                    cond_id, mkt_id, tok_id = row
                    if tok_id:
                        token_ids.append(tok_id)
                        self._token_to_market[tok_id] = cond_id
                        if cond_id not in self._markets:
                            self._markets[cond_id] = {'market_id': mkt_id, 'condition_id': cond_id}
                print(f"🎯 Filtered to {len(token_ids)} tokens from ClickHouse")
            except Exception as e:
                print(f"⚠️ Failed to query ClickHouse for filter: {e}")
        
        if not token_ids:
            print("⚠️ No tokens to subscribe to. Exiting.")
            return
        
        print(f"📊 Subscribing to {len(token_ids)} tokens...")
        
        # Start WebSocket connection and subscription in background
        ws_task = asyncio.create_task(self._run_websocket(token_ids))
        
        tasks = [
            ws_task,
            asyncio.create_task(self._run_flusher()),
            asyncio.create_task(self._run_market_sync()),
            asyncio.create_task(self._run_stats_reporter()),
        ]
        
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
        
        results = await self.writer.flush_all()
        print(f"📤 Final flush: {results}")
        
        await self.rest_client.close()
        self.writer.close()
        print("✅ Ingestion stopped")

    async def _enrich_market_info(self, item: dict) -> None:
        if not item.get('market_id') and item.get('token_id'):
            condition_id = self._token_to_market.get(item['token_id'])
            if condition_id:
                market_data = self._markets.get(condition_id, {})
                item['condition_id'] = condition_id
                item['market_id'] = market_data.get('market_id', '')
                if 'computed_category' in market_data:
                    item['category'] = market_data['computed_category']

    async def _on_trade(self, trade: dict) -> None:
        self._stats['trades_received'] += 1
        await self._enrich_market_info(trade)
        await self.writer.buffer_trade(trade)
    
    async def _on_book(self, book_data: dict) -> None:
        levels = book_data.get('levels', [])
        self._stats['levels_received'] += len(levels)
        
        for level in levels:
            await self._enrich_market_info(level)
        
        if levels:
            await self.writer.buffer_orderbook_levels(levels)
    
    async def _sync_markets(self) -> None:
        print("📥 Syncing markets...")
        try:
            markets = await self.rest_client.fetch_active_markets(limit=self.config.max_markets)
            
            sports_keywords = [
                " vs ", "League", "Cup", "Tournament", "Championship", "NBA", "NFL", 
                "Premier League", "Champions League", "UFC", "F1", "Grand Prix"
            ]
            
            for market in markets:
                cond_id = market['condition_id']
                mkt_id = str(market.get('market_id', ''))
                
                computed_cat = COMPUTED_CATEGORIES.get(mkt_id)
                if not computed_cat:
                    api_cat = market.get('category', 'Unknown')
                    q_text = market.get('question', '')
                    
                    if api_cat == 'Sports': computed_cat = 'Sports'
                    elif any(k in q_text for k in sports_keywords): computed_cat = 'Sports'
                    else: computed_cat = api_cat
                
                market['computed_category'] = computed_cat or 'Other'
                
                self._markets[cond_id] = market
                for token_id in market.get('clob_token_ids', []):
                    self._token_to_market[token_id] = cond_id
                
                await self.writer.buffer_market(market)
            
            count = await self.writer.flush_markets()
            self._stats['markets_synced'] = len(markets)
            print(f"✅ Synced {count} markets. Known tokens: {len(self._token_to_market)}")
            
        except Exception as e:
            print(f"❌ Error syncing markets: {e}")
    
    async def _run_websocket(self, token_ids: list[str]) -> None:
        """Subscribe to tokens in batches to avoid WebSocket limits."""
        if not self.ws_client: return
        
        # WebSocket subscription limit - subscribe in batches
        # Polymarket WebSocket may have limits on number of tokens per subscription
        batch_size = 1000  # Subscribe to 1000 tokens at a time
        total_tokens = len(token_ids)
        total_batches = (total_tokens + batch_size - 1) // batch_size
        
        print(f"📡 Subscribing to {total_tokens:,} tokens in {total_batches} batches of {batch_size}...")
        
        # Manually connect WebSocket (don't use connect() as it blocks on _listen)
        import websockets
        self.ws_client._running = True
        
        try:
            print(f"🔌 Connecting to {self.ws_client.config.websocket_url}...")
            self.ws_client._ws = await websockets.connect(
                self.ws_client.config.websocket_url,
                ping_interval=20,
                ping_timeout=10,
                max_size=None,
            )
            print("✅ WebSocket connected!")
            
            # Start _listen in background
            listen_task = asyncio.create_task(self.ws_client._listen())
            
            # Small delay for connection to stabilize
            await asyncio.sleep(1)
            
            # Subscribe in batches
            successful_batches = 0
            failed_batches = 0
            
            for i in range(0, total_tokens, batch_size):
                batch = token_ids[i:i + batch_size]
                batch_num = i//batch_size + 1
                
                try:
                    await self.ws_client.subscribe(batch)
                    successful_batches += 1
                    if batch_num % 10 == 0 or batch_num <= 5 or batch_num > total_batches - 5:
                        print(f"   ✅ Batch {batch_num}/{total_batches}: {len(batch)} tokens subscribed")
                except Exception as e:
                    failed_batches += 1
                    print(f"   ❌ Batch {batch_num}/{total_batches} failed: {e}")
                
                # Small delay between batches to avoid overwhelming the WebSocket
                if i + batch_size < total_tokens:
                    await asyncio.sleep(0.5)
            
            print(f"\n📊 Subscription Summary:")
            print(f"   ✅ Successful: {successful_batches}/{total_batches} batches")
            print(f"   ❌ Failed: {failed_batches}/{total_batches} batches")
            print(f"   📡 Total tokens attempted: {total_tokens:,}")
            print(f"   📡 Estimated subscribed: {successful_batches * batch_size:,} tokens")
            
            # Keep listen task running
            await listen_task
            
        except Exception as e:
            print(f"❌ WebSocket error: {e}")
            import traceback
            traceback.print_exc()
    
    async def _run_flusher(self) -> None:
        while self._running:
            await asyncio.sleep(self.config.flush_interval)
            try:
                results = await self.writer.flush_all()
                self._stats['trades_written'] += results.get('trades', 0)
                self._stats['levels_written'] += results.get('orderbook_levels', 0)
            except Exception as e:
                print(f"❌ Error flushing: {e}")
    
    async def _run_market_sync(self) -> None:
        while self._running:
            await asyncio.sleep(self.config.polymarket.market_sync_interval)
            await self._sync_markets()
    
    async def _run_stats_reporter(self) -> None:
        while self._running:
            await asyncio.sleep(10)
            print(
                f"📊 Stats | "
                f"Trades: {self._stats['trades_received']} recv / {self._stats['trades_written']} written | "
                f"Levels: {self._stats['levels_received']} recv / {self._stats['levels_written']} written"
            )

async def main():
    config = get_config()
    ingestion = PolymarketIngestion(config)
    
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        print("\nReceived exit signal")
        asyncio.create_task(ingestion.stop())
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        await ingestion.start()
    except KeyboardInterrupt:
        await ingestion.stop()

if __name__ == "__main__":
    asyncio.run(main())

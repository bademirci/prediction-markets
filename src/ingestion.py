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
        self.ws_clients: list[PolymarketWebSocket] = []
        
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
        
        tasks = [
            asyncio.create_task(self._run_websocket(token_ids)),
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
        
        for ws_client in self.ws_clients:
            await ws_client.close()
        
        results = await self.writer.flush_all()
        print(f"📤 Final flush: {results}")
        
        await self.rest_client.close()
        self.writer.close()
        print("✅ Ingestion stopped")

    async def _enrich_market_info(self, item: dict) -> None:
        token_id = item.get('token_id')
        if not token_id:
            return
        
        token_id = str(token_id)
        item['token_id'] = token_id
        condition_id = self._token_to_market.get(token_id)
        if not condition_id:
            return
        
        market_data = self._markets.get(condition_id, {})
        item['condition_id'] = condition_id
        
        if item.get('market_id') in ('', condition_id, None):
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
                    token_id = str(token_id)
                    self._token_to_market[token_id] = cond_id
                
                await self.writer.buffer_market(market)
            
            count = await self.writer.flush_markets()
            self._stats['markets_synced'] = len(markets)
            print(f"✅ Synced {count} markets. Known tokens: {len(self._token_to_market)}")
            
        except Exception as e:
            print(f"❌ Error syncing markets: {e}")
    
    async def _run_websocket(self, token_ids: list[str]) -> None:
        """Subscribe to tokens across multiple WebSocket connections."""
        total_tokens = len(token_ids)
        tokens_per_connection = max(1, self.config.ws_tokens_per_connection)
        total_connections = (total_tokens + tokens_per_connection - 1) // tokens_per_connection
        if total_connections > self.config.max_ws_connections:
            tokens_per_connection = max(
                tokens_per_connection,
                (total_tokens + self.config.max_ws_connections - 1) // self.config.max_ws_connections,
            )
            total_connections = (total_tokens + tokens_per_connection - 1) // tokens_per_connection
        
        print(
            f"📡 Subscribing to {total_tokens:,} tokens across "
            f"{total_connections} WebSocket connections "
            f"({tokens_per_connection} tokens/connection)..."
        )
        
        self.ws_clients = []
        tasks = []
        
        for i in range(0, total_tokens, tokens_per_connection):
            chunk = token_ids[i:i + tokens_per_connection]
            ws_client = PolymarketWebSocket(
                config=self.config.polymarket,
                on_trade=self._on_trade,
                on_book=self._on_book,
                subscribe_batch_size=self.config.ws_subscribe_batch_size,
            )
            self.ws_clients.append(ws_client)
            
            await ws_client.subscribe(chunk)
            tasks.append(asyncio.create_task(ws_client.connect()))
            print(f"   ✅ Connection {len(self.ws_clients)}/{total_connections}: queued {len(chunk)} tokens")
        
        await asyncio.gather(*tasks)
    
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

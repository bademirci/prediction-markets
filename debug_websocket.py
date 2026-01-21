#!/usr/bin/env python3
"""Debug WebSocket messages to see what event types are actually received."""

import asyncio
import websockets
import orjson
from datetime import datetime, timezone
from src.config import get_config

async def debug_websocket():
    """Connect and log all WebSocket messages."""
    config = get_config()
    
    print(f"🔌 Connecting to {config.polymarket.websocket_url}...")
    
    try:
        async with websockets.connect(
            config.polymarket.websocket_url,
            ping_interval=20,
            ping_timeout=10,
            max_size=None,
        ) as ws:
            print("✅ Connected!")
            
            # Subscribe to a few tokens (get from ClickHouse)
            import clickhouse_connect
            ch = clickhouse_connect.get_client(
                host=config.clickhouse.host,
                port=config.clickhouse.port,
                database=config.clickhouse.database,
                user=config.clickhouse.user,
                password=config.clickhouse.password,
            )
            
            # Get some active tokens
            result = ch.query("""
                SELECT DISTINCT token_id 
                FROM orderbook_levels 
                WHERE exchange_ts > now() - INTERVAL 1 HOUR 
                LIMIT 5
            """)
            
            token_ids = [row[0] for row in result.result_rows]
            print(f"📡 Subscribing to {len(token_ids)} tokens: {token_ids[:2]}...")
            
            subscribe_msg = {
                "type": "subscribe",
                "channel": "market",
                "assets_ids": token_ids,
            }
            await ws.send(orjson.dumps(subscribe_msg).decode())
            
            print("\n📥 Listening for messages (press Ctrl+C to stop)...\n")
            print("=" * 80)
            
            event_type_counts = {}
            message_count = 0
            
            try:
                async for message in ws:
                    message_count += 1
                    receipt_ts = datetime.now(timezone.utc)
                    
                    try:
                        data = orjson.loads(message)
                        
                        # Handle array of events
                        events = data if isinstance(data, list) else [data]
                        
                        for event in events:
                            event_type = event.get('event_type') or event.get('type') or 'unknown'
                            event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
                            
                            # Print first few of each type
                            if event_type_counts[event_type] <= 3:
                                print(f"\n[{receipt_ts.strftime('%H:%M:%S.%f')[:-3]}] Event Type: {event_type}")
                                print(f"  Keys: {list(event.keys())}")
                                
                                # Print sample data (truncated)
                                sample = {k: str(v)[:50] for k, v in list(event.items())[:5]}
                                print(f"  Sample: {sample}")
                        
                        # Print stats every 50 messages
                        if message_count % 50 == 0:
                            print(f"\n📊 Stats after {message_count} messages:")
                            for et, count in sorted(event_type_counts.items(), key=lambda x: -x[1]):
                                print(f"   {et}: {count}")
                            print()
                            
                    except Exception as e:
                        print(f"⚠️ Error parsing message: {e}")
                        print(f"   Raw message (first 200 chars): {str(message)[:200]}")
                        
            except KeyboardInterrupt:
                print("\n\n📊 Final Statistics:")
                print("=" * 80)
                for et, count in sorted(event_type_counts.items(), key=lambda x: -x[1]):
                    print(f"   {et}: {count}")
                print(f"\n   Total messages: {message_count}")
                
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_websocket())

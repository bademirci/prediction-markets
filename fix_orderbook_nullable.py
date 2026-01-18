#!/usr/bin/env python3
"""Fix orderbook_levels table to allow Nullable values."""

import clickhouse_connect
from src.config import get_config

def fix_orderbook_schema():
    config = get_config()
    
    try:
        client = clickhouse_connect.get_client(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            database=config.clickhouse.database,
            user=config.clickhouse.user,
            password=config.clickhouse.password,
        )
        
        print("🔧 Fixing orderbook_levels schema (making columns Nullable)...")
        
        # Drop and recreate with Nullable columns
        print("\n🗑️ Dropping old orderbook_levels...")
        client.command("DROP TABLE IF EXISTS orderbook_levels")
        
        print("🛠️ Creating new orderbook_levels with Nullable columns...")
        client.command("""
            CREATE TABLE orderbook_levels
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
        
        print("✅ orderbook_levels fixed with Nullable columns!")
        print("\n🎉 Schema fix completed! You can now restart ingestion.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fix_orderbook_schema()

#!/usr/bin/env python3
"""Migrate ClickHouse tables to HFT schema."""

import clickhouse_connect
from src.config import get_config

def migrate_schema():
    """Drop old tables and recreate with HFT schema."""
    config = get_config()
    
    try:
        client = clickhouse_connect.get_client(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            database=config.clickhouse.database,
            user=config.clickhouse.user,
            password=config.clickhouse.password,
        )
        
        print("🔄 Migrating to HFT Schema...")
        print(f"   Database: {config.clickhouse.database}")
        print(f"   Host: {config.clickhouse.host}:{config.clickhouse.port}\n")
        
        # Check current schema
        print("📊 Checking current schema...")
        try:
            result = client.query("DESCRIBE TABLE trades_raw")
            columns = [row[0] for row in result.result_rows]
            if 'exchange_ts' in columns:
                print("   ✅ trades_raw already has HFT schema")
                return
            else:
                print(f"   ⚠️ trades_raw has old schema (columns: {', '.join(columns[:5])}...)")
        except Exception as e:
            print(f"   ℹ️ trades_raw doesn't exist yet")
        
        # Backup warning
        print("\n⚠️ WARNING: This will DROP existing tables!")
        print("   All data in trades_raw and orderbook_levels will be lost.")
        print("   Press Ctrl+C to cancel, or wait 3 seconds to continue...")
        import time
        time.sleep(3)
        
        # Drop old tables
        print("\n🗑️ Dropping old tables...")
        try:
            client.command("DROP TABLE IF EXISTS trades_raw")
            print("   ✅ Dropped trades_raw")
        except Exception as e:
            print(f"   ⚠️ Error dropping trades_raw: {e}")
        
        try:
            client.command("DROP TABLE IF EXISTS orderbook_levels")
            print("   ✅ Dropped orderbook_levels")
        except Exception as e:
            print(f"   ⚠️ Error dropping orderbook_levels: {e}")
        
        try:
            client.command("DROP TABLE IF EXISTS bbo_raw")
            print("   ✅ Dropped bbo_raw (legacy)")
        except Exception as e:
            pass  # Ignore if doesn't exist
        
        # Create new HFT schema
        print("\n🛠️ Creating HFT schema...")
        
        # 1. TRADES
        client.command("""
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
        print("   ✅ Created trades_raw with HFT schema")
        
        # 2. ORDERBOOK LEVELS
        client.command("""
            CREATE TABLE IF NOT EXISTS orderbook_levels
            (
                exchange_ts DateTime64(6, 'UTC'),
                local_ts DateTime64(6, 'UTC') DEFAULT now64(),
                
                market_id LowCardinality(String),
                condition_id String,
                token_id String,
                level UInt8,
                
                bid_px Float64,
                bid_sz Float64,
                ask_px Float64,
                ask_sz Float64,
                source LowCardinality(String)
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(exchange_ts)
            ORDER BY (market_id, token_id, exchange_ts, level)
        """)
        print("   ✅ Created orderbook_levels with HFT schema")
        
        # Verify
        print("\n✅ Verification:")
        result = client.query("DESCRIBE TABLE trades_raw")
        columns = [row[0] for row in result.result_rows]
        required = ['exchange_ts', 'local_ts', 'delta_t_ms', 'taker_address']
        for col in required:
            if col in columns:
                print(f"   ✅ trades_raw.{col}")
            else:
                print(f"   ❌ trades_raw.{col} MISSING")
        
        result = client.query("DESCRIBE TABLE orderbook_levels")
        columns = [row[0] for row in result.result_rows]
        required = ['exchange_ts', 'local_ts', 'level']
        for col in required:
            if col in columns:
                print(f"   ✅ orderbook_levels.{col}")
            else:
                print(f"   ❌ orderbook_levels.{col} MISSING")
        
        print("\n🎉 Migration completed! You can now restart ingestion.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    migrate_schema()

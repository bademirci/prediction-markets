#!/usr/bin/env python3
"""Test script to verify HFT schema changes."""

import clickhouse_connect
from src.config import get_config

def test_schema():
    """Test that new HFT schema columns exist."""
    config = get_config()
    
    try:
        client = clickhouse_connect.get_client(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            database=config.clickhouse.database,
            user=config.clickhouse.user,
            password=config.clickhouse.password,
        )
        
        print("✅ Connected to ClickHouse")
        
        # Test trades_raw schema
        print("\n📊 Testing trades_raw schema...")
        result = client.query("DESCRIBE TABLE trades_raw")
        columns = [row[0] for row in result.result_rows]
        
        required_cols = ['exchange_ts', 'local_ts', 'delta_t_ms', 'taker_address', 'maker_address']
        for col in required_cols:
            if col in columns:
                print(f"  ✅ {col} exists")
            else:
                print(f"  ❌ {col} MISSING")
        
        # Test orderbook_levels schema
        print("\n📊 Testing orderbook_levels schema...")
        result = client.query("DESCRIBE TABLE orderbook_levels")
        columns = [row[0] for row in result.result_rows]
        
        required_cols = ['exchange_ts', 'local_ts', 'level']
        for col in required_cols:
            if col in columns:
                print(f"  ✅ {col} exists")
            else:
                print(f"  ❌ {col} MISSING")
        
        # Test sample query
        print("\n📊 Testing sample queries...")
        try:
            result = client.query("SELECT count() FROM trades_raw LIMIT 1")
            print(f"  ✅ trades_raw query works: {result.result_rows[0][0]} rows")
        except Exception as e:
            print(f"  ⚠️ trades_raw query failed: {e}")
        
        try:
            result = client.query("SELECT count() FROM orderbook_levels LIMIT 1")
            print(f"  ✅ orderbook_levels query works: {result.result_rows[0][0]} rows")
        except Exception as e:
            print(f"  ⚠️ orderbook_levels query failed: {e}")
        
        print("\n✅ Schema test completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_schema()

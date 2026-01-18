#!/usr/bin/env python3
"""Test script for Grafana dashboard SQL queries."""

import clickhouse_connect
from src.config import get_config
import json

def test_dashboard_queries():
    """Test all SQL queries from the HFT dashboard."""
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
        print(f"   Host: {config.clickhouse.host}:{config.clickhouse.port}")
        print(f"   Database: {config.clickhouse.database}\n")
        
        # Load dashboard JSON
        with open('grafana_hft_microstructure.json') as f:
            dashboard = json.load(f)
        
        # Get a sample market and token for testing
        print("📊 Finding test market and token...")
        result = client.query("""
            SELECT condition_id, token_id, count() as trade_count
            FROM trades_raw
            WHERE exchange_ts >= now() - INTERVAL 24 HOUR
            GROUP BY condition_id, token_id
            ORDER BY trade_count DESC
            LIMIT 1
        """)
        
        if not result.result_rows:
            print("⚠️ No trades found in last 24 hours. Testing with schema only.")
            test_schema_only(client)
            return
        
        test_market = result.result_rows[0][0]
        test_token = result.result_rows[0][1]
        trade_count = result.result_rows[0][2]
        
        print(f"   Test Market: {test_market[:50]}...")
        print(f"   Test Token: {test_token[:30]}...")
        print(f"   Trade Count: {trade_count}\n")
        
        # Test queries from dashboard
        panels = dashboard['dashboard']['panels']
        test_count = 0
        pass_count = 0
        fail_count = 0
        
        print("🧪 Testing Dashboard Queries:\n")
        
        for panel in panels:
            if panel.get('type') == 'row':
                continue  # Skip row panels
            
            panel_title = panel.get('title', 'Unknown')
            targets = panel.get('targets', [])
            
            for target in targets:
                if 'rawSql' not in target:
                    continue
                
                sql = target['rawSql']
                # Replace variables
                sql = sql.replace('${market}', f"'{test_market}'")
                sql = sql.replace('${token}', f"'{test_token}'")
                sql = sql.replace('${category}', "'Esports'")
                sql = sql.replace('$__timeFilter(exchange_ts)', "exchange_ts >= now() - INTERVAL 2 HOUR")
                sql = sql.replace('$__timeFilter(ob.exchange_ts)', "ob.exchange_ts >= now() - INTERVAL 2 HOUR")
                
                test_count += 1
                try:
                    result = client.query(sql)
                    row_count = len(result.result_rows)
                    print(f"  ✅ Panel '{panel_title}' - Query OK ({row_count} rows)")
                    pass_count += 1
                except Exception as e:
                    print(f"  ❌ Panel '{panel_title}' - Query FAILED")
                    print(f"     Error: {str(e)[:100]}")
                    print(f"     SQL: {sql[:150]}...")
                    fail_count += 1
        
        print(f"\n📊 Test Summary:")
        print(f"   Total Queries: {test_count}")
        print(f"   ✅ Passed: {pass_count}")
        print(f"   ❌ Failed: {fail_count}")
        
        if fail_count == 0:
            print("\n🎉 All dashboard queries are working!")
        else:
            print(f"\n⚠️ {fail_count} query(s) need attention.")
        
        # Test specific HFT features
        print("\n🔬 Testing HFT Features:\n")
        
        # Test latency calculation
        try:
            result = client.query(f"""
                SELECT 
                    count() as total,
                    avg(delta_t_ms) as avg_latency,
                    max(delta_t_ms) as max_latency,
                    min(delta_t_ms) as min_latency
                FROM trades_raw
                WHERE condition_id = '{test_market}'
                  AND exchange_ts >= now() - INTERVAL 2 HOUR
                  AND delta_t_ms IS NOT NULL
            """)
            if result.result_rows:
                row = result.result_rows[0]
                print(f"  ✅ Latency Tracking: {row[0]} trades, avg={row[1]:.2f}ms, max={row[2]:.2f}ms")
        except Exception as e:
            print(f"  ⚠️ Latency Tracking: {e}")
        
        # Test orderbook levels
        try:
            result = client.query(f"""
                SELECT 
                    count() as total,
                    max(level) as max_level,
                    count(DISTINCT level) as unique_levels
                FROM orderbook_levels
                WHERE condition_id = '{test_market}'
                  AND token_id = '{test_token}'
                  AND exchange_ts >= now() - INTERVAL 2 HOUR
            """)
            if result.result_rows:
                row = result.result_rows[0]
                print(f"  ✅ Orderbook Levels: {row[0]} records, max_level={row[1]}, unique_levels={row[2]}")
        except Exception as e:
            print(f"  ⚠️ Orderbook Levels: {e}")
        
        # Test taker_address
        try:
            result = client.query(f"""
                SELECT 
                    count(DISTINCT taker_address) as unique_takers,
                    count() as trades_with_taker
                FROM trades_raw
                WHERE condition_id = '{test_market}'
                  AND exchange_ts >= now() - INTERVAL 2 HOUR
                  AND taker_address != ''
            """)
            if result.result_rows:
                row = result.result_rows[0]
                print(f"  ✅ Smart Money Tracking: {row[0]} unique takers, {row[1]} trades with taker_address")
        except Exception as e:
            print(f"  ⚠️ Smart Money Tracking: {e}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def test_schema_only(client):
    """Test schema without data."""
    print("📋 Testing Schema:\n")
    
    # Check tables exist
    tables = ['trades_raw', 'orderbook_levels', 'markets_dim']
    for table in tables:
        try:
            result = client.query(f"SELECT count() FROM {table} LIMIT 1")
            print(f"  ✅ Table '{table}' exists")
        except Exception as e:
            print(f"  ❌ Table '{table}' missing: {e}")
    
    # Check columns
    print("\n📊 Checking HFT columns:\n")
    try:
        result = client.query("DESCRIBE TABLE trades_raw")
        columns = [row[0] for row in result.result_rows]
        required = ['exchange_ts', 'local_ts', 'delta_t_ms', 'taker_address', 'maker_address']
        for col in required:
            if col in columns:
                print(f"  ✅ trades_raw.{col}")
            else:
                print(f"  ❌ trades_raw.{col} MISSING")
    except Exception as e:
        print(f"  ⚠️ Could not check trades_raw: {e}")
    
    try:
        result = client.query("DESCRIBE TABLE orderbook_levels")
        columns = [row[0] for row in result.result_rows]
        required = ['exchange_ts', 'local_ts', 'level']
        for col in required:
            if col in columns:
                print(f"  ✅ orderbook_levels.{col}")
            else:
                print(f"  ❌ orderbook_levels.{col} MISSING")
    except Exception as e:
        print(f"  ⚠️ Could not check orderbook_levels: {e}")

if __name__ == "__main__":
    test_dashboard_queries()

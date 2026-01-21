#!/usr/bin/env python3
"""Check if a specific market exists in ClickHouse."""

import clickhouse_connect
from src.config import get_config
import sys

def search_market(search_term: str):
    """Search for markets containing the search term."""
    config = get_config()
    
    try:
        client = clickhouse_connect.get_client(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            database=config.clickhouse.database,
            user=config.clickhouse.user,
            password=config.clickhouse.password,
        )
        
        print(f"🔍 '{search_term}' için market aranıyor...\n")
        
        # Search in markets_dim
        query = """
        SELECT 
            condition_id,
            market_id,
            question,
            category,
            computed_category
        FROM markets_dim
        WHERE question LIKE %(search)s
           OR question ILIKE %(search)s
        ORDER BY question
        LIMIT 20
        """
        
        result = client.query(query, parameters={'search': f'%{search_term}%'})
        
        if not result.result_rows:
            print(f"❌ '{search_term}' için market bulunamadı.\n")
            print("Tüm market'leri listeliyorum (ilk 20):")
            
            all_query = """
            SELECT 
                condition_id,
                market_id,
                question,
                category
            FROM markets_dim
            ORDER BY question
            LIMIT 20
            """
            all_result = client.query(all_query)
            for row in all_result.result_rows:
                print(f"  - {row[2]} (ID: {row[0]})")
        else:
            print(f"✅ {len(result.result_rows)} market bulundu:\n")
            for row in result.result_rows:
                condition_id, market_id, question, category, computed_category = row
                print(f"📊 {question}")
                print(f"   Condition ID: {condition_id}")
                print(f"   Market ID: {market_id}")
                print(f"   Category: {category}")
                print(f"   Computed Category: {computed_category}")
                
                # Check if there's data for this market
                trades_query = """
                SELECT 
                    count() as trade_count,
                    min(exchange_ts) as first_trade,
                    max(exchange_ts) as last_trade
                FROM trades_raw
                WHERE condition_id = %(cond_id)s
                """
                trades_result = client.query(trades_query, parameters={'cond_id': condition_id})
                if trades_result.result_rows:
                    trade_count, first, last = trades_result.result_rows[0]
                    print(f"   📈 Trades: {trade_count:,}")
                    if first and last:
                        print(f"   📅 İlk: {first}, Son: {last}")
                
                orderbook_query = """
                SELECT count() as level_count
                FROM orderbook_levels
                WHERE condition_id = %(cond_id)s
                """
                orderbook_result = client.query(orderbook_query, parameters={'cond_id': condition_id})
                if orderbook_result.result_rows:
                    level_count = orderbook_result.result_rows[0][0]
                    print(f"   📊 Orderbook Levels: {level_count:,}")
                
                print()
        
    except Exception as e:
        print(f"❌ Hata: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        search_term = " ".join(sys.argv[1:])
    else:
        search_term = "Galatasaray"
    search_market(search_term)

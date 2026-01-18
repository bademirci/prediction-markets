#!/usr/bin/env python3
"""Check current ClickHouse schema."""

import clickhouse_connect
from src.config import get_config

def check_schema():
    config = get_config()
    
    try:
        client = clickhouse_connect.get_client(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            database=config.clickhouse.database,
            user=config.clickhouse.user,
            password=config.clickhouse.password,
        )
        
        print("📊 ClickHouse Schema Kontrolü\n")
        
        # Check trades_raw
        print("1️⃣ trades_raw:")
        try:
            result = client.query("DESCRIBE TABLE trades_raw")
            columns = [row[0] for row in result.result_rows]
            
            required = ['exchange_ts', 'local_ts', 'delta_t_ms', 'taker_address', 'maker_address']
            old_cols = ['ts']  # Old schema
            
            has_new = all(col in columns for col in required)
            has_old = 'ts' in columns
            
            if has_new:
                print("   ✅ HFT Schema (exchange_ts, local_ts, delta_t_ms)")
                for col in required:
                    if col in columns:
                        print(f"      ✅ {col}")
            elif has_old:
                print("   ⚠️ Eski Schema (ts) - Migration gerekli!")
                print("      Çalıştırın: python migrate_to_hft_schema.py")
            else:
                print("   ❌ Tablo bulunamadı veya beklenmeyen şema")
                
        except Exception as e:
            print(f"   ❌ Hata: {e}")
        
        # Check orderbook_levels
        print("\n2️⃣ orderbook_levels:")
        try:
            result = client.query("DESCRIBE TABLE orderbook_levels")
            columns = [row[0] for row in result.result_rows]
            
            required = ['exchange_ts', 'local_ts', 'level']
            has_new = all(col in columns for col in required)
            has_old = 'ts' in columns
            
            if has_new:
                print("   ✅ HFT Schema (exchange_ts, local_ts, level)")
                for col in required:
                    if col in columns:
                        print(f"      ✅ {col}")
            elif has_old:
                print("   ⚠️ Eski Schema (ts) - Migration gerekli!")
            else:
                print("   ❌ Tablo bulunamadı")
                
        except Exception as e:
            print(f"   ❌ Hata: {e}")
        
        # Check data
        print("\n3️⃣ Veri Durumu:")
        try:
            result = client.query("SELECT count() FROM trades_raw WHERE exchange_ts >= now() - INTERVAL 5 MINUTE")
            if result.result_rows:
                print(f"   Son 5 dakikada trades: {result.result_rows[0][0]}")
        except Exception as e:
            print(f"   ⚠️ Veri kontrolü başarısız: {e}")
        
    except Exception as e:
        print(f"❌ Bağlantı hatası: {e}")

if __name__ == "__main__":
    check_schema()

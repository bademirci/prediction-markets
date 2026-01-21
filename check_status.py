#!/usr/bin/env python3
"""Check system status - ingestion, ClickHouse, data flow."""

import clickhouse_connect
from src.config import get_config
from datetime import datetime, timedelta
import subprocess
import sys

def check_status():
    """Check all system components."""
    print("🔍 Sistem Durumu Kontrolü\n")
    print("=" * 60)
    
    # 1. Check if ingestion process is running
    print("\n1️⃣ Ingestion Process:")
    try:
        result = subprocess.run(
            ["ps", "aux"], 
            capture_output=True, 
            text=True, 
            timeout=5
        )
        processes = [p for p in result.stdout.split('\n') if 'run_ingestion' in p or 'ingestion.py' in p]
        if processes:
            print(f"   ✅ Çalışıyor: {len(processes)} process bulundu")
            for p in processes[:2]:
                print(f"      {p.split()[1] if len(p.split()) > 1 else 'N/A'}")
        else:
            print("   ❌ Çalışmıyor - process bulunamadı")
    except Exception as e:
        print(f"   ⚠️  Kontrol edilemedi: {e}")
    
    # 2. Check ClickHouse connection
    print("\n2️⃣ ClickHouse Bağlantısı:")
    try:
        config = get_config()
        client = clickhouse_connect.get_client(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            database=config.clickhouse.database,
            user=config.clickhouse.user,
            password=config.clickhouse.password,
        )
        version = client.command("SELECT version()")
        print(f"   ✅ Bağlı - ClickHouse {version}")
    except Exception as e:
        print(f"   ❌ Bağlantı hatası: {e}")
        return
    
    # 3. Check recent data flow
    print("\n3️⃣ Veri Akışı (Son 5 dakika):")
    try:
        trades = client.query(
            'SELECT count(), max(exchange_ts) FROM trades_raw WHERE exchange_ts > now() - INTERVAL 5 MINUTE'
        ).result_rows[0]
        orderbook = client.query(
            'SELECT count(), max(exchange_ts) FROM orderbook_levels WHERE exchange_ts > now() - INTERVAL 5 MINUTE'
        ).result_rows[0]
        
        print(f"   Trades: {trades[0]:,}")
        print(f"   Orderbook Levels: {orderbook[0]:,}")
        
        if trades[0] == 0 and orderbook[0] == 0:
            print("   ⚠️  Son 5 dakikada HİÇ veri yok!")
        else:
            last_ts = trades[1] if trades[1] else orderbook[1]
            if last_ts:
                if isinstance(last_ts, str):
                    last_dt = datetime.fromisoformat(last_ts.replace('Z', '+00:00'))
                else:
                    last_dt = last_ts
                if isinstance(last_dt, datetime):
                    diff = (datetime.now(last_dt.tzinfo) - last_dt).total_seconds()
                    print(f"   ⏱️  Son veri: {diff:.0f} saniye önce")
                    if diff > 120:
                        print("   ⚠️  Veri akışı durmuş olabilir!")
    except Exception as e:
        print(f"   ⚠️  Kontrol edilemedi: {e}")
    
    # 4. Check total data
    print("\n4️⃣ Toplam Veri:")
    try:
        total_trades = client.query('SELECT count() FROM trades_raw').result_rows[0][0]
        total_orderbook = client.query('SELECT count() FROM orderbook_levels').result_rows[0][0]
        total_markets = client.query('SELECT count() FROM markets_dim').result_rows[0][0]
        
        print(f"   Trades: {total_trades:,}")
        print(f"   Orderbook Levels: {total_orderbook:,}")
        print(f"   Markets: {total_markets:,}")
    except Exception as e:
        print(f"   ⚠️  Kontrol edilemedi: {e}")
    
    # 5. Check latest timestamps
    print("\n5️⃣ Son Kayıtlar:")
    try:
        last_trade = client.query('SELECT max(exchange_ts) FROM trades_raw').result_rows[0][0]
        last_orderbook = client.query('SELECT max(exchange_ts) FROM orderbook_levels').result_rows[0][0]
        
        print(f"   Son Trade: {last_trade}")
        print(f"   Son Orderbook: {last_orderbook}")
    except Exception as e:
        print(f"   ⚠️  Kontrol edilemedi: {e}")
    
    print("\n" + "=" * 60)
    print("\n💡 Öneriler:")
    if not processes:
        print("   - Ingestion'ı başlatın: ./start_ingestion.sh")
    if trades[0] == 0 and orderbook[0] == 0:
        print("   - WebSocket bağlantısını kontrol edin")
        print("   - Market subscription'ları kontrol edin")

if __name__ == "__main__":
    check_status()

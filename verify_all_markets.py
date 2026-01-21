#!/usr/bin/env python3
"""Verify that all active markets are being subscribed."""

import clickhouse_connect
from src.config import get_config
import asyncio
from src.polymarket_rest import PolymarketRestClient

async def verify_subscription():
    """Check if all markets are being subscribed."""
    config = get_config()
    
    print("🔍 Tüm Marketlerin Dinlenip Dinlenmediğini Kontrol Ediyorum...\n")
    
    # 1. Check config
    print(f"📋 Konfigürasyon:")
    print(f"   Category Filter: {config.category_filter}")
    print(f"   Max Markets: {config.max_markets:,}")
    
    # 2. Get total active markets from API
    print(f"\n📡 Polymarket API'den aktif market sayısını çekiyorum...")
    rest_client = PolymarketRestClient(config.polymarket)
    try:
        # Fetch first page to see total
        markets_sample = await rest_client.fetch_active_markets(limit=500)
        print(f"   İlk 500 market çekildi")
        
        # Estimate total (we'll fetch more to be sure)
        print(f"   Tüm marketleri çekiyorum (bu biraz zaman alabilir)...")
        all_markets = await rest_client.fetch_active_markets(limit=config.max_markets)
        print(f"   ✅ Toplam {len(all_markets):,} market çekildi")
        
        # Count tokens
        total_tokens = 0
        for m in all_markets:
            total_tokens += len(m.get('clob_token_ids', []))
        
        print(f"   📊 Toplam {total_tokens:,} token bulundu")
        
    except Exception as e:
        print(f"   ❌ Hata: {e}")
        return
    
    # 3. Check ClickHouse
    print(f"\n💾 ClickHouse'da:")
    try:
        ch = clickhouse_connect.get_client(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            database=config.clickhouse.database,
            user=config.clickhouse.user,
            password=config.clickhouse.password,
        )
        
        ch_markets = ch.query('SELECT count() FROM markets_dim WHERE active = 1').result_rows[0][0]
        ch_tokens = ch.query('SELECT sum(length(clob_token_ids)) FROM markets_dim WHERE active = 1').result_rows[0][0]
        
        print(f"   Markets: {ch_markets:,}")
        print(f"   Tokens: {ch_tokens:,}")
        
        # Check recent subscriptions
        recent_tokens = ch.query('''
            SELECT uniq(token_id) 
            FROM (
                SELECT token_id FROM trades_raw WHERE exchange_ts > now() - INTERVAL 1 HOUR
                UNION ALL
                SELECT token_id FROM orderbook_levels WHERE exchange_ts > now() - INTERVAL 1 HOUR
            )
        ''').result_rows[0][0]
        
        print(f"\n📡 Son 1 saatte dinlenen token'lar: {recent_tokens:,}")
        print(f"   Toplam token'ların yüzdesi: {(recent_tokens/ch_tokens*100):.1f}%")
        
        if recent_tokens < ch_tokens * 0.1:
            print(f"\n⚠️  UYARI: Sadece %{(recent_tokens/ch_tokens*100):.1f} token dinleniyor!")
            print(f"   Tüm marketler dinlenmiyor olabilir.")
        else:
            print(f"\n✅ İyi görünüyor - aktif token'lar dinleniyor")
            
    except Exception as e:
        print(f"   ❌ Hata: {e}")
    
    await rest_client.close()
    
    print(f"\n💡 Not: WebSocket subscription limitleri olabilir.")
    print(f"   Polymarket WebSocket'i çok fazla token'a aynı anda subscribe olmayı desteklemeyebilir.")

if __name__ == "__main__":
    asyncio.run(verify_subscription())

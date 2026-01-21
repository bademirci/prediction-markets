#!/usr/bin/env python3
"""Final check: Are all active markets being subscribed?"""

import clickhouse_connect
from src.config import get_config
import asyncio
from src.polymarket_rest import PolymarketRestClient

async def check_all_markets():
    """Verify all markets are subscribed."""
    config = get_config()
    
    print("🔍 TÜM AKTİF MARKETLERİN DİNLENİP DİNLENMEDİĞİNİ KONTROL EDİYORUM\n")
    print("=" * 70)
    
    # 1. Get from API
    print("\n1️⃣ Polymarket API'den aktif market sayısı:")
    rest_client = PolymarketRestClient(config.polymarket)
    try:
        all_markets = await rest_client.fetch_active_markets(limit=config.max_markets)
        api_market_count = len(all_markets)
        api_token_count = sum(len(m.get('clob_token_ids', [])) for m in all_markets)
        print(f"   ✅ API'den çekilen: {api_market_count:,} market")
        print(f"   ✅ Toplam token: {api_token_count:,}")
    except Exception as e:
        print(f"   ❌ Hata: {e}")
        await rest_client.close()
        return
    
    # 2. Check ClickHouse
    print("\n2️⃣ ClickHouse'da kayıtlı:")
    ch = clickhouse_connect.get_client(
        host=config.clickhouse.host,
        port=config.clickhouse.port,
        database=config.clickhouse.database,
        user=config.clickhouse.user,
        password=config.clickhouse.password,
    )
    
    ch_markets = ch.query('SELECT count() FROM markets_dim').result_rows[0][0]
    ch_tokens = ch.query('SELECT sum(length(clob_token_ids)) FROM markets_dim').result_rows[0][0]
    ch_active = ch.query('SELECT count() FROM markets_dim WHERE active = 1').result_rows[0][0]
    
    print(f"   Markets: {ch_markets:,}")
    print(f"   Aktif Markets: {ch_active:,}")
    print(f"   Tokens: {ch_tokens:,}")
    
    # 3. Check subscription
    print("\n3️⃣ WebSocket Subscription:")
    # Get unique tokens from recent data
    recent_tokens_trades = ch.query('''
        SELECT uniq(token_id) 
        FROM trades_raw 
        WHERE exchange_ts > now() - INTERVAL 1 HOUR
    ''').result_rows[0][0]
    
    recent_tokens_orderbook = ch.query('''
        SELECT uniq(token_id) 
        FROM orderbook_levels 
        WHERE exchange_ts > now() - INTERVAL 1 HOUR
    ''').result_rows[0][0]
    
    recent_tokens_combined = ch.query('''
        SELECT uniq(token_id) 
        FROM (
            SELECT token_id FROM trades_raw WHERE exchange_ts > now() - INTERVAL 1 HOUR
            UNION ALL
            SELECT token_id FROM orderbook_levels WHERE exchange_ts > now() - INTERVAL 1 HOUR
        )
    ''').result_rows[0][0]
    
    print(f"   Son 1 saatte dinlenen token'lar: {recent_tokens_combined:,}")
    print(f"   (Trades'te: {recent_tokens_trades:,}, Orderbook'ta: {recent_tokens_orderbook:,})")
    
    # 4. Comparison
    print("\n4️⃣ Karşılaştırma:")
    print(f"   API Markets: {api_market_count:,}")
    print(f"   ClickHouse Markets: {ch_markets:,}")
    print(f"   Fark: {abs(api_market_count - ch_markets):,}")
    
    if abs(api_market_count - ch_markets) < 100:
        print("   ✅ Market sayıları eşleşiyor!")
    else:
        print("   ⚠️  Market sayıları farklı - sync devam ediyor olabilir")
    
    print(f"\n   API Tokens: {api_token_count:,}")
    print(f"   ClickHouse Tokens: {ch_tokens:,}")
    print(f"   Dinlenen Tokens: {recent_tokens_combined:,}")
    
    coverage = (recent_tokens_combined / api_token_count * 100) if api_token_count > 0 else 0
    print(f"   Kapsama: %{coverage:.2f}")
    
    if coverage > 50:
        print("   ✅ İyi kapsama - çoğu aktif market dinleniyor")
    elif coverage > 10:
        print("   ⚠️  Orta kapsama - bazı marketler dinlenmiyor olabilir")
    else:
        print("   ❌ Düşük kapsama - çoğu market dinlenmiyor!")
    
    print("\n" + "=" * 70)
    print("\n💡 Not: WebSocket subscription limitleri olabilir.")
    print("   Polymarket WebSocket'i çok fazla token'a aynı anda subscribe olmayı")
    print("   desteklemeyebilir. Bu durumda batch'ler halinde subscribe etmek gerekebilir.")
    
    await rest_client.close()

if __name__ == "__main__":
    asyncio.run(check_all_markets())

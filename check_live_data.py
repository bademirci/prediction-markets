#!/usr/bin/env python3
"""Check if ingestion is actively running."""

import clickhouse_connect
from src.config import get_config
from datetime import datetime, timezone

config = get_config()
client = clickhouse_connect.get_client(
    host=config.clickhouse.host,
    port=config.clickhouse.port,
    database=config.clickhouse.database,
    user=config.clickhouse.user,
    password=config.clickhouse.password,
)

print('🔍 GERÇEK ZAMANLI VERİ AKIŞI KONTROLÜ\n')
print('=' * 50)

# Son 30 saniye
result = client.query('''
    SELECT 
        count() as count,
        max(exchange_ts) as latest_trade
    FROM trades_raw
    WHERE exchange_ts >= now() - INTERVAL 30 SECOND
''')

count = 0
diff_seconds = 999

if result.result_rows:
    count, latest = result.result_rows[0]
    now = datetime.now(timezone.utc)
    
    if latest and count > 0:
        if isinstance(latest, datetime):
            latest_dt = latest
        else:
            latest_dt = datetime.fromisoformat(str(latest))
        
        # Ensure both are timezone-aware
        if latest_dt.tzinfo is None:
            latest_dt = latest_dt.replace(tzinfo=timezone.utc)
        
        diff_seconds = (now - latest_dt).total_seconds()
        
        print(f'⏱️  Son 30 Saniye:')
        print(f'   📈 Trades: {count}')
        print(f'   🕐 Son trade: {diff_seconds:.1f} saniye önce')
        
        if diff_seconds < 60:
            print(f'   ✅ AKTİF - Veri akışı devam ediyor!')
        elif diff_seconds < 300:
            print(f'   ⚠️ YAVAŞ - Son {diff_seconds/60:.1f} dakika önce')
        else:
            print(f'   ❌ DURMUŞ - Son {diff_seconds/60:.1f} dakika önce')
    else:
        print(f'⏱️  Son 30 Saniye:')
        print(f'   ❌ VERİ YOK - Son 30 saniyede trade yok')

# Son 2 dakika
result = client.query('''
    SELECT 
        count() as count,
        max(exchange_ts) as latest
    FROM trades_raw
    WHERE exchange_ts >= now() - INTERVAL 2 MINUTE
''')
if result.result_rows:
    count_2min, _ = result.result_rows[0]
    print(f'\n⏱️  Son 2 Dakika:')
    print(f'   📈 Trades: {count_2min}')

# Orderbook updates
result = client.query('''
    SELECT 
        count() as count,
        max(exchange_ts) as latest
    FROM orderbook_levels
    WHERE exchange_ts >= now() - INTERVAL 30 SECOND
''')
if result.result_rows:
    ob_count, ob_latest = result.result_rows[0]
    print(f'\n📊 Orderbook Updates (30 saniye):')
    print(f'   📈 Updates: {ob_count}')
    if ob_latest:
        now = datetime.now(timezone.utc)
        if isinstance(ob_latest, datetime):
            ob_latest_dt = ob_latest
        else:
            ob_latest_dt = datetime.fromisoformat(str(ob_latest))
        
        if ob_latest_dt.tzinfo is None:
            ob_latest_dt = ob_latest_dt.replace(tzinfo=timezone.utc)
        
        ob_diff = (now - ob_latest_dt).total_seconds()
        if ob_diff < 60:
            print(f'   ✅ Son update: {ob_diff:.1f} saniye önce')
        else:
            print(f'   ⚠️ Son update: {ob_diff/60:.1f} dakika önce')

# Process kontrolü
print(f'\n🔌 Process Durumu:')
try:
    import subprocess
    result = subprocess.run(['lsof', '-i', ':18123'], capture_output=True, text=True, timeout=2)
    python_conns = [line for line in result.stdout.split('\n') if 'Python' in line and 'ESTABLISHED' in line]
    if python_conns:
        print(f'   ✅ Python process ClickHouse\'a bağlı')
        print(f'   Bağlantı sayısı: {len(python_conns)}')
    else:
        print(f'   ⚠️ Python bağlantısı görünmüyor')
except Exception as e:
    print(f'   ⚠️ Process kontrolü yapılamadı: {e}')

print(f'\n' + '=' * 50)
print(f'💡 SONUÇ:')
if count > 0 and diff_seconds < 60:
    print(f'   ✅✅✅ INGESTION AKTİF ÇALIŞIYOR! ✅✅✅')
    print(f'   Veriler gerçek zamanlı geliyor.')
elif count > 0:
    print(f'   ⚠️ Veri var ama yavaş/duraklamış olabilir')
    print(f'   Son trade {diff_seconds:.0f} saniye önce')
else:
    print(f'   ❌ Ingestion çalışmıyor')
    print(f'   Yeniden başlatın: ./start_ingestion.sh')

#!/bin/bash
# Veri akışı kontrol scripti

echo "📊 Veri Akışı Kontrolü"
echo "======================"
echo ""

# ClickHouse bağlantısı kontrolü
echo "1️⃣ ClickHouse Durumu:"
if lsof -i :18123 2>/dev/null | grep -q LISTEN; then
    echo "   ✅ ClickHouse çalışıyor (port 18123)"
else
    echo "   ❌ ClickHouse çalışmıyor"
fi

# Python bağlantıları
echo ""
echo "2️⃣ Python Process'leri:"
PYTHON_CONNS=$(lsof -i -P 2>/dev/null | grep -E "Python.*18123|python.*18123" | wc -l)
if [ "$PYTHON_CONNS" -gt 0 ]; then
    echo "   ✅ $PYTHON_CONNS aktif Python bağlantısı ClickHouse'a"
    echo "   Bağlantılar:"
    lsof -i -P 2>/dev/null | grep -E "Python.*18123|python.*18123" | head -3 | awk '{print "      PID "$2" - "$9}'
else
    echo "   ⚠️ ClickHouse'a Python bağlantısı yok"
fi

# Son veri kontrolü (ClickHouse'a bağlanabilirse)
echo ""
echo "3️⃣ Son Veri Kontrolü:"
python3 << 'EOF'
try:
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
    
    # Son 2 dakika
    result = client.query("""
        SELECT 
            count() as count,
            max(exchange_ts) as latest
        FROM trades_raw
        WHERE exchange_ts >= now() - INTERVAL 2 MINUTE
    """)
    
    if result.result_rows:
        count, latest = result.result_rows[0]
        if count > 0 and latest:
            now = datetime.now(timezone.utc)
            if isinstance(latest, datetime):
                latest_dt = latest
            else:
                latest_dt = datetime.fromisoformat(str(latest))
            diff = (now - latest_dt).total_seconds()
            
            if diff < 120:
                print(f"   ✅ AKTİF - Son {diff:.0f} saniye önce")
                print(f"   📈 Son 2 dakikada: {count} trades")
            else:
                print(f"   ⚠️ DURMUŞ - Son {diff/60:.1f} dakika önce")
        else:
            print("   ❌ Son 2 dakikada veri yok")
    
    # Son 1 saat
    result = client.query("SELECT count() FROM trades_raw WHERE exchange_ts >= now() - INTERVAL 1 HOUR")
    if result.result_rows:
        print(f"   📊 Son 1 saatte: {result.result_rows[0][0]} trades")
        
except ImportError:
    print("   ⚠️ clickhouse_connect modülü bulunamadı")
    print("   (Virtual environment aktif değil olabilir)")
except Exception as e:
    print(f"   ⚠️ Bağlantı hatası: {str(e)[:50]}")
EOF

echo ""
echo "💡 Not: Eğer ingestion çalışmıyorsa:"
echo "   python run_ingestion.py"

#!/bin/bash
# Ingestion'ı durdurup yeniden başlat

cd "$(dirname "$0")"

echo "🛑 Mevcut Ingestion Process'lerini Durduruyor..."
echo ""

# Python ingestion process'lerini bul ve durdur
PIDS=$(ps aux | grep -E "python.*run_ingestion|python.*ingestion.py" | grep -v grep | awk '{print $2}')

if [ -z "$PIDS" ]; then
    echo "   ℹ️  Çalışan ingestion process'i bulunamadı"
else
    echo "   Bulunan process'ler: $PIDS"
    for PID in $PIDS; do
        echo "   🛑 Process $PID durduruluyor..."
        kill -TERM $PID 2>/dev/null
        sleep 1
        # Hala çalışıyorsa force kill
        if ps -p $PID > /dev/null 2>&1; then
            echo "   ⚠️  Process $PID hala çalışıyor, force kill..."
            kill -9 $PID 2>/dev/null
        fi
    done
    echo "   ✅ Process'ler durduruldu"
    sleep 2
fi

# ClickHouse bağlantılarını kontrol et
echo ""
echo "🔌 ClickHouse bağlantıları temizleniyor..."
sleep 1

# Virtual environment aktif et
if [ ! -d ".venv" ]; then
    echo "❌ Virtual environment bulunamadı!"
    exit 1
fi

source .venv/bin/activate

# Schema kontrolü
echo ""
echo "📊 Schema kontrolü..."
python -c "
import clickhouse_connect
from src.config import get_config
try:
    config = get_config()
    client = clickhouse_connect.get_client(
        host=config.clickhouse.host,
        port=config.clickhouse.port,
        database=config.clickhouse.database,
        user=config.clickhouse.user,
        password=config.clickhouse.password,
    )
    result = client.query('DESCRIBE TABLE trades_raw')
    columns = [row[0] for row in result.result_rows]
    if 'exchange_ts' in columns:
        print('   ✅ HFT Schema hazır')
    else:
        print('   ⚠️  Eski schema - migration gerekli')
        print('   Çalıştırın: python migrate_to_hft_schema.py')
        exit(1)
except Exception as e:
    print(f'   ❌ Hata: {e}')
    exit(1)
"

if [ $? -ne 0 ]; then
    echo ""
    echo "⚠️  Schema sorunu var. Devam etmek için Enter'a basın veya Ctrl+C ile iptal edin..."
    read
fi

# Yeni ingestion başlat
echo ""
echo "🚀 Yeni Ingestion Başlatılıyor..."
echo "   (Durdurmak için Ctrl+C)"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

python run_ingestion.py

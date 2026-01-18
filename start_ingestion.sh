#!/bin/bash
# Ingestion başlatma scripti

cd "$(dirname "$0")"

echo "🚀 Polymarket Ingestion Başlatılıyor..."
echo ""

# Virtual environment kontrolü
if [ ! -d ".venv" ]; then
    echo "❌ Virtual environment bulunamadı!"
    echo "   Önce: python3 -m venv .venv"
    exit 1
fi

# Virtual environment aktif et
source .venv/bin/activate

# Dependencies kontrolü
echo "📦 Dependencies kontrol ediliyor..."
python -c "import clickhouse_connect" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "⚠️ clickhouse_connect bulunamadı, yükleniyor..."
    pip install -q clickhouse-connect
fi

# ClickHouse bağlantı testi
echo "🔌 ClickHouse bağlantısı test ediliyor..."
python -c "
from src.config import get_config
import clickhouse_connect
try:
    config = get_config()
    client = clickhouse_connect.get_client(
        host=config.clickhouse.host,
        port=config.clickhouse.port,
        database=config.clickhouse.database,
        user=config.clickhouse.user,
        password=config.clickhouse.password,
    )
    print('✅ ClickHouse bağlantısı başarılı')
except Exception as e:
    print(f'❌ ClickHouse bağlantı hatası: {e}')
    exit(1)
"

if [ $? -ne 0 ]; then
    echo ""
    echo "⚠️ ClickHouse bağlantısı başarısız!"
    echo "   ClickHouse çalışıyor mu kontrol edin:"
    echo "   lsof -i :18123"
    exit 1
fi

echo ""
echo "📊 Ingestion başlatılıyor..."
echo "   (Durdurmak için Ctrl+C)"
echo ""

# Ingestion başlat
python run_ingestion.py

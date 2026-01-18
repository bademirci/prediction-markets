# Grafana HFT Dashboard Test Guide

## ✅ Dashboard Validation Results

- **Status**: ✅ Valid JSON
- **Total Panels**: 11
- **Query Panels**: 10
- **Total SQL Queries**: 19
- **Panel Types**: 
  - timeseries: 5
  - barchart: 2
  - row: 1
  - table: 1
  - heatmap: 2

## 📋 Panel Breakdown

1. **Trades and BBO** (timeseries) - Main price chart with ASK/BID and trade markers
2. **5s Volume** (barchart) - High-frequency volume breakdown
3. **60s Volume & Cumulative** (timeseries) - Volume metrics with cumulative tracking
4. **Orderbook Depth (10c)** (timeseries) - Liquidity depth within 10 cents
5. **5m Volume** (barchart) - Medium-term volume analysis
6. **Dual-Timeframe Row** (row) - Collapsible section header
7. **Trades and BBO (Short: 3min)** (timeseries) - High-resolution short-term view
8. **Trades and BBO (Long: 5h)** (timeseries) - Long-term trend view
9. **Smart Money Tracking** (table) - Top taker addresses analysis
10. **Network Latency Heatmap** (heatmap) - delta_t_ms visualization
11. **Orderbook Depth Heatmap** (heatmap) - 10-level depth visualization

## 🧪 Testing Steps

### 1. Import Dashboard to Grafana

```bash
# In Grafana UI:
# 1. Go to Dashboards → Import
# 2. Upload: grafana_hft_microstructure.json
# 3. Select ClickHouse datasource (uid: ff9v08z0shkw0e)
# 4. Click "Import"
```

### 2. Verify Data Source

Ensure ClickHouse datasource is configured:
- **Type**: ClickHouse
- **UID**: `ff9v08z0shkw0e` (or update in dashboard JSON)
- **Host**: Your ClickHouse server
- **Database**: `polymarket`

### 3. Test Variables

Dashboard uses 3 variables:
- **category**: Select from `markets_dim.computed_category`
- **market**: Select from markets in selected category
- **token**: Auto-selected from trades for selected market

### 4. Verify Schema

Run schema test:
```bash
python test_hft_schema.py
```

Expected columns:
- `trades_raw`: exchange_ts, local_ts, delta_t_ms, taker_address, maker_address
- `orderbook_levels`: exchange_ts, local_ts, level (1-10)

### 5. Test Queries Manually

Test a sample query in ClickHouse:
```sql
SELECT 
    exchange_ts AS time,
    ask_px AS "ASK",
    bid_px AS "BID"
FROM polymarket.orderbook_levels
WHERE condition_id = 'YOUR_MARKET_ID'
  AND token_id = 'YOUR_TOKEN_ID'
  AND level = 1
  AND exchange_ts >= now() - INTERVAL 2 HOUR
  AND ask_px IS NOT NULL
  AND bid_px IS NOT NULL
ORDER BY exchange_ts
LIMIT 100;
```

## 🔍 Common Issues & Solutions

### Issue: "No data" in panels

**Solution**: 
1. Check if ingestion is running: `python run_ingestion.py`
2. Verify data exists:
   ```sql
   SELECT count() FROM trades_raw WHERE exchange_ts >= now() - INTERVAL 2 HOUR;
   SELECT count() FROM orderbook_levels WHERE exchange_ts >= now() - INTERVAL 2 HOUR;
   ```

### Issue: "Table not found"

**Solution**: 
- Tables are auto-created on first ingestion run
- Run ingestion once to initialize schema

### Issue: "Variable not found"

**Solution**:
- Ensure `markets_dim` table has data
- Check `computed_category` values exist

### Issue: Heatmap shows no data

**Solution**:
- Verify `delta_t_ms` is calculated (MATERIALIZED column)
- Check orderbook_levels has level 1-10 data

## 📊 Expected Data Ranges

- **Price**: 0.0 - 1.0 (percentunit)
- **Volume**: USD currency
- **Latency**: milliseconds (typically 10-500ms)
- **Orderbook Levels**: 1-10
- **Depth**: Shares (thousands)

## 🚀 Next Steps

1. **Start Ingestion**: `python run_ingestion.py`
2. **Wait for Data**: Let it run for a few minutes
3. **Import Dashboard**: Upload JSON to Grafana
4. **Select Variables**: Choose category, market, token
5. **Adjust Time Range**: Set to match your data window
6. **Verify Panels**: Check each panel loads data

## 📝 Notes

- Dashboard uses `exchange_ts` for all time-based queries
- Variables use `${market}`, `${token}`, `${category}` syntax
- Time filter uses `$__timeFilter(exchange_ts)` for Grafana time picker
- All queries are optimized for ClickHouse MergeTree engine

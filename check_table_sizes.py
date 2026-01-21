#!/usr/bin/env python3
"""Check ClickHouse table sizes and disk usage."""

import clickhouse_connect
from src.config import get_config
from datetime import datetime

def format_bytes(bytes_size: int) -> str:
    """Format bytes to human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"

def check_table_sizes():
    """Check sizes of all tables in the database."""
    config = get_config()
    
    try:
        client = clickhouse_connect.get_client(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            database=config.clickhouse.database,
            user=config.clickhouse.user,
            password=config.clickhouse.password,
        )
        
        print("📊 ClickHouse Tablo Boyutları\n")
        print("=" * 80)
        
        # Get table sizes from system.parts
        query = """
        SELECT 
            table,
            formatReadableSize(sum(bytes)) as size_readable,
            sum(bytes) as size_bytes,
            sum(rows) as total_rows,
            count() as parts_count,
            min(min_date) as oldest_data,
            max(max_date) as newest_data
        FROM system.parts
        WHERE database = %(db)s
          AND active = 1
        GROUP BY table
        ORDER BY size_bytes DESC
        """
        
        result = client.query(query, parameters={'db': config.clickhouse.database})
        
        if not result.result_rows:
            print("⚠️  Hiç tablo bulunamadı veya veri yok.")
            return
        
        total_size = 0
        total_rows = 0
        
        print(f"{'Tablo':<25} {'Boyut':<15} {'Satır Sayısı':<15} {'Part Sayısı':<12} {'Tarih Aralığı'}")
        print("-" * 80)
        
        for row in result.result_rows:
            table_name, size_readable, size_bytes, rows, parts, oldest, newest = row
            total_size += size_bytes
            total_rows += rows
            
            # Format date range
            if oldest and newest:
                date_range = f"{oldest.strftime('%Y-%m-%d')} to {newest.strftime('%Y-%m-%d')}"
            else:
                date_range = "N/A"
            
            print(f"{table_name:<25} {size_readable:<15} {rows:>15,} {parts:>12} {date_range}")
        
        print("-" * 80)
        print(f"{'TOPLAM':<25} {format_bytes(total_size):<15} {total_rows:>15,}")
        print("=" * 80)
        
        # Get detailed breakdown by partition for main tables
        print("\n📈 Detaylı Partition Analizi\n")
        print("=" * 80)
        
        main_tables = ['trades_raw', 'orderbook_levels']
        
        for table in main_tables:
            try:
                partition_query = f"""
                SELECT 
                    partition,
                    formatReadableSize(sum(bytes)) as size_readable,
                    sum(bytes) as size_bytes,
                    sum(rows) as total_rows,
                    min(min_date) as min_date,
                    max(max_date) as max_date
                FROM system.parts
                WHERE database = %(db)s
                  AND table = %(table)s
                  AND active = 1
                GROUP BY partition
                ORDER BY partition DESC
                LIMIT 10
                """
                
                partition_result = client.query(
                    partition_query, 
                    parameters={'db': config.clickhouse.database, 'table': table}
                )
                
                if partition_result.result_rows:
                    print(f"\n{table}:")
                    print(f"{'Partition':<15} {'Boyut':<15} {'Satır Sayısı':<15} {'Tarih'}")
                    print("-" * 60)
                    for p_row in partition_result.result_rows:
                        part, size_readable, size_bytes, rows, min_date, max_date = p_row
                        date_str = min_date.strftime('%Y-%m') if min_date else "N/A"
                        print(f"{part:<15} {size_readable:<15} {rows:>15,} {date_str}")
            except Exception as e:
                print(f"⚠️  {table} için partition bilgisi alınamadı: {e}")
        
        # Get row counts and date ranges
        print("\n\n📋 Tablo İstatistikleri\n")
        print("=" * 80)
        
        for table in main_tables:
            try:
                stats_query = f"""
                SELECT 
                    count() as total_rows,
                    min(exchange_ts) as first_record,
                    max(exchange_ts) as last_record,
                    count() / (dateDiff('second', min(exchange_ts), max(exchange_ts)) + 1) as rows_per_second
                FROM {table}
                """
                
                stats_result = client.query(stats_query)
                if stats_result.result_rows:
                    rows, first, last, rate = stats_result.result_rows[0]
                    print(f"\n{table}:")
                    print(f"  Toplam Satır: {rows:,}")
                    if first and last:
                        print(f"  İlk Kayıt: {first}")
                        print(f"  Son Kayıt: {last}")
                        print(f"  Ortalama Hız: {rate:.2f} satır/saniye")
            except Exception as e:
                print(f"⚠️  {table} için istatistik alınamadı: {e}")
        
        print("\n" + "=" * 80)
        
    except Exception as e:
        print(f"❌ Hata: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_table_sizes()

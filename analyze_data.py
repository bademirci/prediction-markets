"""
Polymarket Data Analysis Script

This script connects to ClickHouse and provides example analyses
of Polymarket trading data.
"""

import clickhouse_connect
import pandas as pd
from datetime import datetime, timedelta
from src.config import ClickHouseConfig, get_config


def get_client() -> clickhouse_connect.driver.Client:
    """Connect to ClickHouse."""
    config = get_config().clickhouse
    client = clickhouse_connect.get_client(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user,
        password=config.password,
    )
    return client


def get_trades_df(
    client: clickhouse_connect.driver.Client,
    condition_id: str | None = None,
    hours: int = 24,
) -> pd.DataFrame:
    """
    Fetch trades as pandas DataFrame.
    
    Args:
        client: ClickHouse client
        condition_id: Filter by specific market (optional)
        hours: How many hours back to fetch
    
    Returns:
        DataFrame with columns: ts, market_id, condition_id, token_id, 
        price, size, notional, side, trade_id
    """
    query = f"""
    SELECT 
        ts,
        market_id,
        condition_id,
        token_id,
        price,
        size,
        price * size as notional,
        side,
        trade_id
    FROM polymarket.trades_raw
    WHERE ts > now() - INTERVAL {hours} HOUR
    """
    
    if condition_id:
        # Escape single quotes in condition_id
        condition_id_escaped = condition_id.replace("'", "''")
        query += f" AND condition_id = '{condition_id_escaped}'"
    
    query += " ORDER BY ts"
    
    result = client.query(query)
    
    # Convert to DataFrame
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    
    # Convert ts to datetime
    if 'ts' in df.columns:
        df['ts'] = pd.to_datetime(df['ts'])
    
    return df


def get_bbo_df(
    client: clickhouse_connect.driver.Client,
    condition_id: str | None = None,
    hours: int = 24,
) -> pd.DataFrame:
    """
    Fetch BBO (Best Bid/Ask) data as pandas DataFrame.
    
    Args:
        client: ClickHouse client
        condition_id: Filter by specific market (optional)
        hours: How many hours back to fetch
    
    Returns:
        DataFrame with columns: ts_bucket, market_id, condition_id, token_id,
        bid_px, bid_sz, ask_px, ask_sz, spread, mid_px
    """
    query = f"""
    SELECT 
        ts_bucket,
        market_id,
        condition_id,
        token_id,
        bid_px,
        bid_sz,
        ask_px,
        ask_sz,
        spread,
        mid_px
    FROM polymarket.bbo_1s
    WHERE ts_bucket > now() - INTERVAL {hours} HOUR
    """
    
    if condition_id:
        # Escape single quotes in condition_id
        condition_id_escaped = condition_id.replace("'", "''")
        query += f" AND condition_id = '{condition_id_escaped}'"
    
    query += " ORDER BY ts_bucket"
    
    result = client.query(query)
    
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    
    if 'ts_bucket' in df.columns:
        df['ts_bucket'] = pd.to_datetime(df['ts_bucket'])
    
    return df


def get_top_markets(
    client: clickhouse_connect.driver.Client,
    hours: int = 24,
    limit: int = 10,
) -> pd.DataFrame:
    """
    Get top markets by volume.
    
    Returns:
        DataFrame with columns: market_id, question, category, 
        computed_category, trade_count, total_volume
    """
    query = f"""
    SELECT 
        t.condition_id as market_id,
        any(m.question) as question,
        any(m.category) as category,
        any(m.computed_category) as computed_category,
        count() as trade_count,
        round(sum(t.price * t.size), 2) as total_volume
    FROM polymarket.trades_raw t
    LEFT JOIN polymarket.markets_dim m ON t.condition_id = m.condition_id
    WHERE t.ts > now() - INTERVAL {hours} HOUR
    GROUP BY t.condition_id
    ORDER BY trade_count DESC
    LIMIT {limit}
    """
    
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    
    return df


def get_category_stats(
    client: clickhouse_connect.driver.Client,
    hours: int = 24,
) -> pd.DataFrame:
    """
    Get volume statistics by category.
    
    Returns:
        DataFrame with columns: computed_category, market_count, 
        trade_count, total_volume
    """
    query = f"""
    SELECT 
        any(m.computed_category) as computed_category,
        uniq(t.condition_id) as market_count,
        count() as trade_count,
        round(sum(t.price * t.size), 2) as total_volume
    FROM polymarket.trades_raw t
    LEFT JOIN polymarket.markets_dim m ON t.condition_id = m.condition_id
    WHERE t.ts > now() - INTERVAL {hours} HOUR
    GROUP BY m.computed_category
    ORDER BY total_volume DESC
    """
    
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    
    return df


def analyze_market(client: clickhouse_connect.driver.Client, condition_id: str) -> dict:
    """
    Comprehensive analysis for a single market.
    
    Returns:
        Dictionary with various statistics
    """
    trades_df = get_trades_df(client, condition_id=condition_id, hours=24)
    
    if trades_df.empty:
        return {"error": "No data found for this market"}
    
    stats = {
        "total_trades": len(trades_df),
        "total_volume": trades_df['notional'].sum(),
        "avg_price": trades_df['price'].mean(),
        "min_price": trades_df['price'].min(),
        "max_price": trades_df['price'].max(),
        "price_std": trades_df['price'].std(),
        "avg_trade_size": trades_df['size'].mean(),
        "buy_volume": trades_df[trades_df['side'] == 'BUY']['notional'].sum(),
        "sell_volume": trades_df[trades_df['side'] == 'SELL']['notional'].sum(),
        "first_trade": trades_df['ts'].min(),
        "last_trade": trades_df['ts'].max(),
    }
    
    # Hourly volume
    trades_df['hour'] = trades_df['ts'].dt.floor('H')
    hourly_volume = trades_df.groupby('hour')['notional'].sum()
    stats['hourly_volume'] = hourly_volume.to_dict()
    
    return stats


# Example usage
if __name__ == "__main__":
    print("🔌 Connecting to ClickHouse...")
    client = get_client()
    print("✅ Connected!\n")
    
    # Example 1: Top markets
    print("📊 Top 10 Markets (Last 24h):")
    print("=" * 80)
    top_markets = get_top_markets(client, hours=24, limit=10)
    print(top_markets.to_string(index=False))
    print("\n")
    
    # Example 2: Category stats
    print("📈 Volume by Category (Last 24h):")
    print("=" * 80)
    cat_stats = get_category_stats(client, hours=24)
    print(cat_stats.to_string(index=False))
    print("\n")
    
    # Example 3: Get trades for a specific market (if available)
    if not top_markets.empty:
        sample_market = top_markets.iloc[0]['market_id']
        print(f"🔍 Analyzing market: {sample_market}")
        print("=" * 80)
        
        trades_df = get_trades_df(client, condition_id=sample_market, hours=24)
        print(f"Total trades: {len(trades_df)}")
        print(f"Total volume: ${trades_df['notional'].sum():,.2f}")
        print(f"Price range: ${trades_df['price'].min():.4f} - ${trades_df['price'].max():.4f}")
        print("\nFirst 5 trades:")
        print(trades_df[['ts', 'price', 'size', 'notional', 'side']].head().to_string(index=False))
    
    client.close()
    print("\n✅ Analysis complete!")

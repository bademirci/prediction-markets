"""Configuration settings for Polymarket ingestion."""

from dataclasses import dataclass


@dataclass
class ClickHouseConfig:
    host: str = "localhost"
    port: int = 18123
    database: str = "polymarket"
    user: str = "default"
    password: str = ""


@dataclass
class PolymarketConfig:
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    data_api_url: str = "https://data-api.polymarket.com"
    clob_api_url: str = "https://clob.polymarket.com"
    websocket_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    
    # Polling intervals (seconds)
    market_sync_interval: int = 300  # 5 minutes
    bbo_poll_interval: int = 2       # 2 seconds (fallback if WS fails)


@dataclass
class Config:
    clickhouse: ClickHouseConfig
    polymarket: PolymarketConfig
    
    # Ingestion settings
    batch_size: int = 1000
    flush_interval: float = 1.0  # seconds
    max_markets: int = 1000000   # All markets (no practical limit)
    ws_tokens_per_connection: int = 1000
    ws_subscribe_batch_size: int = 200
    max_ws_connections: int = 10
    
    # Focus filter (None = all, or category name like 'Sports')
    category_filter: str | None = None


def get_config() -> Config:
    """Get default configuration."""
    return Config(
        clickhouse=ClickHouseConfig(),
        polymarket=PolymarketConfig(),
    )

# Prediction Markets - Polymarket Data Ingestion

A real-time data ingestion system for Polymarket prediction markets, storing market data, trades, and order book updates in ClickHouse.

## Features

- 🔄 **Real-time Data Ingestion**: WebSocket-based streaming of trades and order book updates
- 📊 **Market Synchronization**: Periodic sync of active markets from Polymarket Gamma API
- 💾 **ClickHouse Storage**: Efficient time-series data storage with batch writing
- 🎯 **Category Filtering**: Optional filtering by market category (e.g., Esports)
- 📈 **Statistics Tracking**: Real-time stats on trades, BBO updates, and market syncs

## Architecture

- **REST Client** (`polymarket_rest.py`): Fetches market metadata and order books
- **WebSocket Client** (`websocket_client.py`): Real-time trade and book updates
- **ClickHouse Writer** (`clickhouse_writer.py`): Batch writing to ClickHouse
- **Ingestion Orchestrator** (`ingestion.py`): Coordinates all components

## Requirements

- Python 3.12+
- ClickHouse server (running on localhost:18123 by default)
- Polymarket API access

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd prediction-markets
```

2. Create a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure ClickHouse:
   - Ensure ClickHouse is running
   - Update `src/config.py` with your ClickHouse connection details if needed

## Usage

Run the ingestion system:

```bash
python run_ingestion.py
```

The system will:
1. Connect to ClickHouse
2. Sync active markets from Polymarket
3. Subscribe to WebSocket updates for trades and order books
4. Periodically flush data to ClickHouse
5. Report statistics every 10 seconds

## Configuration

Edit `src/config.py` to customize:

- ClickHouse connection settings (host, port, database, credentials)
- Polymarket API endpoints
- Polling intervals
- Batch sizes and flush intervals
- Category filtering (set `category_filter` to filter by category)

## Project Structure

```
prediction-markets/
├── src/
│   ├── config.py              # Configuration settings
│   ├── ingestion.py           # Main ingestion orchestrator
│   ├── polymarket_rest.py     # REST API client
│   ├── websocket_client.py    # WebSocket client
│   └── clickhouse_writer.py   # ClickHouse writer
├── prompts/                   # Prompt files for market categorization
├── requirements.txt           # Python dependencies
└── run_ingestion.py           # Entry point
```

## Data Schema

The system stores data in ClickHouse tables:
- `markets_dim`: Market metadata and dimensions
- `trades`: Trade transactions
- `bbo`: Best bid/offer updates

## License

[Add your license here]

## Contributing

[Add contribution guidelines if needed]

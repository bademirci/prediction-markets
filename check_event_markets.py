#!/usr/bin/env python3
"""List events with multiple markets, and show markets per event."""

import sys
import clickhouse_connect
from src.config import get_config


def list_events(keyword: str | None = None, limit: int = 20) -> None:
    config = get_config()
    client = clickhouse_connect.get_client(
        host=config.clickhouse.host,
        port=config.clickhouse.port,
        database=config.clickhouse.database,
        user=config.clickhouse.user,
        password=config.clickhouse.password,
    )

    base_where = "event_id != ''"
    params: dict[str, object] = {"limit": limit}
    kw_clause = ""
    if keyword:
        params["kw"] = f"%{keyword}%"
        kw_clause = "AND countIf(event_title ILIKE %(kw)s OR question ILIKE %(kw)s) > 0"

    summary_query = f"""
        SELECT
            event_id,
            any(event_title) AS event_title_agg,
            uniqExact(market_id) AS market_count
        FROM markets_dim
        WHERE {base_where}
        GROUP BY event_id
        HAVING market_count >= 2 {kw_clause}
        ORDER BY market_count DESC
        LIMIT %(limit)s
    """

    summary = client.query(summary_query, parameters=params)
    if not summary.result_rows:
        print("❌ Event bulunamadı (event_id boş olabilir veya filtre çok dar).")
        return

    print(f"✅ {len(summary.result_rows)} event bulundu:\n")
    for event_id, event_title, market_count in summary.result_rows:
        print(f"🧩 Event: {event_title} (ID: {event_id}) — {market_count} market")
        detail_query = """
            SELECT
                market_id,
                argMax(question, updated_at) AS question,
                argMax(computed_category, updated_at) AS computed_category,
                argMax(active, updated_at) AS active,
                argMax(closed, updated_at) AS closed
            FROM markets_dim
            WHERE event_id = %(event_id)s
            GROUP BY market_id
            ORDER BY question
        """
        details = client.query(detail_query, parameters={"event_id": event_id})
        for market_id, question, computed_category, active, closed in details.result_rows:
            print(
                f"  - {question} (Market ID: {market_id}, Cat: {computed_category}, "
                f"Active: {active}, Closed: {closed})"
            )
        print()


if __name__ == "__main__":
    keyword = " ".join(sys.argv[1:]).strip() if len(sys.argv) > 1 else None
    list_events(keyword=keyword, limit=20)

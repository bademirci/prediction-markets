-- ============================================
-- POLYMARKET ORDER BOOK DASHBOARD QUERIES
-- ============================================

-- ============================================
-- 1. ORDER BOOK PRICE LEVELS (Lvl1, Lvl2, Lvl3)
-- ============================================

-- Chart Query: Price levels over time
SELECT 
    ts AS time,
    level,
    bid_px AS "Bid Price",
    ask_px AS "Ask Price"
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND $__timeFilter(ts)
ORDER BY ts, level;

-- Table Query: Summary statistics
SELECT 
    CASE 
        WHEN level = 1 THEN 'YES Bid Lvl1'
        WHEN level = 2 THEN 'YES Bid Lvl2'
        WHEN level = 3 THEN 'YES Bid Lvl3'
    END AS name,
    argMax(bid_px, ts) AS last,
    round(avg(bid_px), 4) AS mean,
    max(bid_px) AS max,
    min(bid_px) AS min
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND $__timeFilter(ts)
  AND bid_px IS NOT NULL
GROUP BY level
UNION ALL
SELECT 
    CASE 
        WHEN level = 1 THEN 'YES Ask Lvl1'
        WHEN level = 2 THEN 'YES Ask Lvl2'
        WHEN level = 3 THEN 'YES Ask Lvl3'
    END AS name,
    argMax(ask_px, ts) AS last,
    round(avg(ask_px), 4) AS mean,
    max(ask_px) AS max,
    min(ask_px) AS min
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND $__timeFilter(ts)
  AND ask_px IS NOT NULL
GROUP BY level
ORDER BY name;


-- ============================================
-- 2. ORDER BOOK SIZES (Lvl1, Lvl2, Lvl3)
-- ============================================

-- Chart Query: Size levels over time
SELECT 
    ts AS time,
    level,
    bid_sz AS "Bid Size",
    ask_sz AS "Ask Size"
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND $__timeFilter(ts)
ORDER BY ts, level;

-- Table Query: Summary statistics
SELECT 
    CASE 
        WHEN level = 1 THEN 'YES Bid Size Lvl1'
        WHEN level = 2 THEN 'YES Bid Size Lvl2'
        WHEN level = 3 THEN 'YES Bid Size Lvl3'
    END AS name,
    argMax(bid_sz, ts) AS last,
    round(avg(bid_sz), 2) AS mean,
    round(sum(bid_sz), 0) AS total
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND $__timeFilter(ts)
  AND bid_sz IS NOT NULL
GROUP BY level
UNION ALL
SELECT 
    CASE 
        WHEN level = 1 THEN 'YES Ask Size Lvl1'
        WHEN level = 2 THEN 'YES Ask Size Lvl2'
        WHEN level = 3 THEN 'YES Ask Size Lvl3'
    END AS name,
    argMax(ask_sz, ts) AS last,
    round(avg(ask_sz), 2) AS mean,
    round(sum(ask_sz), 0) AS total
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND $__timeFilter(ts)
  AND ask_sz IS NOT NULL
GROUP BY level
ORDER BY name;


-- ============================================
-- 3. MID PRICE EVOLUTION
-- ============================================

-- Chart Query: Mid price over time
SELECT 
    ts AS time,
    level,
    (bid_px + ask_px) / 2 AS "Mid Price"
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND $__timeFilter(ts)
  AND bid_px IS NOT NULL 
  AND ask_px IS NOT NULL
ORDER BY ts, level;

-- Table Query: Summary statistics
SELECT 
    CASE 
        WHEN level = 1 THEN 'YES Mid Price'
        WHEN level = 2 THEN 'YES Mid Price Lvl2'
        WHEN level = 3 THEN 'YES Mid Price Lvl3'
    END AS name,
    argMax((bid_px + ask_px) / 2, ts) AS last,
    round(avg((bid_px + ask_px) / 2), 4) AS mean,
    round(argMax((bid_px + ask_px) / 2, ts) - min((bid_px + ask_px) / 2), 4) AS delta
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND $__timeFilter(ts)
  AND bid_px IS NOT NULL 
  AND ask_px IS NOT NULL
GROUP BY level
ORDER BY level;


-- ============================================
-- 4. ORDER BOOK IMBALANCE
-- ============================================

-- Chart Query: Imbalance over time
SELECT 
    ts AS time,
    level,
    CASE 
        WHEN (bid_sz + ask_sz) > 0 
        THEN (bid_sz - ask_sz) / (bid_sz + ask_sz)
        ELSE 0
    END AS "Imbalance"
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND $__timeFilter(ts)
  AND bid_sz IS NOT NULL 
  AND ask_sz IS NOT NULL
ORDER BY ts, level;

-- Table Query: Summary statistics
SELECT 
    CASE 
        WHEN level = 1 THEN 'YES Imbalance'
        WHEN level = 2 THEN 'YES Imbalance Lvl2'
        WHEN level = 3 THEN 'YES Imbalance Lvl3'
    END AS name,
    argMax(
        CASE 
            WHEN (bid_sz + ask_sz) > 0 
            THEN (bid_sz - ask_sz) / (bid_sz + ask_sz)
            ELSE 0
        END, 
        ts
    ) AS last,
    round(avg(
        CASE 
            WHEN (bid_sz + ask_sz) > 0 
            THEN (bid_sz - ask_sz) / (bid_sz + ask_sz)
            ELSE 0
        END
    ), 4) AS mean,
    max(
        CASE 
            WHEN (bid_sz + ask_sz) > 0 
            THEN (bid_sz - ask_sz) / (bid_sz + ask_sz)
            ELSE 0
        END
    ) AS max,
    min(
        CASE 
            WHEN (bid_sz + ask_sz) > 0 
            THEN (bid_sz - ask_sz) / (bid_sz + ask_sz)
            ELSE 0
        END
    ) AS min
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND $__timeFilter(ts)
  AND bid_sz IS NOT NULL 
  AND ask_sz IS NOT NULL
GROUP BY level
ORDER BY level;


-- ============================================
-- 5. BID-ASK SPREAD BY LEVEL
-- ============================================

-- Chart Query: Spread over time
SELECT 
    ts AS time,
    level,
    ask_px - bid_px AS "Spread"
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND $__timeFilter(ts)
  AND bid_px IS NOT NULL 
  AND ask_px IS NOT NULL
ORDER BY ts, level;

-- Table Query: Summary statistics
SELECT 
    CASE 
        WHEN level = 1 THEN 'Spread (Lvl1)'
        WHEN level = 2 THEN 'Spread (Lvl2)'
        WHEN level = 3 THEN 'Spread (Lvl3)'
    END AS name,
    argMax(ask_px - bid_px, ts) AS last,
    round(avg(ask_px - bid_px), 4) AS mean,
    max(ask_px - bid_px) AS max
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND $__timeFilter(ts)
  AND bid_px IS NOT NULL 
  AND ask_px IS NOT NULL
GROUP BY level
ORDER BY level;


-- ============================================
-- 6. TOTAL BOOK LIQUIDITY OVER TIME
-- ============================================

-- Chart Query: Total liquidity over time
SELECT 
    ts AS time,
    level,
    bid_sz + ask_sz AS "Total Liquidity"
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND $__timeFilter(ts)
  AND bid_sz IS NOT NULL 
  AND ask_sz IS NOT NULL
ORDER BY ts, level;

-- Table Query: Summary statistics
SELECT 
    CASE 
        WHEN level = 1 THEN 'YES Total Liquidity'
        WHEN level = 2 THEN 'YES Total Liquidity Lvl2'
        WHEN level = 3 THEN 'YES Total Liquidity Lvl3'
    END AS name,
    argMax(bid_sz + ask_sz, ts) AS last,
    round(avg(bid_sz + ask_sz), 0) AS mean,
    min(bid_sz + ask_sz) AS min
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND $__timeFilter(ts)
  AND bid_sz IS NOT NULL 
  AND ask_sz IS NOT NULL
GROUP BY level
ORDER BY level;


-- ============================================
-- 7. YES/NO IMPLIED PROBABILITY
-- ============================================

-- YES Probability (from Lvl1 mid price)
SELECT 
    ts AS time,
    (bid_px + ask_px) / 2 AS "YES Probability"
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND level = 1
  AND $__timeFilter(ts)
  AND bid_px IS NOT NULL 
  AND ask_px IS NOT NULL
ORDER BY ts;

-- NO Probability (1 - YES)
SELECT 
    ts AS time,
    1 - (bid_px + ask_px) / 2 AS "NO Probability"
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND level = 1
  AND $__timeFilter(ts)
  AND bid_px IS NOT NULL 
  AND ask_px IS NOT NULL
ORDER BY ts;

-- Current YES/NO Probabilities (for stat panels)
SELECT 
    'YES Probability' AS name,
    round(argMax((bid_px + ask_px) / 2, ts) * 100, 2) AS value
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND level = 1
  AND $__timeFilter(ts)
  AND bid_px IS NOT NULL 
  AND ask_px IS NOT NULL
UNION ALL
SELECT 
    'NO Probability' AS name,
    round((1 - argMax((bid_px + ask_px) / 2, ts)) * 100, 2) AS value
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND token_id = '$token'
  AND level = 1
  AND $__timeFilter(ts)
  AND bid_px IS NOT NULL 
  AND ask_px IS NOT NULL;


-- ============================================
-- 8. MARKET PRESSURE (from imbalance)
-- ============================================

-- Market Pressure Gauge (from Lvl1 imbalance)
SELECT 
    CASE 
        WHEN avg_imbalance > 0.3 THEN 'BUY'
        WHEN avg_imbalance < -0.3 THEN 'SELL'
        ELSE 'NEUTRAL'
    END AS pressure
FROM (
    SELECT 
        avg(
            CASE 
                WHEN (bid_sz + ask_sz) > 0 
                THEN (bid_sz - ask_sz) / (bid_sz + ask_sz)
                ELSE 0
            END
        ) AS avg_imbalance
    FROM polymarket.orderbook_levels
    WHERE condition_id = '$market'
      AND token_id = '$token'
      AND level = 1
      AND $__timeFilter(ts)
      AND bid_sz IS NOT NULL 
      AND ask_sz IS NOT NULL
);


-- ============================================
-- BONUS: UNIFIED VIEW (YES/NO on same axis)
-- ============================================

-- This view projects both YES and NO tokens onto a single YES-probability axis
-- Assumes: YES token is clob_token_ids[1], NO token is clob_token_ids[0]

SELECT 
    ts AS time,
    CASE 
        WHEN token_id = (SELECT clob_token_ids[1] FROM polymarket.markets_dim WHERE condition_id = '$market' LIMIT 1)
        THEN (bid_px + ask_px) / 2
        WHEN token_id = (SELECT clob_token_ids[0] FROM polymarket.markets_dim WHERE condition_id = '$market' LIMIT 1)
        THEN 1 - (bid_px + ask_px) / 2
        ELSE NULL
    END AS "YES Probability",
    level
FROM polymarket.orderbook_levels
WHERE condition_id = '$market'
  AND level = 1
  AND $__timeFilter(ts)
  AND bid_px IS NOT NULL 
  AND ask_px IS NOT NULL
ORDER BY ts;

-- Test queries for Grafana variables
-- Copy-paste these into Grafana's query editor to test

-- 1. Category Variable Query
SELECT DISTINCT computed_category 
FROM polymarket.markets_dim 
WHERE active = 1
ORDER BY computed_category;

-- 2. Market Variable Query (for Sports category)
SELECT DISTINCT 
    m.condition_id AS __value, 
    m.question AS __text 
FROM polymarket.markets_dim m 
WHERE m.computed_category = 'Sports' 
  AND m.active = 1 
  AND length(m.clob_token_ids) > 0
  AND m.question LIKE '%JD Gaming%'
ORDER BY m.updated_at DESC 
LIMIT 100;

-- 3. Token Variable Query (for a specific market)
SELECT DISTINCT token_id 
FROM polymarket.orderbook_levels 
WHERE condition_id = '0x67ed3a23437c3a159100d8bbc4ea778f5b408b4bfa394529b39886b3ed6517fb'
LIMIT 1;

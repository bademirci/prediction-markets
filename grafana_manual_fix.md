# Grafana Variable Fix - Manuel Adımlar

## Problem
JDG LoL marketleri dropdown'da görünmüyor.

## Çözüm 1: Variable'ları Manuel Düzelt

1. Grafana'da dashboard'u aç
2. Sağ üstteki **⚙️ Settings** (dişli ikonu) → **Variables**
3. **"market"** variable'ını seç → **Edit**

### Market Variable Query'sini Değiştir:

**ESKİ:**
```sql
SELECT DISTINCT t.condition_id AS __value, any(m.question) AS __text 
FROM polymarket.trades_raw t 
JOIN polymarket.markets_dim m ON t.condition_id = m.condition_id 
WHERE m.computed_category = '${category}' 
GROUP BY t.condition_id 
ORDER BY count() DESC 
LIMIT 30
```

**YENİ:**
```sql
SELECT condition_id AS __value, question AS __text 
FROM polymarket.markets_dim 
WHERE computed_category = '${category}' 
  AND active = 1 
  AND length(clob_token_ids) > 0
ORDER BY updated_at DESC 
LIMIT 100
```

4. **Update** butonuna tıkla
5. Dashboard'u yenile (F5)

---

## Çözüm 2: Direkt Market ID ile Test

Eğer yukarıdaki çözüm çalışmazsa, direkt market ID'yi kullan:

1. Dashboard'da herhangi bir panel'e tıkla
2. **Edit** → **Query** sekmesine git
3. Query'de `'${market}'` yerine direkt market ID kullan:
   ```
   '0x67ed3a23437c3a159100d8bbc4ea778f5b408b4bfa394529b39886b3ed6517fb'
   ```

---

## Test Query'leri

Grafana'da **Explore** → **ClickHouse** → Bu query'leri test et:

```sql
-- 1. Sports kategorisindeki tüm marketler
SELECT condition_id, question 
FROM polymarket.markets_dim 
WHERE computed_category = 'Sports' 
  AND active = 1
  AND question LIKE '%JD Gaming%'
ORDER BY updated_at DESC
LIMIT 10
```

```sql
-- 2. JDG vs IG marketleri
SELECT condition_id, question, computed_category
FROM polymarket.markets_dim 
WHERE question LIKE '%JD Gaming vs Invictus Gaming%'
  AND active = 1
```

-- =============================================================================
-- queries.sql
-- Purpose: Advanced SQL analytical queries for FinSight platform
-- Part of: FinSight Analytics Platform — Phase 3 (Advanced SQL)
-- Author: Mithila Papi Shetty
-- =============================================================================


-- =============================================================================
-- QUERY 1: 30-Day Moving Average
-- Purpose: Calculate 30-day rolling average price for any stock
-- Usage: Change 'AAPL' to any ticker
-- =============================================================================
SELECT 
    s.ticker,
    dp.trade_date,
    dp.close_price,
    ROUND(AVG(dp.close_price) OVER (
        PARTITION BY dp.stock_id
        ORDER BY dp.trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )::numeric, 2) AS ma_30
FROM daily_prices dp
JOIN stocks s ON dp.stock_id = s.stock_id
WHERE s.ticker = 'AAPL'
ORDER BY dp.trade_date DESC
LIMIT 10;


-- =============================================================================
-- QUERY 2: Top 10 Best Performing Stocks (1 Year Return)
-- Purpose: Rank stocks by percentage price change over the year
-- =============================================================================
SELECT 
    s.ticker,
    s.company_name,
    ROUND(MIN(dp.close_price)::numeric, 2) AS price_1yr_ago,
    ROUND(MAX(dp.close_price)::numeric, 2) AS recent_price,
    ROUND(((MAX(dp.close_price) - MIN(dp.close_price)) 
        / MIN(dp.close_price) * 100)::numeric, 2) AS pct_change
FROM daily_prices dp
JOIN stocks s ON dp.stock_id = s.stock_id
GROUP BY s.ticker, s.company_name
ORDER BY pct_change DESC
LIMIT 10;


-- =============================================================================
-- QUERY 3: Daily Price Change with RANK and LAG
-- Purpose: Show each stock's price change from previous day, ranked by price
-- Usage: Change date to any trading day
-- =============================================================================
SELECT *
FROM (
    SELECT
        s.ticker,
        dp.trade_date,
        dp.close_price,
        ROUND(LAG(dp.close_price) OVER (
            PARTITION BY dp.stock_id
            ORDER BY dp.trade_date
        )::numeric, 2) AS prev_day_price,
        ROUND((dp.close_price - LAG(dp.close_price) OVER (
            PARTITION BY dp.stock_id
            ORDER BY dp.trade_date
        ))::numeric, 2) AS day_change,
        RANK() OVER (
            PARTITION BY dp.trade_date
            ORDER BY dp.close_price DESC
        ) AS price_rank
    FROM daily_prices dp
    JOIN stocks s ON dp.stock_id = s.stock_id
) ranked
WHERE trade_date = '2026-04-21'
ORDER BY price_rank;


-- =============================================================================
-- QUERY 4: Sector Performance Analysis
-- Purpose: Compare average price, volume and range across all sectors
-- =============================================================================
SELECT
    sec.sector_name,
    ROUND(AVG(dp.close_price)::numeric, 2) AS avg_price,
    ROUND(MAX(dp.close_price)::numeric, 2) AS highest_price,
    ROUND(MIN(dp.close_price)::numeric, 2) AS lowest_price,
    ROUND(AVG(dp.volume)::numeric, 0)      AS avg_daily_volume,
    COUNT(DISTINCT s.stock_id)             AS num_stocks
FROM daily_prices dp
JOIN stocks s ON dp.stock_id = s.stock_id
JOIN sectors sec ON s.sector_id = sec.sector_id
GROUP BY sec.sector_name
ORDER BY avg_price DESC;


-- =============================================================================
-- QUERY 5: Anomaly Detection — Volume Spikes
-- Purpose: Find days where stocks traded unusually high volume
--          volume_ratio > 3 = significant anomaly worth investigating
-- =============================================================================
SELECT
    s.ticker,
    dp.trade_date,
    dp.volume,
    ROUND(AVG(dp.volume) OVER (
        PARTITION BY dp.stock_id
        ORDER BY dp.trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    )::numeric, 0) AS avg_30day_volume,
    ROUND((dp.volume / AVG(dp.volume) OVER (
        PARTITION BY dp.stock_id
        ORDER BY dp.trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ))::numeric, 2) AS volume_ratio
FROM daily_prices dp
JOIN stocks s ON dp.stock_id = s.stock_id
ORDER BY volume_ratio DESC
LIMIT 15;

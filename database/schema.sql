-- FinSight Database Schema
-- Phase 1: Table Definitions

CREATE TABLE sectors (
    sector_id   SERIAL PRIMARY KEY,
    sector_name VARCHAR(100) NOT NULL UNIQUE,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stocks (
    stock_id     SERIAL PRIMARY KEY,
    ticker       VARCHAR(10) NOT NULL UNIQUE,
    company_name VARCHAR(200) NOT NULL,
    sector_id    INTEGER REFERENCES sectors(sector_id),
    country      VARCHAR(50) DEFAULT 'USA',
    is_active    BOOLEAN DEFAULT TRUE,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE daily_prices (
    price_id    SERIAL PRIMARY KEY,
    stock_id    INTEGER REFERENCES stocks(stock_id),
    trade_date  DATE NOT NULL,
    open_price  NUMERIC(10,2),
    high_price  NUMERIC(10,2),
    low_price   NUMERIC(10,2),
    close_price NUMERIC(10,2),
    volume      BIGINT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, trade_date)
);

CREATE TABLE financials (
    financial_id   SERIAL PRIMARY KEY,
    stock_id       INTEGER REFERENCES stocks(stock_id),
    fiscal_quarter VARCHAR(10) NOT NULL,
    fiscal_year    INTEGER NOT NULL,
    revenue        NUMERIC(20,2),
    net_income     NUMERIC(20,2),
    eps            NUMERIC(10,4),
    pe_ratio       NUMERIC(10,4),
    market_cap     NUMERIC(20,2),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, fiscal_quarter, fiscal_year)
);

-- Indexes
CREATE INDEX idx_daily_prices_stock_id   ON daily_prices(stock_id);
CREATE INDEX idx_daily_prices_trade_date ON daily_prices(trade_date);
CREATE INDEX idx_daily_prices_stock_date ON daily_prices(stock_id, trade_date);
CREATE INDEX idx_stocks_ticker           ON stocks(ticker);
CREATE INDEX idx_financials_stock_id     ON financials(stock_id);

# =============================================================================
# loader.py
# Purpose: Load fetched stock data into PostgreSQL database
# Part of: FinSight Analytics Platform — Phase 2 (Python ETL Pipeline)
# Author: Mithila Papi Shetty
# =============================================================================

# SQLAlchemy — connects Python to PostgreSQL
from sqlalchemy import create_engine, text

# pandas — for handling data tables
import pandas as pd

# os — to read environment variables from .env file
import os

# load_dotenv — reads .env file
from dotenv import load_dotenv

# Import our fetcher function we already wrote
from finsight.ingestion.fetcher import fetch_stock_data

# Load credentials from .env file
load_dotenv()


def get_db_engine():
    """
    Create a connection to PostgreSQL database.
    Reads credentials from .env file — never hardcoded.
    
    Returns:
        SQLAlchemy engine object (our database connection)
    """
    
    # Build the connection string using .env variables
    # Format: postgresql://username:password@host:port/database
    connection_string = (
        f"postgresql://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
        f"/{os.getenv('DB_NAME')}"
    )
    
    # Create and return the engine
    return create_engine(connection_string)


def get_stock_id(engine, ticker: str) -> int:
    """
    Look up the stock_id for a given ticker symbol.
    
    Args:
        engine: database connection
        ticker: stock symbol e.g. 'AAPL'
    
    Returns:
        stock_id integer from stocks table
    """
    
    with engine.connect() as conn:
        # Query the stocks table to find the stock_id for this ticker
        result = conn.execute(
            text("SELECT stock_id FROM stocks WHERE ticker = :ticker"),
            {"ticker": ticker}
        )
        row = result.fetchone()
        
        # If ticker not found in database, raise an error
        if row is None:
            raise ValueError(f"Ticker {ticker} not found in stocks table")
        
        return row[0]


def load_prices_to_db(df: pd.DataFrame, engine) -> int:
    """
    Load a DataFrame of stock prices into daily_prices table.
    Skips rows that already exist (no duplicates).
    
    Args:
        df: DataFrame from fetch_stock_data()
        engine: database connection
    
    Returns:
        Number of rows successfully inserted
    """
    
    # Get the stock_id for this ticker
    ticker = df['ticker'].iloc[0]
    stock_id = get_stock_id(engine, ticker)
    
    # Add stock_id column to dataframe
    df['stock_id'] = stock_id
    
    # Clean the date column — remove timezone info
    # PostgreSQL DATE column doesn't need timezone
    df['date'] = pd.to_datetime(df['date']).dt.date
    
    # Rename columns to match database column names
    df = df.rename(columns={
        'date':   'trade_date',
        'open':   'open_price',
        'high':   'high_price',
        'low':    'low_price',
        'close':  'close_price'
    })
    
    # Keep only columns that exist in daily_prices table
    df = df[['stock_id', 'trade_date', 'open_price',
             'high_price', 'low_price', 'close_price', 'volume']]
    
    # Insert rows — ON CONFLICT DO NOTHING skips duplicates
    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO daily_prices 
                    (stock_id, trade_date, open_price, high_price, 
                     low_price, close_price, volume)
                VALUES 
                    (:stock_id, :trade_date, :open_price, :high_price, 
                     :low_price, :close_price, :volume)
                ON CONFLICT (stock_id, trade_date) DO NOTHING
            """), row.to_dict())
        conn.commit()
    
    rows_inserted = len(df)
    return rows_inserted


def run_pipeline(tickers: list, period: str = "1y"):
    """
    Main pipeline function — fetches and loads data for multiple tickers.
    
    Args:
        tickers: list of stock symbols e.g. ['AAPL', 'GOOGL', 'MSFT']
        period:  how far back to fetch data
    """
    
    print("=" * 50)
    print("FinSight ETL Pipeline Starting...")
    print("=" * 50)
    
    # Create database connection
    engine = get_db_engine()
    
    # Track results
    success = []
    failed  = []
    
    # Loop through each ticker
    for ticker in tickers:
        try:
            # Step 1: Fetch data from Yahoo Finance
            df = fetch_stock_data(ticker, period=period)
            
            # Step 2: Load into PostgreSQL
            rows = load_prices_to_db(df, engine)
            
            print(f"✅ {ticker}: {rows} rows saved to database")
            success.append(ticker)
            
        except Exception as e:
            # If anything fails, log it and continue with next ticker
            print(f"❌ {ticker}: Failed — {e}")
            failed.append(ticker)
    
    # Summary
    print("\n" + "=" * 50)
    print(f"Pipeline Complete!")
    print(f"✅ Success: {len(success)} stocks")
    print(f"❌ Failed:  {len(failed)} stocks")
    if failed:
        print(f"Failed tickers: {failed}")
    print("=" * 50)


# Run the pipeline when this file is executed directly
if __name__ == "__main__":
    
    # All 50 stocks across 5 sectors
    test_tickers = [
        # Technology
        'AAPL', 'GOOGL', 'MSFT', 'NVDA', 'META',
        'INTC', 'AMD', 'CRM', 'ORCL', 'ADBE',
        # Finance
        'JPM', 'BAC', 'GS', 'MS', 'WFC',
        'C', 'BLK', 'AXP', 'SCHW', 'USB',
        # Healthcare
        'JNJ', 'PFE', 'UNH', 'ABBV', 'MRK',
        'TMO', 'ABT', 'BMY', 'AMGN', 'GILD',
        # Energy
        'XOM', 'CVX', 'COP', 'SLB', 'EOG',
        'PSX', 'VLO', 'MPC', 'HAL', 'DVN',
        # Retail
        'AMZN', 'WMT', 'TGT', 'COST', 'HD',
        'LOW', 'NKE', 'SBUX', 'MCD', 'TJX'
    ]
    
    # Run pipeline
    run_pipeline(test_tickers, period="1y")
# =============================================================================
# fetcher.py
# Purpose: Fetch historical stock price data from Yahoo Finance API
# Part of: FinSight Analytics Platform — Phase 2 (Python ETL Pipeline)
# Author: Mithila Papi Shetty
# =============================================================================

# yfinance — library that connects to Yahoo Finance and pulls stock data
import yfinance as yf

# pandas — library for handling data in table format (like Excel in Python)
import pandas as pd

# load_dotenv — reads our .env file so we can use DB credentials securely
from dotenv import load_dotenv

# os — lets Python interact with the operating system (read env variables)
import os

# Load environment variables from .env file
# This must run before anything tries to read DB credentials
load_dotenv()


def fetch_stock_data(ticker: str, period: str = "1y") -> pd.DataFrame:
    """
    Fetch historical stock price data for a single ticker.
    
    Args:
        ticker: Stock symbol e.g. 'AAPL', 'GOOGL', 'MSFT'
        period: How far back to fetch data
                Options: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y
                Default: 1y (one full year of daily prices)
    
    Returns:
        pandas DataFrame with columns:
        date, ticker, open, high, low, close, volume
    """
    
    # Tell the user which stock we're currently fetching
    print(f"Fetching data for {ticker}...")
    
    # Create a Ticker object — this is our connection to Yahoo Finance
    # Think of it like opening a stock's page on finance.yahoo.com
    stock = yf.Ticker(ticker)
    
    # Download the historical price data
    # .history() returns a DataFrame with dates as the index
    # period="1y" means fetch the last 1 year of daily prices
    df = stock.history(period=period)
    
    # Move the date from the index into a regular column
    # Before: date is the row label
    # After:  date is a normal column we can work with
    df = df.reset_index()
    
    # Convert all column names to lowercase for consistency
    # Yahoo Finance returns: 'Open', 'High', 'Low', 'Close', 'Volume'
    # We want:               'open', 'high', 'low', 'close', 'volume'
    df.columns = df.columns.str.lower()
    
    # Add the ticker symbol as a column
    # This is important when we fetch multiple stocks — we need to know
    # which row belongs to which company
    df['ticker'] = ticker
    
    # Keep only the columns we need for our database
    # We don't need dividends, stock splits etc. from Yahoo Finance
    df = df[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']]
    
    # Confirm how many rows we got
    print(f"✅ {ticker}: {len(df)} rows fetched")
    
    # Return the cleaned DataFrame
    return df


# This block only runs when you execute this file directly
# It won't run when this file is imported by another file
# Used for testing the function works correctly
if __name__ == "__main__":
    
    # Test with Apple stock
    df = fetch_stock_data("AAPL")
    
    # Show first 5 rows of dataq
    print("\n--- First 5 rows ---")
    print(df.head())
    
    # Show shape — (rows, columns)
    print(f"\nShape: {df.shape}")
    
    # Show date range of data fetched
    print(f"\nDate range: {df['date'].min()} to {df['date'].max()}")
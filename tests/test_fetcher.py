# =============================================================================
# test_fetcher.py
# Purpose: Unit tests for the fetcher module
# Part of: FinSight Analytics Platform — Phase 4 (CI/CD)
# Author: Mithila Papi Shetty
# =============================================================================

# pytest — Python testing library
import pytest

# pandas — for checking DataFrame output
import pandas as pd

# import our function to test
from finsight.ingestion.fetcher import fetch_stock_data


def test_fetch_returns_dataframe():
    """Test that fetch_stock_data returns a pandas DataFrame"""
    df = fetch_stock_data("AAPL", period="5d")
    assert isinstance(df, pd.DataFrame)


def test_fetch_has_correct_columns():
    """Test that DataFrame has all required columns"""
    df = fetch_stock_data("AAPL", period="5d")
    required_columns = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']
    for col in required_columns:
        assert col in df.columns, f"Missing column: {col}"


def test_fetch_returns_data():
    """Test that DataFrame is not empty"""
    df = fetch_stock_data("AAPL", period="5d")
    assert len(df) > 0, "DataFrame should not be empty"


def test_fetch_ticker_column_correct():
    """Test that ticker column contains correct value"""
    df = fetch_stock_data("AAPL", period="5d")
    assert df['ticker'].iloc[0] == "AAPL"


def test_fetch_no_negative_prices():
    """Test that prices are never negative"""
    df = fetch_stock_data("AAPL", period="5d")
    assert (df['close'] > 0).all(), "Close prices should always be positive"
    
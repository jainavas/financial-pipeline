import logging
from sqlalchemy import create_engine, text
import pandas as pd
from config import DatabaseConfig

logger = logging.getLogger(__name__)


class LoadError(Exception):
    pass


def get_engine():
    """Create database engine with connection pooling."""
    connection_string = DatabaseConfig.get_connection_string()
    
    try:
        engine = create_engine(
            connection_string,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10
        )
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database engine created successfully")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise LoadError(f"Failed to create database engine: {e}")


def create_prices_table(engine):
    """Create the stock prices table if it doesn't exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        id SERIAL PRIMARY KEY,
        ticker VARCHAR(10) NOT NULL,
        date DATE NOT NULL,
        open NUMERIC(12, 4),
        high NUMERIC(12, 4),
        low NUMERIC(12, 4),
        close NUMERIC(12, 4),
        adj_close NUMERIC(12, 4),
        volume BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(ticker, date)
    );
    
    CREATE INDEX IF NOT EXISTS idx_ticker_date ON stock_prices(ticker, date);
    CREATE INDEX IF NOT EXISTS idx_ticker ON stock_prices(ticker);
    CREATE INDEX IF NOT EXISTS idx_date ON stock_prices(date);
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(text(create_table_sql))
        logger.info("Table 'stock_prices' verified/created successfully")
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        raise LoadError(f"Failed to create table: {e}")


def load_prices_upsert(df: pd.DataFrame, engine) -> int:
    """Load with UPSERT - inserts new records, updates existing ones.
    
    Args:
        df: Transformed DataFrame with required columns
        engine: SQLAlchemy engine
    
    Returns:
        Number of rows processed
    
    Raises:
        LoadError: If loading fails
    """
    if df.empty:
        logger.warning("Received empty DataFrame, skipping load")
        return 0
    
    required_cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
    if not all(col in df.columns for col in required_cols):
        missing = set(required_cols) - set(df.columns)
        raise LoadError(f"Missing required columns: {missing}")
    
    upsert_sql = """
    INSERT INTO stock_prices (ticker, date, open, high, low, close, adj_close, volume)
    VALUES (:ticker, :date, :open, :high, :low, :close, :adj_close, :volume)
    ON CONFLICT (ticker, date) 
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        adj_close = EXCLUDED.adj_close,
        volume = EXCLUDED.volume,
        updated_at = CURRENT_TIMESTAMP;
    """
    
    try:
        records = df[required_cols].to_dict(orient='records')
        
        with engine.begin() as conn:
            # Batch execution - much faster than row-by-row
            conn.execute(text(upsert_sql), records)
        
        logger.info(f"Successfully upserted {len(df)} rows into stock_prices")
        return len(df)
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        raise LoadError(f"Failed to load data: {e}")


def get_latest_date(ticker: str, engine) -> str | None:
    """Get the most recent date for a ticker in the database.
    
    Useful for incremental loads.
    """
    query = """
    SELECT MAX(date) as latest_date 
    FROM stock_prices 
    WHERE ticker = :ticker
    """
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), {"ticker": ticker})
            row = result.fetchone()
            if row and row[0]:
                return row[0].strftime("%Y-%m-%d")
            return None
    except Exception as e:
        logger.error(f"Failed to get latest date for {ticker}: {e}")
        return None


def get_record_count(ticker: str, engine) -> int:
    """Get total number of records for a ticker."""
    query = "SELECT COUNT(*) FROM stock_prices WHERE ticker = :ticker"
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), {"ticker": ticker})
            return result.scalar()
    except Exception as e:
        logger.error(f"Failed to get record count for {ticker}: {e}")
        return 0
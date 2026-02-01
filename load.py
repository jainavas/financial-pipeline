import os
from sqlalchemy import create_engine, text
import pandas as pd


class LoadError(Exception):
    pass


def get_engine():
    """Create database engine with connection pooling."""
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "financial_data")
    db_user = os.getenv("DB_USER", "admin")
    db_pass = os.getenv("DB_PASSWORD", "admin123")
    
    connection_string = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    
    try:
        engine = create_engine(connection_string, pool_pre_ping=True)
        return engine
    except Exception as e:
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
        UNIQUE(ticker, date)
    );
    
    CREATE INDEX IF NOT EXISTS idx_ticker_date ON stock_prices(ticker, date);
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
    except Exception as e:
        raise LoadError(f"Failed to create table: {e}")


def load_prices_upsert(df: pd.DataFrame, engine):
    """Load with UPSERT - inserts new records, updates existing ones.
    
    Args:
        df: Transformed DataFrame with columns: ticker, date, open, high, low, close, adj_close, volume
        engine: SQLAlchemy engine
    """
    if df.empty:
        raise LoadError("Cannot load empty DataFrame")
    
    required_cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
    if not all(col in df.columns for col in required_cols):
        raise LoadError(f"Missing required columns. Expected: {required_cols}, got: {df.columns.tolist()}")
    
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
        volume = EXCLUDED.volume;
    """
    
    try:
        records = df[required_cols].to_dict(orient='records')
        
        with engine.begin() as conn:
            conn.execute(text(upsert_sql), records)
        
        print(f"Successfully upserted {len(df)} rows into stock_prices table")
    except Exception as e:
        raise LoadError(f"Failed to load data: {e}")


def load_prices(df: pd.DataFrame, engine, if_exists: str = "append"):
    """Load transformed prices DataFrame into PostgreSQL.
    
    Args:
        df: Transformed DataFrame with columns: ticker, date, open, high, low, close, adj_close, volume
        engine: SQLAlchemy engine
        if_exists: 'append', 'replace', or 'fail'
    """
    if df.empty:
        raise LoadError("Cannot load empty DataFrame")
    
    required_cols = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
    if not all(col in df.columns for col in required_cols):
        raise LoadError(f"Missing required columns. Expected: {required_cols}, got: {df.columns.tolist()}")
    
    try:
        df[required_cols].to_sql(
            name="stock_prices",
            con=engine,
            if_exists=if_exists,
            index=False,
            method="multi"
        )
        print(f"Successfully loaded {len(df)} rows into stock_prices table")
    except Exception as e:
        raise LoadError(f"Failed to load data: {e}")


if __name__ == "__main__":
    from extract import extract_ticker_history
    from transform import transform_prices
    
    # Extract and transform data
    print("Extracting data...")
    raw_data = extract_ticker_history("AMZN")
    
    print("Transforming data...")
    clean_data = transform_prices(raw_data)
    
    # Load into database
    print("Loading data...")
    try:
        engine = get_engine()
        create_prices_table(engine)
        load_prices_upsert(clean_data, engine)
    except Exception as e:
        raise LoadError(f"Failed to get engine: {e}")
    finally:
        engine.dispose()
    
    # Verify
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM stock_prices WHERE ticker = 'AMZN'"))
        count = result.scalar()
        print(f"Total AMZN records in database: {count}")
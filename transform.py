import logging
import pandas as pd

logger = logging.getLogger(__name__)


class TransformationError(Exception):
    pass


REQUIRED_COLUMNS = {
    "Date", "Open", "High", "Low",
    "Close", "Adj Close", "Volume", "ticker"
}


def transform_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw price data into clean format.
    
    Args:
        df: Raw DataFrame from extraction
    
    Returns:
        Cleaned and validated DataFrame
    
    Raises:
        TransformationError: If transformation fails
    """
    logger.info(f"Starting transformation of {len(df)} rows")
    
    data = df.copy()
    
    # Flatten MultiIndex columns if present
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [col[0] if col[1] == '' else col[0] for col in data.columns]
    
    # Validate required columns
    if not REQUIRED_COLUMNS.issubset(data.columns):
        missing = REQUIRED_COLUMNS - set(data.columns)
        raise TransformationError(f"Missing columns: {missing}")

    # Rename columns to lowercase
    data = data.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume"
    })

    # Parse date
    data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.date

    # Numeric coercion
    numeric_cols = ["open", "high", "low", "close", "adj_close", "volume"]
    for col in numeric_cols:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    # Track data quality
    initial_count = len(data)
    data = data.dropna(subset=["date", "ticker", "volume"])
    dropped_count = initial_count - len(data)
    
    if dropped_count > 0:
        logger.warning(f"Dropped {dropped_count} rows due to missing critical values")

    # Sort for consistency
    data = data.sort_values(by=["ticker", "date"])

    if data.empty:
        raise TransformationError("All rows were dropped during transformation")

    logger.info(f"Transformation complete: {len(data)} clean rows")
    return data.reset_index(drop=True)
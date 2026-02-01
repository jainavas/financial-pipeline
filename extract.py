import logging
import time
import yfinance as yf
import pandas as pd
from config import ExtractionConfig

logger = logging.getLogger(__name__)


class ExtractionError(Exception):
    pass


def extract_ticker_history(
    ticker: str,
    start: str | None = None,
    end: str | None = None,
    max_retries: int = ExtractionConfig.MAX_RETRIES
) -> pd.DataFrame:
    """
    Extract historical price data for a ticker with retry logic.
    
    Args:
        ticker: Stock ticker symbol
        start: Start date (YYYY-MM-DD format)
        end: End date (YYYY-MM-DD format)
        max_retries: Maximum number of retry attempts
    
    Returns:
        DataFrame with historical price data
    
    Raises:
        ExtractionError: If extraction fails after all retries
    """
    logger.info(f"Extracting data for {ticker} (start={start}, end={end})")
    
    for attempt in range(max_retries):
        try:
            df = yf.download(
                ticker,
                start=start,
                end=end,
                progress=False,
                auto_adjust=False,
                threads=False
            )
            
            if df.empty:
                # Check if this is expected (e.g., no new data in incremental mode)
                if start:
                    logger.warning(f"No new data available for {ticker} from {start}")
                raise ExtractionError(f"No data returned for {ticker} (start={start}, end={end})")
            
            df = df.reset_index()
            df["ticker"] = ticker
            
            logger.info(f"Successfully extracted {len(df)} rows for {ticker}")
            return df
            
        except Exception as e:
            logger.warning(f"Extraction attempt {attempt + 1}/{max_retries} failed: {e}")
            
            if attempt < max_retries - 1:
                wait_time = ExtractionConfig.RETRY_DELAY * (attempt + 1)
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"All extraction attempts failed for {ticker}")
                raise ExtractionError(f"Data download failed after {max_retries} attempts: {e}")
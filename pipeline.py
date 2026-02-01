"""
Main ETL pipeline orchestrator.
Coordinates extract, transform, and load operations with proper error handling.
"""
import logging
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any

from config import LogConfig, PipelineConfig
from extract import extract_ticker_history, ExtractionError
from transform import transform_prices, TransformationError
from load import (
    get_engine,
    create_prices_table,
    load_prices_upsert,
    get_latest_date,
    get_record_count,
    LoadError
)

# Configure logging
logging.basicConfig(
    level=LogConfig.LEVEL,
    format=LogConfig.FORMAT,
    datefmt=LogConfig.DATE_FORMAT,
    handlers=[
        logging.FileHandler(LogConfig.FILE_NAME),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class PipelineError(Exception):
    """Base exception for pipeline errors."""
    pass


class Pipeline:
    """ETL Pipeline for stock price data."""
    
    def __init__(self):
        self.engine = None
        self.results = []
    
    def __enter__(self):
        """Context manager entry - initialize database connection."""
        try:
            self.engine = get_engine()
            create_prices_table(self.engine)
            logger.info("Pipeline initialized successfully")
            return self
        except Exception as e:
            logger.error(f"Failed to initialize pipeline: {e}")
            raise PipelineError(f"Initialization failed: {e}")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        if self.engine:
            self.engine.dispose()
            logger.info("Pipeline resources cleaned up")
    
    def run_for_ticker(
        self,
        ticker: str,
        start: str | None = None,
        end: str | None = None,
        incremental: bool = False
    ) -> Dict[str, Any]:
        """
        Run complete ETL pipeline for a single ticker.
        
        Args:
            ticker: Stock ticker symbol
            start: Start date (YYYY-MM-DD)
            end: End date (YYYY-MM-DD)
            incremental: If True, only fetch data after latest date in DB
        
        Returns:
            Dictionary with pipeline execution results
        """
        result = {
            "ticker": ticker,
            "status": "pending",
            "rows_processed": 0,
            "error": None,
            "start_time": datetime.now(),
            "end_time": None
        }
        
        logger.info(f"{'='*60}")
        logger.info(f"Starting pipeline for {ticker}")
        logger.info(f"{'='*60}")
        
        try:
            # Incremental load: start from day after last known date
            if incremental:
                latest_date = get_latest_date(ticker, self.engine)
                if latest_date:
                    latest_dt = datetime.strptime(latest_date, "%Y-%m-%d")
                    start = (latest_dt + timedelta(days=1)).strftime("%Y-%m-%d")
                    logger.info(f"Incremental mode: fetching data from {start} onwards (last DB date: {latest_date})")
            
            # EXTRACT
            logger.info("Phase 1/3: EXTRACT")
            raw_data = extract_ticker_history(ticker, start, end)
            
            # TRANSFORM
            logger.info("Phase 2/3: TRANSFORM")
            clean_data = transform_prices(raw_data)
            
            # LOAD
            logger.info("Phase 3/3: LOAD")
            rows_loaded = load_prices_upsert(clean_data, self.engine)
            
            # Verify
            total_records = get_record_count(ticker, self.engine)
            
            result["status"] = "success"
            result["rows_processed"] = rows_loaded
            result["total_records_in_db"] = total_records
            
            logger.info(f"✓ Pipeline completed successfully for {ticker}")
            logger.info(f"  - Rows processed: {rows_loaded}")
            logger.info(f"  - Total records in DB: {total_records}")
            
        except ExtractionError as e:
            # Check if it's a "no data" error in incremental mode
            if incremental and "No data returned" in str(e):
                result["status"] = "success"
                result["rows_processed"] = 0
                result["message"] = "No new data available (already up to date)"
                logger.info(f"✓ {ticker} is already up to date - no new data to fetch")
            else:
                result["status"] = "failed"
                result["error"] = f"Extraction error: {str(e)}"
                logger.error(f"✗ Extraction failed for {ticker}: {e}")
        
        except TransformationError as e:
            result["status"] = "failed"
            result["error"] = f"Transformation error: {str(e)}"
            logger.error(f"✗ Transformation failed for {ticker}: {e}")
        
        except LoadError as e:
            result["status"] = "failed"
            result["error"] = f"Load error: {str(e)}"
            logger.error(f"✗ Load failed for {ticker}: {e}")
        
        except Exception as e:
            result["status"] = "failed"
            result["error"] = f"Unexpected error: {str(e)}"
            logger.error(f"✗ Unexpected error for {ticker}: {e}", exc_info=True)
        
        finally:
            result["end_time"] = datetime.now()
            result["duration_seconds"] = (result["end_time"] - result["start_time"]).total_seconds()
            self.results.append(result)
        
        return result
    
    def run_for_multiple_tickers(
        self,
        tickers: List[str],
        start: str | None = None,
        end: str | None = None,
        incremental: bool = False,
        stop_on_error: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Run pipeline for multiple tickers.
        
        Args:
            tickers: List of ticker symbols
            start: Start date
            end: End date
            incremental: Use incremental loading
            stop_on_error: If True, stop on first error
        
        Returns:
            List of results for each ticker
        """
        logger.info(f"Running pipeline for {len(tickers)} tickers")
        results = []
        
        for ticker in tickers:
            result = self.run_for_ticker(ticker, start, end, incremental)
            results.append(result)
            
            if stop_on_error and result["status"] == "failed":
                logger.error(f"Stopping pipeline due to error in {ticker}")
                break
        
        # Summary
        successful = sum(1 for r in results if r["status"] == "success")
        failed = len(results) - successful
        total_rows = sum(r.get("rows_processed", 0) for r in results)
        
        logger.info(f"\n{'='*60}")
        logger.info("PIPELINE SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total tickers: {len(results)}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Total rows processed: {total_rows}")
        logger.info(f"{'='*60}\n")
        
        return results


def main():
    """Example usage of the pipeline."""
    
    # Single ticker - fetch last year of data
    with Pipeline() as pipeline:
        result = pipeline.run_for_ticker("AAPL", start="2024-01-01", end="2025-12-31", incremental=False)
        print(f"\nResult: {result}")
    
    # Multiple tickers
    # with Pipeline() as pipeline:
    #     tickers = ["AAPL", "GOOGL", "MSFT", "TSLA"]
    #     results = pipeline.run_for_multiple_tickers(
    #         tickers,
    #         start="2024-01-01",
    #         end="2025-12-31",
    #         incremental=False,
    #         stop_on_error=False
    #     )


if __name__ == "__main__":
    main()
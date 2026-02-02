# dags/stock_prices_backfill_dag.py
"""
One-time backfill DAG for historical data.
Manually trigger to load historical data for new tickers.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from dags.config.tickers import DEFAULT_TICKERS

import sys
sys.path.insert(0, '/opt/airflow')

from pipeline import Pipeline

default_args = {
    'owner': 'data-engineering',
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}


def backfill_ticker(ticker: str, start_date: str, **context):
    """
    Backfill historical data for a ticker.
    
    Args:
        ticker: Stock ticker
        start_date: Start date for backfill (YYYY-MM-DD)
    """
    print(f"Backfilling {ticker} from {start_date}")
    
    with Pipeline() as pipeline:
        result = pipeline.run_for_ticker(
            ticker=ticker,
            start=start_date,
            incremental=False,
        )
    
    if result['status'] == 'failed':
        raise Exception(f"Backfill failed: {result['error']}")
    
    print(f"Backfill complete: {result['rows_processed']} rows")
    return result


with DAG(
    dag_id='stock_prices_backfill',
    default_args=default_args,
    description='Backfill historical stock prices',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['finance', 'backfill', 'maintenance'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Example: Backfill AAPL from 2020
    backfill_aapl = PythonOperator(
        task_id='backfill_AAPL',
        python_callable=backfill_ticker,
        op_kwargs={
            'ticker': 'AAPL',
            'start_date': '2020-01-01',
        },
        provide_context=True,
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> backfill_aapl >> end
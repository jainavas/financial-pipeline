"""
Airflow DAG for daily stock price ingestion.

This DAG:
1. Fetches stock prices for configured tickers
2. Processes each ticker in parallel
3. Loads data into PostgreSQL with UPSERT logic
4. Sends notifications on failures

Schedule: Daily at 6 PM EST (after market close)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

import sys
sys.path.insert(0, '/opt/airflow')

from pipeline import Pipeline
from extract import ExtractionError
from transform import TransformationError
from load import LoadError
from dags.config.tickers import DEFAULT_TICKERS

# DAG default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,  # Set to True and add email in production
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}


def process_ticker(ticker: str, **context):
    """
    Process a single ticker through the ETL pipeline.
    
    Args:
        ticker: Stock ticker symbol
        context: Airflow context with execution_date, etc.
    """
    execution_date = context['execution_date']
    
    print(f"Processing {ticker} for execution_date: {execution_date}")
    
    with Pipeline() as pipeline:
        result = pipeline.run_for_ticker(
            ticker=ticker,
            incremental=True,  # Only fetch new data
        )
    
    # Push result to XCom for monitoring
    context['task_instance'].xcom_push(key='result', value=result)
    
    if result['status'] == 'failed':
        raise Exception(f"Pipeline failed for {ticker}: {result['error']}")
    
    return result


def aggregate_results(**context):
    """
    Aggregate results from all ticker tasks.
    Useful for sending summary notifications.
    """
    task_instance = context['task_instance']
    
    results = []
    for ticker in DEFAULT_TICKERS:
        task_id = f'process_tickers.process_{ticker}'
        result = task_instance.xcom_pull(task_ids=task_id, key='result')
        if result:
            results.append(result)
    
    total_rows = sum(r.get('rows_processed', 0) for r in results)
    successful = sum(1 for r in results if r.get('status') == 'success')
    failed = len(results) - successful
    
    summary = {
        'total_tickers': len(results),
        'successful': successful,
        'failed': failed,
        'total_rows_processed': total_rows,
        'execution_date': context['execution_date'].isoformat(),
    }
    
    print(f"\n{'='*60}")
    print("PIPELINE EXECUTION SUMMARY")
    print(f"{'='*60}")
    print(f"Execution Date: {summary['execution_date']}")
    print(f"Total Tickers: {summary['total_tickers']}")
    print(f"Successful: {summary['successful']}")
    print(f"Failed: {summary['failed']}")
    print(f"Total Rows Processed: {summary['total_rows_processed']}")
    print(f"{'='*60}\n")
    
    return summary


# Define the DAG
with DAG(
    dag_id='stock_prices_daily_ingestion',
    default_args=default_args,
    description='Daily ingestion of stock prices for multiple tickers',
    schedule_interval='0 18 * * 1-5',  # 6 PM EST, Monday-Friday
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't backfill historical runs
    max_active_runs=1,  # Only one DAG run at a time
    tags=['finance', 'etl', 'stocks'],
) as dag:
    
    # Start task
    start = EmptyOperator(
        task_id='start',
    )
    
    # Task group for parallel ticker processing
    with TaskGroup(group_id='process_tickers') as process_tickers_group:
        for ticker in DEFAULT_TICKERS:
            PythonOperator(
                task_id=f'process_{ticker}',
                python_callable=process_ticker,
                op_kwargs={'ticker': ticker},
                provide_context=True,
            )
    
    # Aggregate results
    aggregate = PythonOperator(
        task_id='aggregate_results',
        python_callable=aggregate_results,
        provide_context=True,
    )
    
    # End task
    end = EmptyOperator(
        task_id='end',
    )
    
    # Define dependencies
    start >> process_tickers_group >> aggregate >> end


# Additional DAG for weekly full refresh
with DAG(
    dag_id='stock_prices_weekly_full_refresh',
    default_args=default_args,
    description='Weekly full refresh of all stock prices',
    schedule_interval='0 2 * * 0',  # 2 AM every Sunday
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['finance', 'etl', 'stocks', 'maintenance'],
) as dag_weekly:
    
    def full_refresh_ticker(ticker: str, **context):
        """Full refresh: fetch all available history."""
        print(f"Full refresh for {ticker}")
        
        with Pipeline() as pipeline:
            result = pipeline.run_for_ticker(
                ticker=ticker,
                start=None,  # Fetch all history
                incremental=False,
            )
        
        context['task_instance'].xcom_push(key='result', value=result)
        
        if result['status'] == 'failed':
            raise Exception(f"Full refresh failed for {ticker}: {result['error']}")
        
        return result
    
    start_weekly = EmptyOperator(task_id='start')
    
    with TaskGroup(group_id='full_refresh_tickers') as refresh_group:
        for ticker in DEFAULT_TICKERS:
            PythonOperator(
                task_id=f'refresh_{ticker}',
                python_callable=full_refresh_ticker,
                op_kwargs={'ticker': ticker},
                provide_context=True,
            )
    
    aggregate_weekly = PythonOperator(
        task_id='aggregate_results',
        python_callable=aggregate_results,
        provide_context=True,
    )
    
    end_weekly = EmptyOperator(task_id='end')
    
    start_weekly >> refresh_group >> aggregate_weekly >> end_weekly
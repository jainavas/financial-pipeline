# Financial Data Pipeline

A production-ready ETL pipeline for ingesting and managing stock market data using Apache Airflow, PostgreSQL, and Python.

## 📋 Overview

This project implements a robust, scalable data pipeline that extracts historical and real-time stock price data from Yahoo Finance, transforms it into a clean format, and loads it into a PostgreSQL database. The pipeline is orchestrated using Apache Airflow with Docker containerization for easy deployment and management.

### Key Features

- **Automated Data Ingestion**: Daily scheduled data collection for multiple stock tickers
- **Incremental Loading**: Smart incremental updates to avoid redundant data fetching
- **Data Quality**: Built-in validation and transformation logic
- **Parallel Processing**: Concurrent ticker processing for improved performance
- **UPSERT Logic**: Handles duplicate data gracefully with update-on-conflict
- **Retry Mechanism**: Automatic retry with exponential backoff for failed operations
- **Monitoring & Logging**: Comprehensive logging for troubleshooting and audit trails
- **Docker Deployment**: Fully containerized environment for consistent deployments

## 🏗️ Architecture

```
┌─────────────────┐
│  Yahoo Finance  │
│      API        │
└────────┬────────┘
         │
         │ Extract (yfinance)
         ▼
┌─────────────────┐
│   Raw Data      │
│   DataFrame     │
└────────┬────────┘
         │
         │ Transform
         ▼
┌─────────────────┐
│  Clean Data     │
│  (Validated)    │
└────────┬────────┘
         │
         │ Load (UPSERT)
         ▼
┌─────────────────┐
│   PostgreSQL    │
│   Database      │
└─────────────────┘
```

### Components

- **Extract** (`extract.py`): Fetches historical stock data using yfinance API
- **Transform** (`transform.py`): Cleans, validates, and standardizes data
- **Load** (`load.py`): Persists data to PostgreSQL with upsert logic
- **Pipeline** (`pipeline.py`): Orchestrates ETL flow with error handling
- **Airflow DAGs**: 
  - `stock_prices_dag.py`: Daily incremental data ingestion
  - `stock_prices_backfill_dag.py`: Historical data backfill

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Git
- Make (optional, for convenience commands)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd financial-pipeline
   ```

2. **Configure environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration if needed
   ```

3. **Initialize Airflow**
   ```bash
   make init
   ```

4. **Start the services**
   ```bash
   make up
   ```

5. **Access Airflow UI**
   - URL: http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

6. **Access PostgreSQL**
   - Host: `localhost`
   - Port: `5432`
   - Database: `financial_data`
   - Username: `admin`
   - Password: `admin123`

## 📁 Project Structure

```
financial-pipeline/
├── config.py                      # Central configuration management
├── docker-compose.yml              # Docker services definition
├── extract.py                      # Data extraction logic
├── load.py                         # Database loading operations
├── transform.py                    # Data transformation logic
├── pipeline.py                     # Main ETL orchestrator
├── requirements.txt                # Python dependencies
├── Makefile                        # Convenience commands
├── .env.example                    # Environment variables template
├── .gitignore                      # Git ignore rules
├── dags/
│   ├── stock_prices_dag.py        # Daily ingestion DAG
│   ├── stock_prices_backfill_dag.py # Historical backfill DAG
│   └── config/
│       └── tickers.py             # Ticker symbol configurations
├── logs/                           # Application and Airflow logs
├── plugins/                        # Custom Airflow plugins
└── src/                            # Additional source code
```

## 🔧 Configuration

### Database Configuration

Edit `.env` file to configure database connection:

```env
DB_HOST=postgres-data
DB_PORT=5432
DB_NAME=financial_data
DB_USER=admin
DB_PASSWORD=admin123
```

### Ticker Configuration

Modify `dags/config/tickers.py` to add or remove stock tickers:

```python
DEFAULT_TICKERS = [
    "AAPL",   # Apple
    "MSFT",   # Microsoft
    "GOOGL",  # Alphabet
    # Add more tickers here
]
```

### Schedule Configuration

The daily DAG runs at **6 PM EST (Monday-Friday)** after market close. Modify the schedule in `stock_prices_dag.py`:

```python
schedule_interval='0 18 * * 1-5',  # Cron expression
```

## 💻 Usage

### Running the Pipeline Manually

Execute the pipeline locally (outside Docker):

```bash
make test
```

Or directly with Python:

```bash
python pipeline.py
```

### Triggering Airflow DAGs

1. **Daily Ingestion** (automatic):
   - Runs daily at 6 PM EST
   - Fetches only new data incrementally
   - Processes all tickers in parallel

2. **Historical Backfill** (manual):
   - Navigate to Airflow UI
   - Find `stock_prices_backfill` DAG
   - Click "Trigger DAG" button
   - Customize parameters as needed

### Makefile Commands

```bash
make help       # Show all available commands
make init       # Initialize Airflow (first time setup)
make up         # Start all services
make down       # Stop all services
make restart    # Restart all services
make logs       # View all logs
make test       # Run pipeline test locally
make clean      # Clean up volumes and logs
```

## 📊 Database Schema

### Table: `stock_prices`

| Column     | Type           | Description                    |
|------------|----------------|--------------------------------|
| id         | SERIAL         | Primary key                    |
| ticker     | VARCHAR(10)    | Stock ticker symbol            |
| date       | DATE           | Trading date                   |
| open       | NUMERIC(12,4)  | Opening price                  |
| high       | NUMERIC(12,4)  | Highest price                  |
| low        | NUMERIC(12,4)  | Lowest price                   |
| close      | NUMERIC(12,4)  | Closing price                  |
| adj_close  | NUMERIC(12,4)  | Adjusted closing price         |
| volume     | BIGINT         | Trading volume                 |
| created_at | TIMESTAMP      | Record creation timestamp      |
| updated_at | TIMESTAMP      | Last update timestamp          |

**Constraints:**
- Unique constraint on `(ticker, date)`
- Indexes on `ticker`, `date`, and `(ticker, date)` for query optimization

## 🔍 Monitoring & Logging

### Application Logs

Logs are written to:
- **File**: `logs/pipeline.log`
- **Console**: stdout (visible in Docker logs)

View logs:
```bash
# All services
make logs

# Airflow scheduler only
make logs-airflow

# Specific service
docker-compose logs -f airflow-webserver
```

### Log Levels

Configure in `.env`:
```env
LOG_LEVEL=INFO  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
```

### Monitoring DAG Execution

- **Airflow UI**: http://localhost:8080
  - View DAG runs, task status, and logs
  - Check XCom values for pipeline results
  - Review task duration and success rates

## 🛠️ Development

### Local Development Setup

1. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run tests**
   ```bash
   python pipeline.py
   ```

### Adding New Tickers

1. Edit `dags/config/tickers.py`
2. Add ticker symbol to appropriate list
3. Restart Airflow scheduler: `make restart`

### Extending the Pipeline

The modular design allows easy extensions:

- **New data sources**: Modify `extract.py`
- **Additional transformations**: Update `transform.py`
- **Different storage**: Modify `load.py`
- **Custom validation**: Add logic in `transform.py`

## 🐛 Troubleshooting

### Common Issues

1. **Port conflicts**
   ```bash
   # Check if ports 5432 or 8080 are in use
   lsof -i :5432
   lsof -i :8080
   
   # Modify ports in docker-compose.yml if needed
   ```

2. **Permission errors**
   ```bash
   # Fix log directory permissions
   chmod -R 755 logs/
   ```

3. **Database connection errors**
   ```bash
   # Verify database is healthy
   docker-compose ps
   
   # Check database logs
   docker-compose logs postgres-data
   ```

4. **Airflow initialization fails**
   ```bash
   # Clean up and reinitialize
   make clean
   make init
   make up
   ```

### Debug Mode

Enable detailed logging:
```env
LOG_LEVEL=DEBUG
```

## 🔐 Security Considerations

### Production Deployment Checklist

- [ ] Change default passwords in `.env`
- [ ] Use secrets management (e.g., AWS Secrets Manager, HashiCorp Vault)
- [ ] Enable SSL for database connections
- [ ] Configure Airflow authentication (LDAP, OAuth)
- [ ] Set up network security groups/firewalls
- [ ] Enable email notifications for failures
- [ ] Implement data retention policies
- [ ] Regular backups of PostgreSQL database

## 📈 Performance Optimization

- **Parallel Processing**: Tickers are processed concurrently in Airflow
- **Incremental Loading**: Only fetches new data after last known date
- **Connection Pooling**: SQLAlchemy connection pool (size: 5, overflow: 10)
- **Batch Processing**: Bulk inserts with UPSERT
- **Indexes**: Optimized database indexes for common queries

## 📝 Dependencies

### Core Technologies

- **Python 3.11**: Programming language
- **Apache Airflow 2.8.0**: Workflow orchestration
- **PostgreSQL 15**: Relational database
- **Docker & Docker Compose**: Containerization
- **SQLAlchemy 2.0**: Database ORM
- **pandas 2.1**: Data manipulation
- **yfinance 1.1**: Stock data API

See [requirements.txt](requirements.txt) for complete dependency list.

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👥 Authors

jainavas

## 🙏 Acknowledgments

- Yahoo Finance for providing free stock data API
- Apache Airflow community for excellent documentation
- Open-source contributors

## 📞 Support

For issues and questions:
- Open an issue on GitHub

---

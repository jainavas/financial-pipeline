"""
Central configuration management using environment variables.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project paths
BASE_DIR = Path(__file__).parent
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# Database configuration
class DatabaseConfig:
    HOST = os.getenv("DB_HOST", "localhost")
    PORT = os.getenv("DB_PORT", "5432")
    NAME = os.getenv("DB_NAME", "financial_data")
    USER = os.getenv("DB_USER", "admin")
    PASSWORD = os.getenv("DB_PASSWORD", "admin123")
    
    @classmethod
    def get_connection_string(cls) -> str:
        return f"postgresql://{cls.USER}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}/{cls.NAME}"

# Logging configuration
class LogConfig:
    LEVEL = os.getenv("LOG_LEVEL", "INFO")
    FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    FILE_NAME = LOGS_DIR / "pipeline.log"

# Data extraction configuration
class ExtractionConfig:
    DEFAULT_START_DATE = None  # None = max available history
    DEFAULT_END_DATE = None    # None = today
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # seconds

# Pipeline configuration
class PipelineConfig:
    BATCH_SIZE = 1000  # For chunked processing if needed
    ENABLE_VALIDATION = True
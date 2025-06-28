"""Configuration management module"""

import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Settings(BaseSettings):
    """Application settings"""
    
    # Environment
    env: str = os.getenv("ENV", "development")
    debug: bool = os.getenv("DEBUG", "false").lower() == "true"  # Added debug
    
    # Paths - Fixed paths to avoid Pydantic issues
    project_root: Path = Path(__file__).parent.parent.parent
    data_path_raw: Path = Path(__file__).parent.parent.parent / "data" / "raw"
    data_path_processed: Path = Path(__file__).parent.parent.parent / "data" / "processed"
    
    # Snowflake
    snowflake_account: Optional[str] = os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_user: Optional[str] = os.getenv("SNOWFLAKE_USER")
    snowflake_password: Optional[str] = os.getenv("SNOWFLAKE_PASSWORD")
    snowflake_warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    snowflake_database: str = os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_ANALYTICS")
    snowflake_schema: str = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
    snowflake_role: str = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    
    # Spark
    spark_app_name: str = "EcommerceAnalytics"
    spark_driver_memory: str = os.getenv("SPARK_DRIVER_MEMORY", "4g")
    spark_executor_memory: str = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    spark_max_result_size: str = os.getenv("SPARK_MAX_RESULT_SIZE", "2g")
    spark_log_level: str = "WARN"
    
    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()
    
    # Performance
    enable_profiling: bool = os.getenv("ENABLE_PROFILING", "false").lower() == "true"
    enable_metrics: bool = os.getenv("ENABLE_METRICS", "true").lower() == "true"
    metrics_port: int = 8000

    class Config:
        case_sensitive = False

# Create settings instance
settings = Settings()

# Simple analytics config
class AnalyticsConfig:
    """Analytics configuration"""
    def __init__(self):
        self.high_value_threshold = float(os.getenv("HIGH_VALUE_THRESHOLD", "100.0"))
        self.bulk_order_threshold = int(os.getenv("BULK_ORDER_THRESHOLD", "100"))
        self.large_order_threshold = 20
        self.medium_order_threshold = 5
        self.premium_price_threshold = 50.0
        self.standard_price_threshold = 10.0
        self.economy_price_threshold = 5.0
        self.min_rows_threshold = 1000
        self.max_duplicate_rate = 0.05
        self.cache_enabled = True
        self.batch_size = 10000

analytics_config = AnalyticsConfig()
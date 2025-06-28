"""Configuration management module"""

import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import field_validator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Settings(BaseSettings):
    """Application settings"""
    
    # Environment
    env: str = os.getenv("ENV", "development")
    
    # Paths
    project_root: Path = Path(__file__).parent.parent.parent
    data_path_raw: Path = project_root / "data" / "raw"
    data_path_processed: Path = project_root / "data" / "processed"
    
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
    
    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Ensure log level is uppercase and valid"""
        v = v.upper()
        valid_levels = ["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]
        if v not in valid_levels:
            return "INFO"
        return v

    class Config:
        case_sensitive = False

# Create global settings instance
settings = Settings()
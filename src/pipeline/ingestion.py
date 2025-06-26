"""Data ingestion module"""

from pathlib import Path
from typing import Optional
from pyspark.sql import DataFrame
from src.utils.spark_manager import SparkManager
from src.utils.logger import logger
from src.utils.config import settings
from src.pipeline.schemas import EcommerceSchema

class DataIngestion:
    """Handle data ingestion from various sources"""
    
    def __init__(self):
        self.spark = SparkManager.get_session()
        self.schema = EcommerceSchema()
    
    def read_csv_data(self, filepath: Optional[Path] = None) -> DataFrame:
        """Read CSV data with schema"""
        if filepath is None:
            filepath = settings.data_path_raw
        
        # Find CSV files
        csv_files = list(Path(filepath).glob("*.csv"))
        
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {filepath}")
        
        # Log found files
        logger.info(f"Found {len(csv_files)} CSV files:")
        for file in csv_files:
            logger.info(f"  - {file.name}")
        
        # Read the main file (looking for ecommerce_data.csv or data.csv)
        main_file = None
        for file in csv_files:
            if 'ecommerce_data' in file.name or 'data.csv' in file.name:
                main_file = file
                break
        
        if not main_file:
            main_file = csv_files[0]  # Use first file if no match
            
        logger.info(f"Reading main file: {main_file.name}")
        
        # Read with schema
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(self.schema.sales_schema()) \
            .csv(str(main_file))
        
        # Log basic statistics
        row_count = df.count()
        col_count = len(df.columns)
        logger.info(f"Loaded {row_count:,} rows with {col_count} columns")
        
        # Show sample
        logger.info("Sample data:")
        df.show(5, truncate=False)
        
        return df
    
    def validate_schema(self, df: DataFrame) -> bool:
        """Validate dataframe schema"""
        expected_cols = set(self.schema.standardized_columns().values())
        actual_cols = set(df.columns)
        
        missing_cols = expected_cols - actual_cols
        extra_cols = actual_cols - expected_cols
        
        if missing_cols:
            logger.error(f"Missing columns: {missing_cols}")
            return False
        
        if extra_cols:
            logger.warning(f"Extra columns found: {extra_cols}")
        
        logger.success("Schema validation passed")
        return True

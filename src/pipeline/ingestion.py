"""Data ingestion module with optimizations"""

from pathlib import Path
from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F  
from pyspark.sql.types import StructType
from src.utils.spark_manager import SparkManager
from src.utils.logger import logger
from src.utils.config import settings
from src.pipeline.schemas import EcommerceSchema
import time

class DataIngestion:
    """Handle data ingestion from various sources with optimizations"""
    
    def __init__(self):
        self.spark = SparkManager.get_session()
        self.schema = EcommerceSchema()
    
    def read_csv_data(self, filepath: Optional[Path] = None) -> DataFrame:
        """Read CSV data with schema and optimizations"""
        start_time = time.time()
        
        if filepath is None:
            filepath = settings.data_path_raw
        
        # Find CSV files
        csv_files = self._find_csv_files(filepath)
        
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {filepath}")
        
        # Select main file
        main_file = self._select_main_file(csv_files)
        
        logger.info(f"Reading main file: {main_file.name}")
        
        # Read with optimized settings
        df = self._read_optimized(main_file)
        
        # Validate and log statistics
        self._validate_and_log(df, start_time)
        
        return df
    
    def _find_csv_files(self, filepath: Path) -> List[Path]:
        """Find all CSV files in directory"""
        csv_files = list(Path(filepath).glob("*.csv"))
        
        # Also check for compressed files
        compressed_files = list(Path(filepath).glob("*.csv.gz"))
        csv_files.extend(compressed_files)
        
        logger.info(f"Found {len(csv_files)} CSV files:")
        for file in csv_files:
            size_mb = file.stat().st_size / (1024 * 1024)
            logger.info(f"  - {file.name} ({size_mb:.1f} MB)")
        
        return csv_files
    
    def _select_main_file(self, csv_files: List[Path]) -> Path:
        """Select the main data file intelligently"""
        # Priority order for file selection
        priority_patterns = [
            'ecommerce_data',
            'data.csv',
            'transactions',
            'sales'
        ]
        
        for pattern in priority_patterns:
            for file in csv_files:
                if pattern in file.name.lower():
                    return file
        
        # If no match, return the largest file
        return max(csv_files, key=lambda f: f.stat().st_size)
    
    def _read_optimized(self, filepath: Path) -> DataFrame:
        """Read CSV with optimized settings"""
        # Determine if file is compressed
        is_compressed = filepath.suffix == '.gz'
        
        # Base read options
        read_options = {
            "header": "true",
            "inferSchema": "false",
            "mode": "PERMISSIVE",  # Don't fail on corrupt records
            "columnNameOfCorruptRecord": "_corrupt_record",
            "multiLine": "false",
            "encoding": "UTF-8",
            "quote": '"',
            "escape": '"',
            "nullValue": "",
            "nanValue": "NaN",
            "timestampFormat": "M/d/yyyy H:mm"
        }
        
        # Add compression codec if needed
        if is_compressed:
            read_options["compression"] = "gzip"
        
        # Create reader with options
        reader = self.spark.read
        for key, value in read_options.items():
            reader = reader.option(key, value)
        
        # Apply schema and read
        df = reader.schema(self.schema.sales_schema()).csv(str(filepath))
        
        # Check for corrupt records
        if "_corrupt_record" in df.columns:
            corrupt_count = df.filter(df["_corrupt_record"].isNotNull()).count()
            if corrupt_count > 0:
                logger.warning(f"Found {corrupt_count} corrupt records")
                # Log sample corrupt records for debugging
                df.filter(df["_corrupt_record"].isNotNull()).select("_corrupt_record").show(5, False)
            
            # Drop corrupt record column
            df = df.drop("_corrupt_record")
        
        return df
    
    def validate_schema(self, df: DataFrame) -> bool:
        """Validate dataframe schema with detailed reporting"""
        expected_cols = set(self.schema.standardized_columns().values())
        actual_cols = set(df.columns)
        
        missing_cols = expected_cols - actual_cols
        extra_cols = actual_cols - expected_cols
        
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
        
        if extra_cols:
            logger.warning(f"Extra columns found (will be kept): {extra_cols}")
        
        # Validate data types
        schema_fields = {field.name: field.dataType for field in df.schema.fields}
        expected_types = {field.name: field.dataType for field in self.schema.sales_schema().fields}
        
        for col_name, expected_type in expected_types.items():
            if col_name in schema_fields:
                actual_type = schema_fields[col_name]
                if str(actual_type) != str(expected_type):
                    logger.warning(f"Column '{col_name}' has type {actual_type}, expected {expected_type}")
        
        logger.success("Schema validation passed")
        return True
    
    def _validate_and_log(self, df: DataFrame, start_time: float):
        """Validate data and log statistics"""
        # Basic statistics
        row_count = df.count()
        col_count = len(df.columns)
        
        duration = time.time() - start_time
        throughput = row_count / duration if duration > 0 else 0
        
        logger.info(f"Data ingestion completed in {duration:.1f}s")
        logger.info(f"Loaded {row_count:,} rows with {col_count} columns")
        logger.info(f"Throughput: {throughput:,.0f} records/second")
        
        # Show data sample if in debug mode
        if settings.debug:
            logger.info("Sample data:")
            df.show(5, truncate=False)
        
        # Data profile
        if row_count < 1_000_000:  # Only for smaller datasets
            self._quick_profile(df)
    
    def _quick_profile(self, df: DataFrame):
        """Quick data profiling for small datasets"""
        logger.info("Quick data profile:")
        
        # Null counts
        null_counts = df.select([
            F.sum(F.col(c).isNull().cast("int")).alias(c)
            for c in df.columns[:5]  # First 5 columns only
        ]).collect()[0].asDict()
        
        for col, nulls in null_counts.items():
            if nulls > 0:
                logger.info(f"  - {col}: {nulls:,} nulls")
        
        # Unique counts for key columns
        if "CustomerID" in df.columns:
            unique_customers = df.select("CustomerID").distinct().count()
            logger.info(f"  - Unique customers: {unique_customers:,}")
        
        if "StockCode" in df.columns:
            unique_products = df.select("StockCode").distinct().count()
            logger.info(f"  - Unique products: {unique_products:,}")
    
    def read_parquet_data(self, filepath: Path) -> DataFrame:
        """Read Parquet data with optimizations"""
        logger.info(f"Reading Parquet data from {filepath}")
        
        df = self.spark.read \
            .option("mergeSchema", "true") \
            .option("recursiveFileLookup", "true") \
            .parquet(str(filepath))
        
        logger.info(f"Loaded {df.count():,} rows from Parquet")
        return df
    
    def read_json_data(self, filepath: Path, multiline: bool = False) -> DataFrame:
        """Read JSON data with schema inference"""
        logger.info(f"Reading JSON data from {filepath}")
        
        df = self.spark.read \
            .option("multiline", str(multiline).lower()) \
            .option("mode", "PERMISSIVE") \
            .json(str(filepath))
        
        logger.info(f"Loaded {df.count():,} rows from JSON")
        return df
    
    def read_from_database(self, connection_props: Dict[str, str], 
                          query: str) -> DataFrame:
        """Read data from database with optimizations"""
        logger.info("Reading data from database")
        
        # Optimize with partition column if available
        partition_column = connection_props.get("partitionColumn")
        
        if partition_column:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", connection_props["url"]) \
                .option("dbtable", f"({query}) as subquery") \
                .option("user", connection_props["user"]) \
                .option("password", connection_props["password"]) \
                .option("driver", connection_props.get("driver", "org.postgresql.Driver")) \
                .option("partitionColumn", partition_column) \
                .option("lowerBound", connection_props.get("lowerBound", "1")) \
                .option("upperBound", connection_props.get("upperBound", "1000000")) \
                .option("numPartitions", connection_props.get("numPartitions", "10")) \
                .option("fetchsize", "10000") \
                .load()
        else:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", connection_props["url"]) \
                .option("dbtable", f"({query}) as subquery") \
                .option("user", connection_props["user"]) \
                .option("password", connection_props["password"]) \
                .option("driver", connection_props.get("driver", "org.postgresql.Driver")) \
                .option("fetchsize", "10000") \
                .load()
        
        logger.info(f"Loaded {df.count():,} rows from database")
        return df
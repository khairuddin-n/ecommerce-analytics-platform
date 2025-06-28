"""Test configuration and fixtures"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random
import os
import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Disable metrics for tests to avoid Prometheus conflicts
os.environ["ENABLE_METRICS"] = "false"

# Create a single Spark session for all tests
_spark = None

def get_spark():
    """Get or create Spark session"""
    global _spark
    if _spark is None:
        _spark = SparkSession.builder \
            .appName("test") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
            .getOrCreate()
    return _spark

@pytest.fixture(scope="session")
def spark():
    """Create Spark session for tests"""
    spark_session = get_spark()
    yield spark_session
    # Don't stop - let pytest handle it

@pytest.fixture(scope="function", autouse=True)
def spark_context_fixture(spark):
    """Ensure spark context is available for each test"""
    # This will run before each test to ensure spark is initialized
    yield spark

@pytest.fixture
def sample_ecommerce_data(spark):
    """Generate sample e-commerce transaction data"""
    from pyspark.sql.types import Row
    
    # Generate sample data
    data = []
    n_transactions = 200
    customers = [f"CUST{i:04d}" for i in range(1, 51)]
    products = [f"PROD{i:04d}" for i in range(1, 31)]
    countries = ["United Kingdom", "France", "Germany", "EIRE", "Spain"]
    
    base_date = datetime(2011, 1, 1)
    
    for i in range(n_transactions):
        invoice_date = base_date + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(8, 20),
            minutes=random.randint(0, 59)
        )
        
        invoice_no = f"INV{(i // 5):05d}"
        
        # Create Row with exact field order matching schema
        row = Row(
            InvoiceNo=invoice_no,
            StockCode=random.choice(products),
            Description=f"Product Description {random.randint(1, 30)}",
            Quantity=random.randint(1, 12),
            InvoiceDate=invoice_date.strftime("%m/%d/%Y %H:%M"),
            UnitPrice=round(random.uniform(0.5, 100.0), 2),
            CustomerID=random.choice(customers) if random.random() > 0.1 else None,
            Country=random.choice(countries)
        )
        data.append(row)
    
    # Create DataFrame
    return spark.createDataFrame(data)

@pytest.fixture
def transformed_data(spark, sample_ecommerce_data):
    """Create pre-transformed data for analytics tests"""
    from src.pipeline.transformations import DataTransformer
    
    transformer = DataTransformer()
    standardized = transformer.standardize_columns(sample_ecommerce_data)
    cleaned = transformer.clean_data(standardized)
    enriched = transformer.add_business_flags(cleaned)
    return enriched

# Set up pytest to use our spark session
def pytest_configure(config):
    """Configure pytest with spark session"""
    # Initialize Spark early
    get_spark()

def pytest_unconfigure(config):
    """Clean up spark session"""
    global _spark
    if _spark:
        _spark.stop()
        _spark = None
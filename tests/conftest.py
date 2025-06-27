"""Test configuration and fixtures"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random

@pytest.fixture(scope="session")
def spark():
    """Create Spark session for tests"""
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    yield spark
    spark.stop()

@pytest.fixture
def sample_ecommerce_data(spark):
    """Generate sample e-commerce transaction data"""
    # Generate sample data matching our schema
    data = []
    
    # Configuration
    n_transactions = 200
    customers = [f"CUST{i:04d}" for i in range(1, 51)]
    products = [f"PROD{i:04d}" for i in range(1, 31)]
    countries = ["United Kingdom", "France", "Germany", "EIRE", "Spain", "Netherlands", "Belgium", "Switzerland"]
    
    # Generate transactions
    base_date = datetime(2011, 1, 1)
    
    for i in range(n_transactions):
        # Generate invoice date
        invoice_date = base_date + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(8, 20),
            minutes=random.randint(0, 59)
        )
        
        # Group transactions by invoice
        invoice_no = f"INV{(i // 5):05d}"  # 5 items per invoice average
        
        data.append({
            "InvoiceNo": invoice_no,
            "StockCode": random.choice(products),
            "Description": f"Product Description {random.randint(1, 30)}",
            "Quantity": random.randint(1, 12),
            "InvoiceDate": invoice_date.strftime("%m/%d/%Y %H:%M"),
            "UnitPrice": round(random.uniform(0.5, 100.0), 2),
            "CustomerID": random.choice(customers) if random.random() > 0.1 else None,  # 10% null customers
            "Country": random.choice(countries)
        })
    
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

@pytest.fixture
def enriched_data(spark, transformed_data):
    """Alias for transformed_data with business flags"""
    return transformed_data
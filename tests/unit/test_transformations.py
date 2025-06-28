"""Unit tests for data transformations"""

import pytest
from pyspark.sql import functions as F
from src.pipeline.transformations import DataTransformer
from src.pipeline.schemas import EcommerceSchema

class TestDataTransformer:
    """Test data transformation functions"""
    
    def test_standardize_columns(self, spark, sample_ecommerce_data):
        """Test column standardization"""
        transformer = DataTransformer()
        
        # Apply transformation
        result = transformer.standardize_columns(sample_ecommerce_data)
        
        # Check column names are lowercase with underscores
        expected_columns = set(EcommerceSchema.standardized_columns().values())
        actual_columns = set(result.columns)
        
        assert expected_columns.issubset(actual_columns)
        assert "InvoiceNo" not in result.columns
        assert "invoice_no" in result.columns
    
    def test_clean_data_removes_invalid_records(self, spark):
        """Test that clean_data removes invalid records"""
        transformer = DataTransformer()
        
        # Create test data with invalid records
        data = [
            ("INV001", "PROD001", "Product 1", 5, "12/1/2010 8:26", 10.0, "CUST001", "UK"),
            ("INV002", "PROD002", "Product 2", -3, "12/1/2010 9:00", 15.0, "CUST002", "UK"),  # Negative qty
            ("INV003", "PROD003", "Product 3", 2, "12/1/2010 10:00", 0.0, "CUST003", "UK"),   # Zero price
            ("INV004", "PROD004", "Product 4", 0, "12/1/2010 11:00", 20.0, None, "UK"),       # Zero qty
        ]
        columns = ["InvoiceNo", "StockCode", "Description", "Quantity", 
                   "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
        
        df = spark.createDataFrame(data, columns)
        
        # Standardize and clean
        standardized = transformer.standardize_columns(df)
        result = transformer.clean_data(standardized)
        
        # Should only have 1 valid record
        assert result.count() == 1
        assert result.filter(F.col("invoice_no") == "INV001").count() == 1
    
    def test_date_parsing(self, spark):
        """Test date parsing functionality"""
        transformer = DataTransformer()
        
        # Create test data with various date formats
        data = [
            ("INV001", "PROD001", "Product 1", 1, "12/1/2010 8:26", 10.0, "CUST001", "UK"),
            ("INV002", "PROD002", "Product 2", 1, "1/15/2011 14:30", 15.0, "CUST002", "UK"),
            ("INV003", "PROD003", "Product 3", 1, "12/31/2011 23:59", 20.0, "CUST003", "UK"),
        ]
        columns = ["InvoiceNo", "StockCode", "Description", "Quantity", 
                   "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
        
        df = spark.createDataFrame(data, columns)
        
        # Process
        standardized = transformer.standardize_columns(df)
        result = transformer.clean_data(standardized)
        
        # Check date columns exist
        assert "invoice_date" in result.columns
        assert "invoice_datetime" in result.columns
        assert "year" in result.columns
        assert "month" in result.columns
        assert "day" in result.columns
        assert "hour" in result.columns
        
        # Check date values
        first_row = result.filter(F.col("invoice_no") == "INV001").first()
        assert first_row["year"] == 2010
        assert first_row["month"] == 12
        assert first_row["day"] == 1
        assert first_row["hour"] == 8
    
    def test_business_flags(self, spark):
        """Test business flag creation"""
        transformer = DataTransformer()
        
        # Create test data with various scenarios
        data = [
            ("INV001", "PROD001", "Product 1", 150, "12/1/2010 8:00", 5.0, "CUST001", "United Kingdom"),   # Bulk, Economy
            ("INV002", "PROD002", "Product 2", 3, "12/1/2010 9:00", 75.0, "CUST002", "France"),          # Small, Premium
            ("INV003", "PROD003", "Product 3", 25, "12/1/2010 10:00", 12.0, "CUST003", "Germany"),       # Large, Standard
            ("INV004", "PROD004", "Product 4", 8, "12/1/2010 11:00", 3.0, "CUST004", "United Kingdom"),  # Medium, Budget
        ]
        columns = ["InvoiceNo", "StockCode", "Description", "Quantity", 
                   "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
        
        df = spark.createDataFrame(data, columns)
        
        # Process
        standardized = transformer.standardize_columns(df)
        cleaned = transformer.clean_data(standardized)
        result = transformer.add_business_flags(cleaned)
        
        # Check flags exist
        assert "order_size" in result.columns
        assert "price_category" in result.columns
        assert "is_high_value" in result.columns
        assert "is_uk" in result.columns
        assert "is_international" in result.columns
        
        # Verify flag values
        rows = result.orderBy("invoice_no").collect()
        
        # First row checks
        assert rows[0]["order_size"] == "Bulk"
        assert rows[0]["price_category"] == "Economy"
        assert rows[0]["is_uk"] == 1
        assert rows[0]["is_international"] == 0
        
        # Second row checks
        assert rows[1]["order_size"] == "Small"
        assert rows[1]["price_category"] == "Premium"
        assert rows[1]["is_uk"] == 0
        assert rows[1]["is_international"] == 1
    
    def test_null_handling(self, spark):
        """Test null value handling"""
        transformer = DataTransformer()
        
        # Create data with nulls
        data = [
            ("INV001", "PROD001", None, 5, "12/1/2010 8:26", 10.0, None, "UK"),           # Null description and customer
            ("INV002", None, "Product 2", 3, "12/1/2010 9:00", 15.0, "CUST002", "UK"),   # Null product
            (None, "PROD003", "Product 3", 2, "12/1/2010 10:00", 20.0, "CUST003", "UK"), # Null invoice
        ]
        columns = ["InvoiceNo", "StockCode", "Description", "Quantity", 
                   "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
        
        df = spark.createDataFrame(data, columns)
        
        # Process
        standardized = transformer.standardize_columns(df)
        result = transformer.clean_data(standardized)
        
        # Should handle customer null as "Unknown"
        unknown_customers = result.filter(F.col("customer_id") == "Unknown").count()
        assert unknown_customers == 1
        
        # Should drop rows with null invoice or product
        assert result.count() == 1
        assert result.filter(F.col("invoice_no") == "INV001").count() == 1

    def test_clean_data_optimized(self, spark):
        """Test optimized data cleaning"""
        transformer = DataTransformer()
        
        # Create test data with various issues
        data = [
            ("INV001", "PROD001", "Product 1", 5, "12/1/2010 8:26", 10.0, "CUST001", "UK"),
            ("INV001", "PROD001", "Product 1", 5, "12/1/2010 8:26", 10.0, "CUST001", "UK"),  # Duplicate
            ("INV002", "PROD002", "Product 2", -3, "12/1/2010 9:00", 15.0, "CUST002", "UK"),  # Negative qty
            ("INV003", "PROD003", "Product 3", 2, "12/1/2010 10:00", 0.0, "CUST003", "UK"),   # Zero price
            ("INV004", "PROD004", "Product 4", 15000, "12/1/2010 11:00", 20.0, None, "UK"),   # Extreme qty
            ("INV005", "PROD005", "Product 5", 10, "12/1/2010 12:00", 15000.0, None, "UK"),   # Extreme price
        ]
        columns = ["InvoiceNo", "StockCode", "Description", "Quantity", 
                "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
        
        df = spark.createDataFrame(data, columns)
        
        # Standardize and clean
        standardized = transformer.standardize_columns(df)
        result = transformer.clean_data(standardized)
        
        # Should filter out invalid records
        assert result.count() == 1  # Only first record is valid
        assert result.filter(F.col("invoice_no") == "INV001").count() == 1

    def test_add_product_flags(self, spark):
        """Test product flag addition"""
        transformer = DataTransformer()
        
        # Create test data with specific descriptions
        data = [
            ("INV001", "PROD001", "VINTAGE FLOWER VASE", 1, "12/1/2010 8:00", 50.0, "CUST001", "UK"),
            ("INV002", "PROD002", "CHRISTMAS DECORATION SET", 2, "12/1/2010 9:00", 25.0, "CUST002", "UK"),
            ("INV003", "PROD003", "GIFT WRAP PAPER", 5, "12/1/2010 10:00", 5.0, "CUST003", "UK"),
            ("INV004", "PROD004", "REGULAR ITEM", 1, "12/1/2010 11:00", 10.0, "CUST004", "UK"),
        ]
        columns = ["InvoiceNo", "StockCode", "Description", "Quantity", 
                "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
        
        df = spark.createDataFrame(data, columns)
        
        # Process
        standardized = transformer.standardize_columns(df)
        cleaned = transformer.clean_data(standardized)
        result = transformer.add_business_flags(cleaned)
        
        # Check product flags
        assert "is_vintage" in result.columns
        assert "is_christmas" in result.columns
        assert "is_gift" in result.columns
        
        # Verify flag values
        vintage_count = result.filter(F.col("is_vintage") == 1).count()
        christmas_count = result.filter(F.col("is_christmas") == 1).count()
        gift_count = result.filter(F.col("is_gift") == 1).count()
        
        assert vintage_count == 1
        assert christmas_count == 1
        assert gift_count == 1

    def test_add_business_flags_with_config(self, spark, monkeypatch):
        """Test business flags use configuration values"""
        # Mock config values
        from src.utils.config import analytics_config
        monkeypatch.setattr(analytics_config, "high_value_threshold", 50.0)
        monkeypatch.setattr(analytics_config, "bulk_order_threshold", 100)
        
        transformer = DataTransformer()
        
        # Create test data
        data = [
            ("INV001", "PROD001", "Product 1", 150, "12/1/2010 8:00", 1.0, "CUST001", "UK"),  # Bulk order, total=150
            ("INV002", "PROD002", "Product 2", 10, "12/1/2010 9:00", 4.0, "CUST002", "UK"),   # Not high value, total=40
        ]
        columns = ["InvoiceNo", "StockCode", "Description", "Quantity", 
                "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
        
        df = spark.createDataFrame(data, columns)
        
        # Process
        standardized = transformer.standardize_columns(df)
        cleaned = transformer.clean_data(standardized)
        result = transformer.add_business_flags(cleaned)
        
        # Verify flags use config values
        bulk_orders = result.filter(F.col("order_size") == "Bulk").count()
        high_value_orders = result.filter(F.col("is_high_value") == 1).count()
        
        assert bulk_orders == 1
        # First order: 150 * 1 = 150 (high value)
        # Second order: 10 * 4 = 40 (not high value)
        assert high_value_orders == 1
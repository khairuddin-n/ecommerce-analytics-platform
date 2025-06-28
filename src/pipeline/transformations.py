"""Data transformation module"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.logger import logger
from src.pipeline.schemas import EcommerceSchema
from typing import Dict

class DataTransformer:
    """Handle data transformations"""
    
    def __init__(self):
        self.column_mapping = EcommerceSchema.standardized_columns()
    
    def standardize_columns(self, df: DataFrame) -> DataFrame:
        """Standardize column names"""
        logger.info("Standardizing column names")
        
        # Rename columns
        for old_name, new_name in self.column_mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
        
        return df
    
    def clean_data(self, df: DataFrame) -> DataFrame:
        """Clean and prepare data"""
        logger.info("Cleaning data")
        
        # Initial count
        initial_count = df.count()
        
        # Remove duplicates
        df = df.dropDuplicates(["invoice_no", "product_id", "invoice_date"])
        
        # Handle nulls
        df = self._handle_nulls(df)
        
        # Parse dates
        df = self._parse_dates(df)
        
        # Calculate total price
        df = df.withColumn("total_price", F.col("quantity") * F.col("unit_price"))
        
        # Add derived columns
        df = self._add_derived_columns(df)
        
        # Filter invalid data
        df = df.filter(
            (F.col("quantity") > 0) &  # Remove returns/cancellations
            (F.col("unit_price") > 0) &  # Remove invalid prices
            (F.col("invoice_date").isNotNull())
        )
        
        final_count = df.count()
        logger.info(f"Cleaned data: {initial_count:,} → {final_count:,} rows")
        
        return df
    
    def _handle_nulls(self, df: DataFrame) -> DataFrame:
        """Handle null values"""
        # Check null counts
        null_counts = df.select([
            F.sum(F.col(c).isNull().cast("int")).alias(c) 
            for c in df.columns
        ]).collect()[0].asDict()
        
        # Log columns with nulls
        for col, null_count in null_counts.items():
            if null_count > 0:
                pct = (null_count / df.count()) * 100
                logger.warning(f"Column '{col}' has {null_count:,} nulls ({pct:.1f}%)")
        
        # Handle CustomerID nulls (common in this dataset)
        df = df.fillna({"customer_id": "Unknown"})
        
        # Drop rows with critical nulls
        critical_cols = ["invoice_no", "product_id", "quantity", "unit_price"]
        df = df.dropna(subset=critical_cols)
        
        # Clean product descriptions
        df = df.fillna({"product_description": "No Description"})
        
        return df
    
    def _parse_dates(self, df: DataFrame) -> DataFrame:
        """Parse date columns"""
        logger.info("Parsing date columns")
        
        # Parse invoice date (format: "12/1/2010 8:26")
        df = df.withColumn("invoice_datetime", 
                          F.to_timestamp(F.col("invoice_date"), "M/d/yyyy H:mm"))
        
        # Create date-only column
        df = df.withColumn("invoice_date", F.to_date("invoice_datetime"))
        
        # Validate dates
        invalid_dates = df.filter(F.col("invoice_datetime").isNull()).count()
        if invalid_dates > 0:
            logger.warning(f"Found {invalid_dates:,} rows with invalid dates - removing")
            df = df.filter(F.col("invoice_datetime").isNotNull())
        
        return df
    
    def _add_derived_columns(self, df: DataFrame) -> DataFrame:
        """Add calculated columns"""
        logger.info("Adding derived columns")
        
        # Fix weekofyear deprecation
        df = df.withColumn("year", F.year("invoice_date")) \
            .withColumn("month", F.month("invoice_date")) \
            .withColumn("day", F.dayofmonth("invoice_date")) \
            .withColumn("hour", F.hour("invoice_datetime")) \
            .withColumn("day_of_week", F.dayofweek("invoice_date")) \
            .withColumn("is_weekend", 
                        F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0)) \
            .withColumn("quarter", F.quarter("invoice_date")) 
        
        # Time-based features
        df = df.withColumn("is_morning", 
                          F.when((F.col("hour") >= 6) & (F.col("hour") < 12), 1).otherwise(0)) \
               .withColumn("is_afternoon",
                          F.when((F.col("hour") >= 12) & (F.col("hour") < 18), 1).otherwise(0)) \
               .withColumn("is_evening",
                          F.when((F.col("hour") >= 18) & (F.col("hour") < 24), 1).otherwise(0))
        
        return df
    
    def add_business_flags(self, df: DataFrame) -> DataFrame:
        """Add business logic flags"""
        logger.info("Adding business flags")
        
        # Quantity-based flags
        df = df.withColumn("order_size",
                          F.when(F.col("quantity") >= 100, "Bulk")
                           .when(F.col("quantity") >= 20, "Large")
                           .when(F.col("quantity") >= 5, "Medium")
                           .otherwise("Small"))
        
        # Price-based flags
        df = df.withColumn("price_category",
                          F.when(F.col("unit_price") >= 50, "Premium")
                           .when(F.col("unit_price") >= 10, "Standard")
                           .when(F.col("unit_price") >= 5, "Economy")
                           .otherwise("Budget"))
        
        # Revenue-based flags
        df = df.withColumn("is_high_value",
                          F.when(F.col("total_price") >= 100, 1).otherwise(0))
        
        # Country-based flags
        df = df.withColumn("is_uk",
                          F.when(F.col("country") == "United Kingdom", 1).otherwise(0)) \
               .withColumn("is_international",
                          F.when(F.col("country") != "United Kingdom", 1).otherwise(0))
        
        return df

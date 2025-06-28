"""Data transformation module with optimizations"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, broadcast 
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from src.utils.logger import logger
from src.utils.config import analytics_config, settings
from src.pipeline.schemas import EcommerceSchema
from typing import Dict, Optional
import time

class DataTransformer:
    """Handle data transformations with optimizations"""
    
    def __init__(self):
        self.column_mapping = EcommerceSchema.standardized_columns()
        self.config = analytics_config
    
    def standardize_columns(self, df: DataFrame) -> DataFrame:
        """Standardize column names efficiently"""
        logger.info("Standardizing column names")
        
        # Batch rename columns
        for old_name, new_name in self.column_mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
        
        return df
    
    def clean_data(self, df: DataFrame) -> DataFrame:
        """Clean and prepare data with optimizations"""
        start_time = time.time()
        logger.info("Cleaning data")
        
        # Initial count
        initial_count = df.count()
        
        # Remove duplicates efficiently
        df = df.dropDuplicates(["invoice_no", "product_id", "invoice_date"])
        
        # Handle nulls with optimized logic
        df = self._handle_nulls_optimized(df)
        
        # Parse dates efficiently
        df = self._parse_dates_optimized(df)
        
        # Calculate total price
        df = df.withColumn("total_price", 
                          F.round(col("quantity") * col("unit_price"), 2))
        
        # Add derived columns
        df = self._add_derived_columns_optimized(df)
        
        # Filter invalid data in one pass
        df = df.filter(
            (col("quantity") > 0) &  
            (col("unit_price") > 0) & 
            (col("invoice_date").isNotNull()) &
            (col("total_price") > 0) &
            # Additional filters
            (col("unit_price") < 10000) &  # Remove extreme prices
            (col("quantity") < 10000)  # Remove extreme quantities
        )
        
        final_count = df.count()
        duration = time.time() - start_time
        
        logger.info(f"Cleaned data in {duration:.1f}s: {initial_count:,} → {final_count:,} rows "
                   f"({(final_count/initial_count)*100:.1f}% retained)")
        
        return df
    
    def _handle_nulls_optimized(self, df: DataFrame) -> DataFrame:
        """Handle null values efficiently"""
        # Check null counts only if in debug mode
        if settings.debug:  
            null_counts = df.select([
                F.sum(col(c).isNull().cast("int")).alias(c) 
                for c in df.columns
            ]).collect()[0].asDict()
            
            for col_name, null_count in null_counts.items():
                if null_count > 0:
                    pct = (null_count / df.count()) * 100
                    logger.debug(f"Column '{col_name}' has {null_count:,} nulls ({pct:.1f}%)")
        
        # Efficient null handling
        df = df.fillna({
            "customer_id": "Unknown",
            "product_description": "No Description",
            "country": "Unknown"
        })
        
        # Drop rows with critical nulls
        critical_cols = ["invoice_no", "product_id", "quantity", "unit_price"]
        df = df.dropna(subset=critical_cols)
        
        return df
    
    def _parse_dates_optimized(self, df: DataFrame) -> DataFrame:
        """Parse date columns with optimization"""
        logger.info("Parsing date columns")
        
        # Use Spark's built-in date parsing with format
        df = df.withColumn("invoice_datetime", 
                          F.to_timestamp(col("invoice_date"), "M/d/yyyy H:mm"))
        
        # Filter invalid dates early
        df = df.filter(col("invoice_datetime").isNotNull())
        
        # Create date-only column
        df = df.withColumn("invoice_date", F.to_date("invoice_datetime"))
        
        # Validate date range (2010-2012 for this dataset)
        df = df.filter(
            (F.year("invoice_date") >= 2010) & 
            (F.year("invoice_date") <= 2012)
        )
        
        return df
    
    def _add_derived_columns_optimized(self, df: DataFrame) -> DataFrame:
        """Add calculated columns efficiently"""
        logger.info("Adding derived columns")
        
        # Date features in one pass
        df = df.select("*",
            F.year("invoice_date").alias("year"),
            F.month("invoice_date").alias("month"),
            F.dayofmonth("invoice_date").alias("day"),
            F.hour("invoice_datetime").alias("hour"),
            F.dayofweek("invoice_date").alias("day_of_week"),
            F.quarter("invoice_date").alias("quarter"),
            F.weekofyear("invoice_date").alias("week_of_year")
        )
        
        # Time-based features using when() for efficiency
        df = df.withColumn("is_weekend",
                          when(col("day_of_week").isin([1, 7]), 1).otherwise(0)) \
               .withColumn("is_morning",
                          when((col("hour") >= 6) & (col("hour") < 12), 1).otherwise(0)) \
               .withColumn("is_afternoon",
                          when((col("hour") >= 12) & (col("hour") < 18), 1).otherwise(0)) \
               .withColumn("is_evening",
                          when((col("hour") >= 18) & (col("hour") < 24), 1).otherwise(0)) \
               .withColumn("is_business_hours",
                          when((col("hour") >= 9) & (col("hour") < 17) & 
                               (col("day_of_week").between(2, 6)), 1).otherwise(0))
        
        return df
    
    def add_business_flags(self, df: DataFrame) -> DataFrame:
        """Add business logic flags with optimizations"""
        start_time = time.time()
        logger.info("Adding business flags")
        
        # Use config values
        config = self.config
        
        # Quantity-based flags using case expressions for efficiency
        df = df.withColumn("order_size",
            F.expr(f"""
                CASE 
                    WHEN quantity >= {config.bulk_order_threshold} THEN 'Bulk'
                    WHEN quantity >= {config.large_order_threshold} THEN 'Large'
                    WHEN quantity >= {config.medium_order_threshold} THEN 'Medium'
                    ELSE 'Small'
                END
            """))
        
        # Price-based flags
        df = df.withColumn("price_category",
            F.expr(f"""
                CASE
                    WHEN unit_price >= {config.premium_price_threshold} THEN 'Premium'
                    WHEN unit_price >= {config.standard_price_threshold} THEN 'Standard'
                    WHEN unit_price >= {config.economy_price_threshold} THEN 'Economy'
                    ELSE 'Budget'
                END
            """))
        
        # Revenue-based flags
        df = df.withColumn("is_high_value",
                          when(col("total_price") >= config.high_value_threshold, 1)
                          .otherwise(0))
        
        # Country-based flags with broadcast for performance
        uk_countries = ["United Kingdom", "EIRE", "Channel Islands"]
        
        df = df.withColumn("is_uk",
                          when(col("country").isin(uk_countries), 1).otherwise(0)) \
               .withColumn("is_international",
                          when(~col("country").isin(uk_countries), 1).otherwise(0))
        
        # Customer type flags
        df = df.withColumn("is_identified_customer",
                          when(col("customer_id") != "Unknown", 1).otherwise(0))
        
        # Product flags based on description patterns
        df = self._add_product_flags(df)
        
        duration = time.time() - start_time
        logger.info(f"Business flags added in {duration:.1f}s")
        
        return df
    
    def _add_product_flags(self, df: DataFrame) -> DataFrame:
        """Add product-related flags based on descriptions"""
        # Common product patterns
        df = df.withColumn("is_vintage",
                          when(F.lower(col("product_description")).contains("vintage"), 1)
                          .otherwise(0)) \
               .withColumn("is_christmas",
                          when(F.lower(col("product_description")).contains("christmas"), 1)
                          .otherwise(0)) \
               .withColumn("is_gift",
                          when(F.lower(col("product_description")).rlike("gift|present"), 1)
                          .otherwise(0))
        
        return df
    
    def add_advanced_features(self, df: DataFrame) -> DataFrame:
        """Add advanced features for ML models (optional)"""
        logger.info("Adding advanced features")
        
        # Customer purchase patterns
        customer_window = Window.partitionBy("customer_id").orderBy("invoice_date")
        
        df = df.withColumn("customer_purchase_number",
                          F.row_number().over(customer_window)) \
               .withColumn("days_since_first_purchase",
                          F.datediff(col("invoice_date"), 
                                    F.first("invoice_date").over(customer_window)))
        
        # Product popularity
        product_counts = df.groupBy("product_id").count().withColumnRenamed("count", "product_popularity")
        df = df.join(broadcast(product_counts), "product_id", "left")
        
        return df
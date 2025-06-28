"""Unit tests for analytics module"""

import pytest
from pyspark.sql import functions as F
from src.pipeline.analytics import AnalyticsEngine
from src.pipeline.transformations import DataTransformer

class TestAnalyticsEngine:
    """Test analytics calculations"""
    
    def test_customer_metrics_calculation(self, spark, transformed_data):
        """Test customer metrics calculation"""
        analytics = AnalyticsEngine()
        
        # Calculate metrics
        result = analytics.calculate_customer_metrics(transformed_data)
        
        # Check basic structure
        assert result.count() > 0
        
        # Check required columns exist
        required_columns = [
            "customer_id", "country", "total_orders", "total_spent",
            "unique_products", "customer_segment", "value_segment"
        ]
        for col in required_columns:
            assert col in result.columns
        
        # Check calculations
        # Each customer should have at least 1 order
        assert result.filter(F.col("total_orders") < 1).count() == 0
        
        # Total spent should be positive
        assert result.filter(F.col("total_spent") <= 0).count() == 0
        
        # Segments should be assigned
        assert result.filter(F.col("customer_segment").isNull()).count() == 0
    
    def test_customer_segmentation_logic(self, spark):
        """Test customer segmentation logic"""
        analytics = AnalyticsEngine()
        
        # Create specific test data
        data = [
            # High value, frequent customer
            ("CUST001", "UK", 50, 30, 5000.0, 500, 10.0, "2010-01-01", "2010-12-31", 30, 200.0),
            # Low value, occasional customer  
            ("CUST002", "UK", 2, 2, 50.0, 10, 1.0, "2010-06-01", "2010-06-15", 2, 0.0),
            # Medium value, regular customer
            ("CUST003", "UK", 15, 10, 1500.0, 150, 5.0, "2010-01-01", "2010-06-30", 15, 50.0),
        ]
        columns = ["customer_id", "country", "total_orders", "unique_products", 
                   "total_spent", "total_items", "avg_order_value", "first_order_date", 
                   "last_order_date", "active_days", "high_value_orders"]
        
        df = spark.createDataFrame(data, columns).select(
            F.col("customer_id"), F.col("country"), F.col("total_orders"),
            F.col("unique_products"), F.col("total_spent"), F.col("total_items"),
            F.col("avg_order_value"), F.to_date(F.col("first_order_date")).alias("first_order_date"),
            F.to_date(F.col("last_order_date")).alias("last_order_date"),
            F.col("active_days"), F.col("high_value_orders")
        )
        
        # Add required calculated columns
        df = df.withColumn("customer_lifetime_days", 
                          F.datediff(F.col("last_order_date"), F.col("first_order_date")) + 1) \
               .withColumn("order_frequency",
                          F.col("total_orders") / F.col("customer_lifetime_days") * 30) \
               .withColumn("avg_basket_size", F.col("total_items") / F.col("total_orders")) \
               .withColumn("high_value_rate", F.col("high_value_orders") / F.col("total_orders"))
        
        # Apply segmentation
        result = analytics._segment_customers(df)
        
        # Check segments
        segments = result.select("customer_id", "customer_segment").collect()
        segment_dict = {row["customer_id"]: row["customer_segment"] for row in segments}
        
        # CUST001 should be Champions (high value, frequent)
        assert segment_dict["CUST001"] == "Champions"
    
    def test_product_performance_metrics(self, spark, transformed_data):
        """Test product performance calculation"""
        analytics = AnalyticsEngine()
        
        # Calculate metrics
        result = analytics.calculate_product_performance(transformed_data)
        
        # Check structure
        assert result.count() > 0
        
        # Check columns
        required_columns = [
            "product_id", "total_quantity_sold", "total_revenue",
            "unique_customers", "revenue_rank"
        ]
        for col in required_columns:
            assert col in result.columns
        
        # Revenue should be positive
        assert result.filter(F.col("total_revenue") <= 0).count() == 0
        
        # Rank should start from 1
        assert result.filter(F.col("revenue_rank") == 1).count() >= 1
    
    def test_time_series_metrics(self, spark, transformed_data):
        """Test time series calculations"""
        analytics = AnalyticsEngine()
        
        # Calculate metrics
        result = analytics.calculate_time_series_metrics(transformed_data)
        
        # Check structure
        assert result.count() > 0
        
        # Check date column
        assert "invoice_date" in result.columns
        
        # Check metrics columns
        metrics_columns = [
            "total_orders", "unique_customers", "total_revenue",
            "revenue_ma7", "revenue_ma30", "daily_growth"
        ]
        for col in metrics_columns:
            assert col in result.columns
        
        # Orders and revenue should be positive
        assert result.filter(F.col("total_orders") <= 0).count() == 0
        assert result.filter(F.col("total_revenue") <= 0).count() == 0
    
    def test_country_metrics(self, spark, transformed_data):
        """Test country-level metrics"""
        analytics = AnalyticsEngine()
        
        # Calculate metrics
        result = analytics.calculate_country_metrics(transformed_data)
        
        # Check structure
        assert result.count() > 0
        
        # Check required columns
        assert "country" in result.columns
        assert "total_revenue" in result.columns
        assert "unique_customers" in result.columns
        assert "revenue_rank" in result.columns
        
        # Each country should have positive metrics
        assert result.filter(F.col("total_revenue") <= 0).count() == 0
        assert result.filter(F.col("unique_customers") <= 0).count() == 0
    
    def test_hourly_patterns(self, spark, transformed_data):
        """Test hourly pattern analysis"""
        analytics = AnalyticsEngine()
        
        # Calculate patterns
        result = analytics.calculate_hourly_patterns(transformed_data)
        
        # Check structure
        assert result.count() > 0
        
        # Check columns
        assert "hour" in result.columns
        assert "day_of_week" in result.columns
        assert "day_name" in result.columns
        assert "total_revenue" in result.columns
        
        # Hour should be 0-23
        assert result.filter((F.col("hour") < 0) | (F.col("hour") > 23)).count() == 0
        
        # Day of week should be 1-7
        assert result.filter((F.col("day_of_week") < 1) | (F.col("day_of_week") > 7)).count() == 0

    def test_segment_customers_optimized(self, spark):
        """Test optimized customer segmentation"""
        analytics = AnalyticsEngine()
        
        # Create larger test dataset
        data = []
        for i in range(100):
            data.append((
                f"CUST{i:03d}", 
                "UK", 
                i % 20 + 1,  # orders
                i % 15 + 1,  # products
                float(i * 100),  # spent
                i * 10,  # items
                float(i * 10),  # avg order
                "2010-01-01",
                "2010-12-31",
                30,
                i % 5  # high value orders
            ))
        
        columns = ["customer_id", "country", "total_orders", "unique_products", 
                "total_spent", "total_items", "avg_order_value", "first_order_date", 
                "last_order_date", "active_days", "high_value_orders"]
        
        df = spark.createDataFrame(data, columns).select(
            F.col("customer_id"), F.col("country"), F.col("total_orders"),
            F.col("unique_products"), F.col("total_spent"), F.col("total_items"),
            F.col("avg_order_value"), 
            F.to_date(F.lit("2010-01-01")).alias("first_order_date"),
            F.to_date(F.lit("2010-12-31")).alias("last_order_date"),
            F.col("active_days"), F.col("high_value_orders")
        )
        
        # Add required columns
        df = df.withColumn("customer_lifetime_days", F.lit(365)) \
            .withColumn("order_frequency", F.col("total_orders") / 365 * 30) \
            .withColumn("avg_basket_size", F.col("total_items") / F.col("total_orders")) \
            .withColumn("high_value_rate", F.col("high_value_orders") / F.col("total_orders"))
        
        # Test segmentation - use percentile_approx instead of approx_quantile
        result = analytics._segment_customers_optimized(df)
        
        # Verify all customers got segments
        assert result.filter(F.col("customer_segment").isNull()).count() == 0
        
        # Verify segments exist
        segments = result.select("customer_segment").distinct().collect()
        segment_names = [row["customer_segment"] for row in segments]
        
        # Should have multiple segments
        assert len(segment_names) > 3
        assert any(seg in segment_names for seg in ["Champions", "Loyal Customers", "New Customers"])

    def test_calculate_country_metrics_with_top_products(self, spark, transformed_data):
        """Test country metrics with top products calculation"""
        analytics = AnalyticsEngine()
        
        # Calculate metrics
        result = analytics.calculate_country_metrics(transformed_data)
        
        # Check new columns
        assert "revenue_share" in result.columns
        assert "customer_share" in result.columns
        assert "market_tier" in result.columns
        assert "top_3_products" in result.columns
        
        # Verify market tiers
        tiers = result.select("market_tier").distinct().collect()
        tier_names = [row["market_tier"] for row in tiers]
        assert any(tier in tier_names for tier in ["Tier 1", "Tier 2", "Tier 3", "Tier 4"])

    def test_calculate_hourly_patterns_with_peak_detection(self, spark, transformed_data):
        """Test hourly patterns with peak hour detection"""
        analytics = AnalyticsEngine()
        
        # Calculate patterns
        result = analytics.calculate_hourly_patterns(transformed_data)
        
        # Check new columns
        assert "time_period" in result.columns
        assert "is_peak_hour" in result.columns
        assert "transaction_share" in result.columns
        assert "revenue_share" in result.columns
        
        # Verify time periods
        periods = result.select("time_period").distinct().collect()
        period_names = [row["time_period"] for row in periods]
        assert all(period in ["Morning", "Afternoon", "Evening", "Night"] for period in period_names)
        
        # Verify peak hours exist
        peak_hours = result.filter(F.col("is_peak_hour") == 1).count()
        assert peak_hours > 0
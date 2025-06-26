"""Analytics calculations module"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.logger import logger
from typing import Dict, List, Tuple

class AnalyticsEngine:
    """Calculate business analytics and metrics"""
    
    def calculate_customer_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate customer-level metrics"""
        logger.info("Calculating customer metrics")
        
        # Customer aggregations
        customer_metrics = df.groupBy("customer_id", "country") \
            .agg(
                F.countDistinct("invoice_no").alias("total_orders"),
                F.countDistinct("product_id").alias("unique_products"),
                F.sum("total_price").alias("total_spent"),
                F.sum("quantity").alias("total_items"),
                F.avg("total_price").alias("avg_order_value"),
                F.min("invoice_date").alias("first_order_date"),
                F.max("invoice_date").alias("last_order_date"),
                F.countDistinct("invoice_date").alias("active_days"),
                F.sum("is_high_value").alias("high_value_orders")
            )
        
        # Calculate derived metrics
        customer_metrics = customer_metrics \
            .withColumn("customer_lifetime_days",
                       F.datediff(F.col("last_order_date"), 
                                 F.col("first_order_date")) + 1) \
            .withColumn("order_frequency",
                       F.when(F.col("customer_lifetime_days") > 0,
                             F.col("total_orders") / F.col("customer_lifetime_days") * 30)
                        .otherwise(0)) \
            .withColumn("avg_basket_size",
                       F.col("total_items") / F.col("total_orders")) \
            .withColumn("high_value_rate",
                       F.col("high_value_orders") / F.col("total_orders"))
        
        # Customer segmentation
        customer_metrics = self._segment_customers(customer_metrics)
        
        return customer_metrics
    
    def _segment_customers(self, df: DataFrame) -> DataFrame:
        """Segment customers based on behavior"""
        # Calculate percentiles
        percentiles = df.select(
            F.expr("percentile_approx(total_spent, 0.75)").alias("spent_p75"),
            F.expr("percentile_approx(total_spent, 0.50)").alias("spent_p50"),
            F.expr("percentile_approx(total_spent, 0.25)").alias("spent_p25"),
            F.expr("percentile_approx(order_frequency, 0.75)").alias("freq_p75"),
            F.expr("percentile_approx(order_frequency, 0.50)").alias("freq_p50")
        ).collect()[0]
        
        # Create segments
        df = df.withColumn("value_segment",
            F.when(F.col("total_spent") >= percentiles["spent_p75"], "High")
             .when(F.col("total_spent") >= percentiles["spent_p50"], "Medium")
             .when(F.col("total_spent") >= percentiles["spent_p25"], "Low")
             .otherwise("Very Low"))
        
        df = df.withColumn("frequency_segment",
            F.when(F.col("order_frequency") >= percentiles["freq_p75"], "Frequent")
             .when(F.col("order_frequency") >= percentiles["freq_p50"], "Regular")
             .otherwise("Occasional"))
        
        # Combined segmentation
        df = df.withColumn("customer_segment",
            F.when((F.col("value_segment") == "High") & 
                   (F.col("frequency_segment") == "Frequent"), "Champions")
             .when((F.col("value_segment") == "High") & 
                   (F.col("frequency_segment") == "Regular"), "Loyal Customers")
             .when((F.col("value_segment") == "Medium") & 
                   (F.col("frequency_segment") == "Frequent"), "Potential Loyalists")
             .when(F.col("total_orders") == 1, "New Customers")
             .when(F.col("customer_lifetime_days") > 180, "At Risk")
             .otherwise("Needs Attention"))
        
        return df
    
    def calculate_product_performance(self, df: DataFrame) -> DataFrame:
        """Calculate product performance metrics"""
        logger.info("Calculating product performance")
        
        product_metrics = df.groupBy("product_id", "product_description") \
            .agg(
                F.count("*").alias("transaction_count"),
                F.countDistinct("invoice_no").alias("order_count"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.sum("quantity").alias("total_quantity_sold"),
                F.sum("total_price").alias("total_revenue"),
                F.avg("unit_price").alias("avg_unit_price"),
                F.stddev("unit_price").alias("price_volatility"),
                F.countDistinct("country").alias("countries_sold_to"),
                F.sum(F.col("is_international")).alias("international_orders")
            )
        
        # Calculate additional metrics
        product_metrics = product_metrics \
            .withColumn("avg_quantity_per_order",
                       F.col("total_quantity_sold") / F.col("order_count")) \
            .withColumn("revenue_per_customer",
                       F.col("total_revenue") / F.col("unique_customers")) \
            .withColumn("international_rate",
                       F.col("international_orders") / F.col("order_count")) \
            .withColumn("popularity_score",
                       F.col("unique_customers") * F.col("order_count"))
        
        # Rank products
        window_spec = Window.orderBy(F.desc("total_revenue"))
        product_metrics = product_metrics \
            .withColumn("revenue_rank", F.dense_rank().over(window_spec))
        
        return product_metrics
    
    def calculate_time_series_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate time-based metrics"""
        logger.info("Calculating time series metrics")
        
        # Daily metrics
        daily_metrics = df.groupBy("invoice_date") \
            .agg(
                F.countDistinct("invoice_no").alias("total_orders"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.sum("total_price").alias("total_revenue"),
                F.sum("quantity").alias("total_items_sold"),
                F.avg("total_price").alias("avg_order_value"),
                F.countDistinct("product_id").alias("unique_products_sold"),
                F.sum("is_international").alias("international_orders")
            ) \
            .withColumn("avg_items_per_order",
                       F.col("total_items_sold") / F.col("total_orders")) \
            .withColumn("international_rate",
                       F.col("international_orders") / F.col("total_orders"))
        
        # Add moving averages
        window_7d = Window.orderBy("invoice_date").rowsBetween(-6, 0)
        window_30d = Window.orderBy("invoice_date").rowsBetween(-29, 0)
        
        daily_metrics = daily_metrics \
            .withColumn("revenue_ma7", F.avg("total_revenue").over(window_7d)) \
            .withColumn("revenue_ma30", F.avg("total_revenue").over(window_30d)) \
            .withColumn("orders_ma7", F.avg("total_orders").over(window_7d)) \
            .withColumn("orders_ma30", F.avg("total_orders").over(window_30d))
        
        # Add growth metrics
        daily_metrics = daily_metrics \
            .withColumn("revenue_yesterday",
                       F.lag("total_revenue", 1).over(Window.orderBy("invoice_date"))) \
            .withColumn("daily_growth",
                       F.when(F.col("revenue_yesterday") > 0,
                             (F.col("total_revenue") - F.col("revenue_yesterday")) / 
                             F.col("revenue_yesterday") * 100)
                        .otherwise(0))
        
        return daily_metrics
    
    def calculate_country_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate country-level metrics"""
        logger.info("Calculating country metrics")
        
        country_metrics = df.groupBy("country") \
            .agg(
                F.countDistinct("invoice_no").alias("total_orders"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.sum("total_price").alias("total_revenue"),
                F.sum("quantity").alias("total_items"),
                F.avg("total_price").alias("avg_order_value"),
                F.countDistinct("product_id").alias("unique_products"),
                F.min("invoice_date").alias("first_order_date"),
                F.max("invoice_date").alias("last_order_date")
            ) \
            .withColumn("avg_customer_value",
                       F.col("total_revenue") / F.col("unique_customers")) \
            .withColumn("market_days",
                       F.datediff(F.col("last_order_date"), F.col("first_order_date")) + 1)
        
        # Rank countries
        country_metrics = country_metrics \
            .withColumn("revenue_rank",
                       F.dense_rank().over(Window.orderBy(F.desc("total_revenue"))))
        
        return country_metrics
    
    def calculate_hourly_patterns(self, df: DataFrame) -> DataFrame:
        """Analyze hourly sales patterns"""
        logger.info("Calculating hourly patterns")
        
        hourly_patterns = df.groupBy("hour", "day_of_week") \
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("total_price").alias("total_revenue"),
                F.avg("total_price").alias("avg_transaction_value"),
                F.countDistinct("customer_id").alias("unique_customers")
            ) \
            .withColumn("day_name",
                F.when(F.col("day_of_week") == 1, "Sunday")
                 .when(F.col("day_of_week") == 2, "Monday")
                 .when(F.col("day_of_week") == 3, "Tuesday")
                 .when(F.col("day_of_week") == 4, "Wednesday")
                 .when(F.col("day_of_week") == 5, "Thursday")
                 .when(F.col("day_of_week") == 6, "Friday")
                 .otherwise("Saturday"))
        
        return hourly_patterns

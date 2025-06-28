"""Analytics calculations module with optimizations"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, when, col, lit
from src.utils.logger import logger
from src.utils.config import analytics_config, settings
from typing import Dict, List, Tuple, Optional
import time

class AnalyticsEngine:
    """Calculate business analytics and metrics with optimizations"""
    
    def __init__(self):
        self.config = analytics_config
    
    def calculate_customer_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate customer-level metrics with optimizations"""
        start_time = time.time()
        logger.info("Calculating customer metrics")
        
        # Pre-aggregate to reduce shuffle
        customer_agg = df.groupBy("customer_id", "country") \
            .agg(
                F.countDistinct("invoice_no").alias("total_orders"),
                F.countDistinct("product_id").alias("unique_products"),
                F.sum("total_price").alias("total_spent"),
                F.sum("quantity").alias("total_items"),
                F.avg("total_price").alias("avg_order_value"),
                F.min("invoice_date").alias("first_order_date"),
                F.max("invoice_date").alias("last_order_date"),
                F.countDistinct("invoice_date").alias("active_days"),
                F.sum("is_high_value").alias("high_value_orders"),
                # Additional aggregations for better insights
                F.stddev("total_price").alias("order_value_stddev"),
                F.max("total_price").alias("max_order_value"),
                F.sum(when(col("is_weekend") == 1, 1).otherwise(0)).alias("weekend_orders")
            )
        
        # Calculate derived metrics with optimized expressions
        customer_metrics = customer_agg \
            .withColumn("customer_lifetime_days",
                       F.datediff(col("last_order_date"), col("first_order_date")) + 1) \
            .withColumn("order_frequency",
                       when(col("customer_lifetime_days") > 0,
                            col("total_orders") / col("customer_lifetime_days") * 30)
                        .otherwise(0)) \
            .withColumn("avg_basket_size",
                       when(col("total_orders") > 0,
                            col("total_items") / col("total_orders"))
                        .otherwise(0)) \
            .withColumn("high_value_rate",
                       when(col("total_orders") > 0,
                            col("high_value_orders") / col("total_orders"))
                        .otherwise(0)) \
            .withColumn("weekend_order_rate",
                       when(col("total_orders") > 0,
                            col("weekend_orders") / col("total_orders"))
                        .otherwise(0)) \
            .withColumn("order_consistency",
                       when((col("order_value_stddev").isNotNull()) & (col("avg_order_value") > 0),
                            1 - (col("order_value_stddev") / col("avg_order_value")))
                        .otherwise(0))
        
        # Segment customers with optimized logic
        customer_metrics = self._segment_customers_optimized(customer_metrics)
        
        # Add customer rank
        window_spec = Window.orderBy(F.desc("total_spent"))
        customer_metrics = customer_metrics \
            .withColumn("revenue_rank", F.dense_rank().over(window_spec)) \
            .withColumn("revenue_percentile", 
                       F.percent_rank().over(window_spec) * 100)
        
        duration = time.time() - start_time
        logger.info(f"Customer metrics calculated in {duration:.1f}s")
        
        return customer_metrics
    
    def _segment_customers(self, df: DataFrame) -> DataFrame:
        """Segment customers based on behavior (backward compatibility)"""
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

    def _segment_customers_optimized(self, df: DataFrame) -> DataFrame:
        """Optimized customer segmentation using approximate percentiles"""

        quantiles = df.selectExpr(
            "percentile_approx(total_spent, array(0.25, 0.5, 0.75)) as spent_quantiles",
            "percentile_approx(order_frequency, array(0.5, 0.75)) as freq_quantiles"
        ).collect()[0]
        
        spent_q25, spent_q50, spent_q75 = quantiles["spent_quantiles"]
        freq_q50, freq_q75 = quantiles["freq_quantiles"]
        
        # Create value segments using broadcast variables
        df = df.withColumn("value_segment",
            when(col("total_spent") >= spent_q75, "High")
            .when(col("total_spent") >= spent_q50, "Medium")
            .when(col("total_spent") >= spent_q25, "Low")
            .otherwise("Very Low"))
        
        df = df.withColumn("frequency_segment",
            when(col("order_frequency") >= freq_q75, "Frequent")
            .when(col("order_frequency") >= freq_q50, "Regular")
            .otherwise("Occasional"))
        
        # Advanced segmentation with more categories
        df = df.withColumn("customer_segment",
            when((col("value_segment") == "High") & 
                 (col("frequency_segment") == "Frequent"), "Champions")
            .when((col("value_segment") == "High") & 
                  (col("frequency_segment") == "Regular"), "Loyal Customers")
            .when((col("value_segment") == "Medium") & 
                  (col("frequency_segment") == "Frequent"), "Potential Loyalists")
            .when((col("value_segment") == "High") & 
                  (col("frequency_segment") == "Occasional"), "Big Spenders")
            .when((col("value_segment") == "Medium") & 
                  (col("frequency_segment") == "Regular"), "Promising")
            .when(col("total_orders") == 1, "New Customers")
            .when((col("customer_lifetime_days") > 180) & 
                  (col("order_frequency") < freq_q50), "At Risk")
            .when(col("customer_lifetime_days") > 365, "Can't Lose Them")
            .otherwise("Needs Attention"))
        
        return df
    
    def calculate_product_performance(self, df: DataFrame) -> DataFrame:
        """Calculate product performance metrics with optimizations"""
        start_time = time.time()
        logger.info("Calculating product performance")
        
        # Pre-calculate frequently used expressions
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
                F.sum("is_international").alias("international_orders"),
                # Additional metrics
                F.min("unit_price").alias("min_price"),
                F.max("unit_price").alias("max_price"),
                F.avg("quantity").alias("avg_quantity_per_transaction"),
                F.collect_set("country").alias("country_list")
            )
        
        # Calculate additional metrics
        product_metrics = product_metrics \
            .withColumn("avg_quantity_per_order",
                       col("total_quantity_sold") / col("order_count")) \
            .withColumn("revenue_per_customer",
                       col("total_revenue") / col("unique_customers")) \
            .withColumn("international_rate",
                       when(col("order_count") > 0,
                            col("international_orders") / col("order_count"))
                       .otherwise(0)) \
            .withColumn("popularity_score",
                       col("unique_customers") * F.log(col("order_count") + 1)) \
            .withColumn("price_range",
                       col("max_price") - col("min_price")) \
            .withColumn("market_penetration",
                       col("countries_sold_to") / 38.0)  # 38 total countries
        
        # Categorize products
        product_metrics = self._categorize_products(product_metrics)
        
        # Rank products with multiple criteria
        revenue_window = Window.orderBy(F.desc("total_revenue"))
        popularity_window = Window.orderBy(F.desc("popularity_score"))
        
        product_metrics = product_metrics \
            .withColumn("revenue_rank", F.dense_rank().over(revenue_window)) \
            .withColumn("popularity_rank", F.dense_rank().over(popularity_window)) \
            .withColumn("revenue_percentile", 
                       F.percent_rank().over(revenue_window) * 100)
        
        duration = time.time() - start_time
        logger.info(f"Product performance calculated in {duration:.1f}s")
        
        return product_metrics.drop("country_list")  # Drop array column for storage
    
    def _categorize_products(self, df: DataFrame) -> DataFrame:
        """Categorize products based on performance"""
        # Calculate thresholds using percentile_approx
        thresholds = df.selectExpr(
            "percentile_approx(total_revenue, array(0.8)) as revenue_p80",
            "percentile_approx(unique_customers, array(0.8)) as customers_p80"
        ).collect()[0]
        
        revenue_p80 = thresholds["revenue_p80"][0]
        customers_p80 = thresholds["customers_p80"][0]
        
        # Categorize products
        df = df.withColumn("product_category",
            when((col("total_revenue") >= revenue_p80) & 
                 (col("unique_customers") >= customers_p80), "Star")
            .when((col("total_revenue") >= revenue_p80) & 
                  (col("unique_customers") < customers_p80), "Cash Cow")
            .when((col("total_revenue") < revenue_p80) & 
                  (col("unique_customers") >= customers_p80), "Question Mark")
            .otherwise("Dog"))
        
        return df
    
    def calculate_time_series_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate time-based metrics with optimizations"""
        start_time = time.time()
        logger.info("Calculating time series metrics")
        
        # Daily aggregations with additional metrics
        daily_metrics = df.groupBy("invoice_date") \
            .agg(
                F.countDistinct("invoice_no").alias("total_orders"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.sum("total_price").alias("total_revenue"),
                F.sum("quantity").alias("total_items_sold"),
                F.avg("total_price").alias("avg_order_value"),
                F.countDistinct("product_id").alias("unique_products_sold"),
                F.sum("is_international").alias("international_orders"),
                # Additional time-based metrics
                F.stddev("total_price").alias("revenue_stddev"),
                F.max("total_price").alias("max_order_value"),
                F.count(when(col("is_high_value") == 1, True)).alias("high_value_orders"),
                F.sum(when(col("customer_id") != "Unknown", 1).otherwise(0)).alias("identified_orders")
            ) \
            .withColumn("avg_items_per_order",
                       col("total_items_sold") / col("total_orders")) \
            .withColumn("international_rate",
                       col("international_orders") / col("total_orders")) \
            .withColumn("high_value_rate",
                       col("high_value_orders") / col("total_orders")) \
            .withColumn("customer_identification_rate",
                       col("identified_orders") / col("total_orders"))
        
        # Add time-based features
        daily_metrics = daily_metrics \
            .withColumn("day_of_week", F.dayofweek("invoice_date")) \
            .withColumn("day_name", 
                F.when(col("day_of_week") == 1, "Sunday")
                .when(col("day_of_week") == 2, "Monday")
                .when(col("day_of_week") == 3, "Tuesday")
                .when(col("day_of_week") == 4, "Wednesday")
                .when(col("day_of_week") == 5, "Thursday")
                .when(col("day_of_week") == 6, "Friday")
                .otherwise("Saturday")) \
            .withColumn("is_weekend",
                when(col("day_of_week").isin([1, 7]), 1).otherwise(0))
        
        # Add moving averages with partitioned windows for better performance
        window_7d = Window.orderBy("invoice_date").rowsBetween(-6, 0)
        window_30d = Window.orderBy("invoice_date").rowsBetween(-29, 0)
        window_prev = Window.orderBy("invoice_date").rowsBetween(-1, -1)
        
        daily_metrics = daily_metrics \
            .withColumn("revenue_ma7", F.avg("total_revenue").over(window_7d)) \
            .withColumn("revenue_ma30", F.avg("total_revenue").over(window_30d)) \
            .withColumn("orders_ma7", F.avg("total_orders").over(window_7d)) \
            .withColumn("orders_ma30", F.avg("total_orders").over(window_30d)) \
            .withColumn("customers_ma7", F.avg("unique_customers").over(window_7d))
        
        # Add growth metrics
        daily_metrics = daily_metrics \
            .withColumn("revenue_yesterday",
                       F.first("total_revenue").over(window_prev)) \
            .withColumn("daily_growth",
                       when(col("revenue_yesterday").isNotNull() & (col("revenue_yesterday") > 0),
                            ((col("total_revenue") - col("revenue_yesterday")) / 
                             col("revenue_yesterday")) * 100)
                        .otherwise(0)) \
            .withColumn("orders_yesterday",
                       F.first("total_orders").over(window_prev)) \
            .withColumn("order_growth",
                       when(col("orders_yesterday").isNotNull() & (col("orders_yesterday") > 0),
                            ((col("total_orders") - col("orders_yesterday")) / 
                             col("orders_yesterday")) * 100)
                        .otherwise(0))
        
        # Add seasonality indicators
        daily_metrics = daily_metrics \
            .withColumn("month", F.month("invoice_date")) \
            .withColumn("quarter", F.quarter("invoice_date")) \
            .withColumn("year", F.year("invoice_date")) \
            .withColumn("week_of_year", F.weekofyear("invoice_date"))
        
        duration = time.time() - start_time
        logger.info(f"Time series metrics calculated in {duration:.1f}s")
        
        return daily_metrics
    
    def calculate_country_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate country-level metrics with optimizations"""
        start_time = time.time()
        logger.info("Calculating country metrics")
        
        # Get total metrics for percentage calculations
        total_revenue = df.agg(F.sum("total_price")).collect()[0][0]
        total_customers = df.select("customer_id").distinct().count()
        
        country_metrics = df.groupBy("country") \
            .agg(
                F.countDistinct("invoice_no").alias("total_orders"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.sum("total_price").alias("total_revenue"),
                F.sum("quantity").alias("total_items"),
                F.avg("total_price").alias("avg_order_value"),
                F.countDistinct("product_id").alias("unique_products"),
                F.min("invoice_date").alias("first_order_date"),
                F.max("invoice_date").alias("last_order_date"),
                # Additional country metrics
                F.sum(when(col("is_high_value") == 1, 1).otherwise(0)).alias("high_value_orders"),
                F.sum(when(col("order_size") == "Bulk", 1).otherwise(0)).alias("bulk_orders"),
                F.stddev("total_price").alias("revenue_volatility"),
                F.collect_list(F.struct(
                    col("product_id"), 
                    col("total_price")
                )).alias("top_products_data")
            )
        
        # Calculate derived metrics with broadcast variables
        total_revenue_bc = F.lit(total_revenue)
        total_customers_bc = F.lit(total_customers)
        
        country_metrics = country_metrics \
            .withColumn("avg_customer_value",
                       col("total_revenue") / col("unique_customers")) \
            .withColumn("market_days",
                       F.datediff(col("last_order_date"), col("first_order_date")) + 1) \
            .withColumn("daily_revenue_avg",
                       col("total_revenue") / col("market_days")) \
            .withColumn("revenue_share",
                       (col("total_revenue") / total_revenue_bc) * 100) \
            .withColumn("customer_share",
                       (col("unique_customers") / total_customers_bc) * 100) \
            .withColumn("high_value_rate",
                       col("high_value_orders") / col("total_orders")) \
            .withColumn("bulk_order_rate",
                       col("bulk_orders") / col("total_orders"))
        
        # Process top products per country
        country_metrics = self._process_top_products_per_country(country_metrics)
        
        # Rank countries
        revenue_window = Window.orderBy(F.desc("total_revenue"))
        customer_window = Window.orderBy(F.desc("unique_customers"))
        
        country_metrics = country_metrics \
            .withColumn("revenue_rank", F.dense_rank().over(revenue_window)) \
            .withColumn("customer_rank", F.dense_rank().over(customer_window)) \
            .withColumn("market_tier",
                       when(col("revenue_share") >= 10, "Tier 1")
                       .when(col("revenue_share") >= 5, "Tier 2")
                       .when(col("revenue_share") >= 1, "Tier 3")
                       .otherwise("Tier 4"))
        
        duration = time.time() - start_time
        logger.info(f"Country metrics calculated in {duration:.1f}s")
        
        return country_metrics.drop("top_products_data")
    
    def _process_top_products_per_country(self, df: DataFrame) -> DataFrame:
        """Extract top products per country from collected data"""
        from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType
        
        # UDF to get top 3 products
        def get_top_products(products_data):
            if not products_data:
                return []
            
            # Aggregate by product
            product_revenue = {}
            for item in products_data:
                product_id = item['product_id']
                revenue = item['total_price']
                product_revenue[product_id] = product_revenue.get(product_id, 0) + revenue
            
            # Sort and get top 3
            top_products = sorted(product_revenue.items(), key=lambda x: x[1], reverse=True)[:3]
            return [{"product_id": p[0], "revenue": p[1]} for p in top_products]
        
        # Register UDF
        top_products_schema = ArrayType(
            StructType([
                StructField("product_id", StringType(), True),
                StructField("revenue", DoubleType(), True)
            ])
        )
        
        get_top_products_udf = F.udf(get_top_products, top_products_schema)
        
        # Apply UDF
        df = df.withColumn("top_3_products", get_top_products_udf(col("top_products_data")))
        
        return df
    
    def calculate_hourly_patterns(self, df: DataFrame) -> DataFrame:
        """Analyze hourly sales patterns with optimizations"""
        start_time = time.time()
        logger.info("Calculating hourly patterns")
        
        # Pre-calculate total metrics for normalization
        total_transactions = df.count()
        total_revenue = df.agg(F.sum("total_price")).collect()[0][0]
        
        hourly_patterns = df.groupBy("hour", "day_of_week") \
            .agg(
                F.count("*").alias("transaction_count"),
                F.sum("total_price").alias("total_revenue"),
                F.avg("total_price").alias("avg_transaction_value"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.countDistinct("product_id").alias("unique_products"),
                F.sum("quantity").alias("total_items"),
                # Additional pattern metrics
                F.stddev("total_price").alias("revenue_stddev"),
                F.sum(when(col("is_high_value") == 1, 1).otherwise(0)).alias("high_value_transactions"),
                F.sum(when(col("is_international") == 1, 1).otherwise(0)).alias("international_transactions")
            )
        
        # Add normalized metrics using broadcast values
        total_trans_bc = F.lit(total_transactions)
        total_rev_bc = F.lit(total_revenue)
        
        hourly_patterns = hourly_patterns \
            .withColumn("transaction_share",
                    (col("transaction_count") / total_trans_bc) * 100) \
            .withColumn("revenue_share",
                    (col("total_revenue") / total_rev_bc) * 100) \
            .withColumn("avg_items_per_transaction",
                    col("total_items") / col("transaction_count")) \
            .withColumn("high_value_rate",
                    col("high_value_transactions") / col("transaction_count")) \
            .withColumn("international_rate",
                    col("international_transactions") / col("transaction_count"))
        
        # Add day names
        hourly_patterns = hourly_patterns \
            .withColumn("day_name",
                when(col("day_of_week") == 1, "Sunday")
                .when(col("day_of_week") == 2, "Monday")
                .when(col("day_of_week") == 3, "Tuesday")
                .when(col("day_of_week") == 4, "Wednesday")
                .when(col("day_of_week") == 5, "Thursday")
                .when(col("day_of_week") == 6, "Friday")
                .otherwise("Saturday")) \
            .withColumn("time_period",
                when((col("hour") >= 6) & (col("hour") < 12), "Morning")
                .when((col("hour") >= 12) & (col("hour") < 18), "Afternoon")
                .when((col("hour") >= 18) & (col("hour") < 22), "Evening")
                .otherwise("Night"))
        
        # Calculate peak hours using percentile_approx instead of approx_quantile
        hourly_totals = hourly_patterns.groupBy("hour") \
            .agg(F.sum("transaction_count").alias("total_hour_transactions"))
        
        # Use percentile_approx
        peak_threshold = hourly_totals.selectExpr(
            "percentile_approx(total_hour_transactions, 0.75) as threshold"
        ).collect()[0][0]
        
        # Join back and mark peak hours
        hourly_patterns = hourly_patterns.join(
            broadcast(hourly_totals),
            "hour"
        ).withColumn("is_peak_hour",
            when(col("total_hour_transactions") >= peak_threshold, 1).otherwise(0)
        ).drop("total_hour_transactions")
        
        duration = time.time() - start_time
        logger.info(f"Hourly patterns calculated in {duration:.1f}s")
        
        return hourly_patterns
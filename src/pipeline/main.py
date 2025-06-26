"""Main pipeline orchestrator"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
from pyspark.sql import functions as F

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.spark_manager import SparkManager
from src.utils.logger import logger
from src.utils.config import settings
from src.pipeline.ingestion import DataIngestion
from src.pipeline.transformations import DataTransformer
from src.pipeline.analytics import AnalyticsEngine
from src.quality.data_quality import DataQualityChecker

class EcommerceAnalyticsPipeline:
    """Main analytics pipeline for e-commerce data"""
    
    def __init__(self):
        self.spark = SparkManager.get_session()
        self.ingestion = DataIngestion()
        self.transformer = DataTransformer()
        self.analytics = AnalyticsEngine()
        self.quality_checker = DataQualityChecker()
        
    def run(self):
        """Run the complete pipeline"""
        start_time = datetime.now()
        logger.info("="*60)
        logger.info("Starting E-Commerce Analytics Pipeline")
        logger.info("="*60)
        
        try:
            # Step 1: Data Ingestion
            logger.info("Step 1: Data Ingestion")
            raw_df = self.ingestion.read_csv_data()
            
            # Step 2: Data Transformation
            logger.info("Step 2: Data Transformation")
            standardized_df = self.transformer.standardize_columns(raw_df)
            cleaned_df = self.transformer.clean_data(standardized_df)
            enriched_df = self.transformer.add_business_flags(cleaned_df)
            
            # Cache for performance
            enriched_df.cache()
            logger.info(f"Cached {enriched_df.count():,} rows for processing")
            
            # Step 3: Data Quality Checks
            logger.info("Step 3: Data Quality Checks")
            quality_passed, quality_results = self.quality_checker.run_all_checks(enriched_df)
            
            if not quality_passed:
                logger.warning("Some quality checks failed, but continuing with pipeline")
            
            # Step 4: Analytics Calculations
            logger.info("Step 4: Analytics Calculations")
            
            # Customer metrics
            customer_metrics = self.analytics.calculate_customer_metrics(enriched_df)
            logger.info(f"Calculated metrics for {customer_metrics.count():,} customers")
            
            # Product performance
            product_metrics = self.analytics.calculate_product_performance(enriched_df)
            logger.info(f"Calculated metrics for {product_metrics.count():,} products")
            
            # Time series metrics
            daily_metrics = self.analytics.calculate_time_series_metrics(enriched_df)
            logger.info(f"Calculated metrics for {daily_metrics.count():,} days")
            
            # Country metrics
            country_metrics = self.analytics.calculate_country_metrics(enriched_df)
            logger.info(f"Calculated metrics for {country_metrics.count():,} countries")
            
            # Hourly patterns
            hourly_patterns = self.analytics.calculate_hourly_patterns(enriched_df)
            logger.info(f"Calculated hourly patterns")
            
            # Step 5: Save Results
            logger.info("Step 5: Saving Results")
            self._save_results(
                enriched_data=enriched_df,
                customer_metrics=customer_metrics,
                product_metrics=product_metrics,
                daily_metrics=daily_metrics,
                country_metrics=country_metrics,
                hourly_patterns=hourly_patterns
            )
            
            # Calculate execution time
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.success("="*60)
            logger.success(f"Pipeline completed successfully in {duration:.1f} seconds!")
            logger.success("="*60)
            
            # Show sample results
            self._show_sample_results(customer_metrics, product_metrics, daily_metrics, country_metrics)
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            # Cleanup
            SparkManager.stop_session()
    
    def _save_results(self, **dataframes):
        """Save all result dataframes"""
        output_path = settings.data_path_processed
        
        for name, df in dataframes.items():
            path = output_path / name
            logger.info(f"Saving {name} to {path}")
            
            # Coalesce to reduce number of files
            df.coalesce(4) \
                .write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(str(path))
    
    def _show_sample_results(self, customer_df, product_df, daily_df, country_df):
        """Display sample results"""
        logger.info("\n" + "="*60)
        logger.info("SAMPLE RESULTS")
        logger.info("="*60)
        
        # Top customers
        logger.info("\n📊 Top 10 Customers by Revenue:")
        customer_df.select(
            "customer_id", "country", "total_spent", "total_orders", 
            "unique_products", "customer_segment"
        ).orderBy(F.desc("total_spent")).show(10, truncate=False)
        
        # Top products
        logger.info("\n📦 Top 10 Products by Revenue:")
        product_df.select(
            "product_id", "product_description", "total_revenue", 
            "unique_customers", "total_quantity_sold"
        ).orderBy(F.desc("total_revenue")).show(10, truncate=False)
        
        # Recent daily metrics
        logger.info("\n📅 Recent Daily Metrics:")
        daily_df.select(
            "invoice_date", "total_orders", "total_revenue", 
            "unique_customers", "daily_growth"
        ).orderBy(F.desc("invoice_date")).show(10)
        
        # Top countries
        logger.info("\n🌍 Top Countries by Revenue:")
        country_df.select(
            "country", "total_revenue", "unique_customers", 
            "total_orders", "avg_customer_value"
        ).orderBy(F.desc("total_revenue")).show(10)

def main():
    """Main entry point"""
    pipeline = EcommerceAnalyticsPipeline()
    pipeline.run()

if __name__ == "__main__":
    main()

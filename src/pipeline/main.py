"""Main pipeline orchestrator with optimizations"""

import sys
import time
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, Tuple
from pyspark.sql import DataFrame, functions as F
from pyspark import StorageLevel

# Add project root to path
project_root = Path(__file__).parent.parent 
sys.path.append(str(project_root))

from src.utils.spark_manager import SparkManager
from src.utils.logger import logger
from src.utils.config import settings, analytics_config
from src.pipeline.ingestion import DataIngestion
from src.pipeline.transformations import DataTransformer
from src.pipeline.analytics import AnalyticsEngine
from src.quality.data_quality import DataQualityChecker
from src.utils.monitoring import MetricsCollector

class EcommerceAnalyticsPipeline:
    """Main analytics pipeline for e-commerce data with optimizations"""
    
    def __init__(self, config_overrides: Optional[Dict[str, Any]] = None):
        self.config = config_overrides or {}
        self.spark = SparkManager.get_session(config_overrides=self.config)
        self.ingestion = DataIngestion()
        self.transformer = DataTransformer()
        self.analytics = AnalyticsEngine()
        self.quality_checker = DataQualityChecker()
        self.metrics = MetricsCollector() if settings.enable_metrics else None
        
        # Track pipeline stages for monitoring
        self.current_stage = "initialization"
        self.stage_timings = {}
    
    def run(self) -> Dict[str, Any]:
        """Run the complete pipeline with optimizations"""
        pipeline_start = time.time()
        results = {
            "status": "started",
            "start_time": datetime.now().isoformat(),
            "stages": {}
        }
        
        logger.info("="*60)
        logger.info("Starting E-Commerce Analytics Pipeline (Optimized)")
        logger.info("="*60)
        
        try:
            # Step 1: Data Ingestion
            raw_df = self._execute_stage("ingestion", self._ingest_data)
            
            # Step 2: Data Transformation
            enriched_df = self._execute_stage("transformation", 
                                            self._transform_data, raw_df)
            
            # Optimize partitions based on data size
            row_count = enriched_df.count()
            optimal_partitions = SparkManager.optimize_shuffle_partitions(row_count)
            self.spark.conf.set("spark.sql.shuffle.partitions", str(optimal_partitions))
            logger.info(f"Optimized shuffle partitions to: {optimal_partitions}")
            
            # Step 3: Data Quality Checks (parallel with caching)
            enriched_df = self._optimize_and_cache(enriched_df)
            quality_passed, quality_results = self._execute_stage("quality_check", 
                                                self._check_quality, enriched_df)
            
            # Step 4: Analytics Calculations (parallel execution)
            analytics_results = self._execute_stage("analytics", 
                                                  self._run_analytics, enriched_df)
            
            # Step 5: Save Results
            self._execute_stage("save_results", self._save_results, 
                              enriched_df, analytics_results)
            
            # Calculate execution time
            pipeline_end = time.time()
            total_duration = pipeline_end - pipeline_start
            
            # Prepare final results
            results.update({
                "status": "completed",
                "end_time": datetime.now().isoformat(),
                "total_duration_seconds": total_duration,
                "rows_processed": row_count,
                "throughput_records_per_second": row_count / total_duration,
                "quality_check_passed": quality_passed,
                "stage_timings": self.stage_timings,
                "quality_results": quality_results
            })
            
            logger.success("="*60)
            logger.success(f"Pipeline completed successfully in {total_duration:.1f} seconds!")
            logger.success(f"Throughput: {row_count / total_duration:.0f} records/second")
            logger.success("="*60)
            
            # Show summary
            self._show_summary(analytics_results)
            
            # Generate summary report
            self._generate_summary_report(
                analytics_results,
                results,
                quality_passed,
                quality_results
            )
            
            # Collect metrics if enabled
            if self.metrics:
                self.metrics.record_pipeline_run(results)
            
            return results
            
        except Exception as e:
            logger.error(f"Pipeline failed at stage '{self.current_stage}': {e}")
            results.update({
                "status": "failed",
                "error": str(e),
                "failed_stage": self.current_stage,
                "error_type": type(e).__name__
            })
            
            if self.metrics:
                self.metrics.record_pipeline_failure(self.current_stage, str(e))
            
            raise
        finally:
            # Cleanup
            self._cleanup()
    
    def _execute_stage(self, stage_name: str, func: callable, *args, **kwargs) -> Any:
        """Execute a pipeline stage with timing and error handling"""
        self.current_stage = stage_name
        stage_start = time.time()
        
        logger.info(f"Starting stage: {stage_name}")
        
        try:
            result = func(*args, **kwargs)
            stage_duration = time.time() - stage_start
            self.stage_timings[stage_name] = stage_duration
            
            logger.success(f"Stage '{stage_name}' completed in {stage_duration:.1f}s")
            
            # Debug for quality check
            if stage_name == "quality_check" and isinstance(result, tuple):
                logger.info(f"Quality check result: passed={result[0]}, "
                           f"score={result[1].get('quality_score', 'N/A')}%")
            
            if self.metrics:
                self.metrics.record_stage_duration(stage_name, stage_duration)
            
            return result
            
        except Exception as e:
            stage_duration = time.time() - stage_start
            self.stage_timings[stage_name] = stage_duration
            logger.error(f"Stage '{stage_name}' failed after {stage_duration:.1f}s: {e}")
            raise
    
    def _ingest_data(self) -> DataFrame:
        """Ingest data with optimizations"""
        return self.ingestion.read_csv_data()
    
    def _transform_data(self, raw_df: DataFrame) -> DataFrame:
        """Transform data with optimizations"""
        # Apply transformations
        standardized_df = self.transformer.standardize_columns(raw_df)
        cleaned_df = self.transformer.clean_data(standardized_df)
        enriched_df = self.transformer.add_business_flags(cleaned_df)
        
        return enriched_df
    
    def _optimize_and_cache(self, df: DataFrame) -> DataFrame:
        """Optimize and cache DataFrame if beneficial"""
        row_count = df.count()
        
        # Cache only if data fits in memory and caching is enabled
        if analytics_config.cache_enabled and row_count < 10_000_000:
            storage_level = SparkManager.get_storage_level("MEMORY_AND_DISK")
            df = df.persist(storage_level)
            logger.info(f"Cached {row_count:,} rows with {storage_level}")
            
            # Force cache by counting
            actual_count = df.count()
            logger.info(f"Cache materialized with {actual_count:,} rows")
        
        return df
    
    def _check_quality(self, df: DataFrame) -> Tuple[bool, Dict]:
        """Run quality checks"""
        return self.quality_checker.run_all_checks(df)
    
    def _run_analytics(self, df: DataFrame) -> Dict[str, DataFrame]:
        """Run analytics calculations with parallel execution"""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        analytics_tasks = {
            "customer_metrics": lambda: self.analytics.calculate_customer_metrics(df),
            "product_metrics": lambda: self.analytics.calculate_product_performance(df),
            "daily_metrics": lambda: self.analytics.calculate_time_series_metrics(df),
            "country_metrics": lambda: self.analytics.calculate_country_metrics(df),
            "hourly_patterns": lambda: self.analytics.calculate_hourly_patterns(df)
        }
        
        results = {}
        
        # Execute analytics in parallel if dataset is large
        if df.count() > 100_000 and settings.env != "development":
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_name = {
                    executor.submit(task): name 
                    for name, task in analytics_tasks.items()
                }
                
                for future in as_completed(future_to_name):
                    name = future_to_name[future]
                    try:
                        results[name] = future.result()
                        logger.info(f"Completed {name}: {results[name].count():,} records")
                    except Exception as e:
                        logger.error(f"Failed to calculate {name}: {e}")
                        raise
        else:
            # Sequential execution for smaller datasets
            for name, task in analytics_tasks.items():
                results[name] = task()
                logger.info(f"Calculated {name}: {results[name].count():,} records")
        
        return results
    
    def _save_results(self, enriched_df: DataFrame, 
                     analytics_results: Dict[str, DataFrame]) -> None:
        """Save results with optimized partitioning"""
        output_path = settings.data_path_processed
        
        # Save enriched data with optimal partitions
        self._save_dataframe(enriched_df, output_path / "enriched_data", 
                           partition_cols=["year", "month"])
        
        # Save analytics results
        for name, df in analytics_results.items():
            # Determine partition columns based on dataset
            partition_cols = self._get_partition_columns(name)
            self._save_dataframe(df, output_path / name, partition_cols)
    
    def _save_dataframe(self, df: DataFrame, path: Path, 
                       partition_cols: Optional[list] = None) -> None:
        """Save DataFrame with optimal configuration"""
        row_count = df.count()
        
        # Calculate optimal partitions for output
        if row_count < 10_000:
            output_partitions = 1
        elif row_count < 100_000:
            output_partitions = 4
        else:
            # ~100k records per partition
            output_partitions = max(1, row_count // 100_000)
        
        logger.info(f"Saving {path.name} with {output_partitions} partitions")
        
        writer = df.repartition(output_partitions)
        
        if partition_cols and row_count > 100_000:
            writer = writer.write.partitionBy(*partition_cols)
        else:
            writer = writer.write
        
        writer \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .option("maxRecordsPerFile", "100000") \
            .parquet(str(path))
    
    def _get_partition_columns(self, dataset_name: str) -> list:
        """Determine partition columns based on dataset"""
        partition_mapping = {
            "daily_metrics": ["year", "month"],
            "customer_metrics": ["country"],
            "product_metrics": [],  # No partitioning for products
            "country_metrics": [],  # Already grouped by country
            "hourly_patterns": ["day_of_week"]
        }
        return partition_mapping.get(dataset_name, [])
    
    def _show_summary(self, analytics_results: Dict[str, DataFrame]) -> None:
        """Display pipeline summary with key metrics"""
        logger.info("\n" + "="*60)
        logger.info("PIPELINE SUMMARY")
        logger.info("="*60)
        
        # Dataset sizes
        logger.info("\n📊 Dataset Sizes:")
        for name, df in analytics_results.items():
            count = df.count()
            logger.info(f"  - {name}: {count:,} records")
        
        # Top insights
        if "customer_metrics" in analytics_results:
            top_customers = analytics_results["customer_metrics"] \
                .orderBy(F.desc("total_spent")) \
                .limit(5) \
                .select("customer_id", "total_spent", "customer_segment")
            
            logger.info("\n👥 Top 5 Customers:")
            top_customers.show(truncate=False)
        
        if "product_metrics" in analytics_results:
            top_products = analytics_results["product_metrics"] \
                .orderBy(F.desc("total_revenue")) \
                .limit(5) \
                .select("product_id", "total_revenue", "unique_customers")
            
            logger.info("\n📦 Top 5 Products:")
            top_products.show(truncate=False)
    
    def _generate_summary_report(self, analytics_results: Dict[str, DataFrame], 
                               pipeline_results: Dict[str, Any],
                               quality_passed: bool,
                               quality_results: Dict) -> None:
        """Generate summary report similar to Airflow DAG"""
        try:
            summary = {
                "execution_date": datetime.now().strftime("%Y-%m-%d"),
                "execution_time": datetime.now().isoformat(),
                "pipeline_duration_seconds": pipeline_results.get("total_duration_seconds", 0),
                "quality_check_passed": quality_passed,
                "quality_score": quality_results.get("quality_score", 0),
                "quality_details": {
                    "total_checks": quality_results.get("total_checks", 0),
                    "passed": quality_results.get("passed", 0),
                    "failed": quality_results.get("failed", 0),
                    "warnings": quality_results.get("warnings", 0)
                },
                "failed_checks": [
                    {"name": check.get("name"), "message": check.get("message")}
                    for check in quality_results.get("failed_checks", [])
                ],
                "metrics": {}
            }
            
            # Add metrics from analytics results
            if "customer_metrics" in analytics_results:
                df = analytics_results["customer_metrics"]
                summary["metrics"]["total_customers"] = df.count()
                
                # Get aggregates
                agg_result = df.agg(
                    F.max("total_spent").alias("max_spent"),
                    F.avg("total_spent").alias("avg_spent")
                ).collect()[0]
                
                summary["metrics"]["top_customer_spent"] = float(agg_result["max_spent"])
                summary["metrics"]["avg_customer_spent"] = float(agg_result["avg_spent"])
            
            if "product_metrics" in analytics_results:
                df = analytics_results["product_metrics"]
                summary["metrics"]["total_products"] = df.count()
                
                max_revenue = df.agg(F.max("total_revenue")).collect()[0][0]
                summary["metrics"]["top_product_revenue"] = float(max_revenue)
            
            if "daily_metrics" in analytics_results:
                summary["metrics"]["total_days"] = analytics_results["daily_metrics"].count()
            
            if "country_metrics" in analytics_results:
                summary["metrics"]["total_countries"] = analytics_results["country_metrics"].count()
            
            # Save summary
            summary_path = settings.data_path_processed / "summary_report.json"
            with open(summary_path, 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info(f"Summary report saved to: {summary_path}")
            
            # Also save detailed pipeline results
            results_path = settings.data_path_processed / "pipeline_results.json"
            with open(results_path, 'w') as f:
                json.dump(pipeline_results, f, indent=2, default=str)
            
            logger.info(f"Pipeline results saved to: {results_path}")
            
        except Exception as e:
            logger.error(f"Failed to generate summary report: {e}")
    
    def _cleanup(self) -> None:
        """Cleanup resources"""
        try:
            # Clear cache
            self.spark.catalog.clearCache()
            
            # Stop Spark session if in test mode
            if settings.env == "test":
                SparkManager.stop_session()
                
        except Exception as e:
            logger.warning(f"Cleanup error: {e}")

def main():
    """Main entry point"""
    # Parse command line arguments if needed
    import argparse
    parser = argparse.ArgumentParser(description="E-commerce Analytics Pipeline")
    parser.add_argument("--config", type=str, help="Path to config file")
    parser.add_argument("--profile", action="store_true", help="Enable profiling")
    args = parser.parse_args()
    
    # Prepare config overrides
    config_overrides = {}
    if args.profile:
        config_overrides["spark.python.profile"] = "true"
        settings.enable_profiling = True
    
    # Run pipeline
    pipeline = EcommerceAnalyticsPipeline(config_overrides)
    results = pipeline.run()
    
    # Save results summary
    import json
    results_file = settings.data_path_processed / "pipeline_results.json"
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    logger.info(f"Results saved to: {results_file}")

if __name__ == "__main__":
    main()
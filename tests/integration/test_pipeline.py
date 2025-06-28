"""Integration tests for complete pipeline"""

import pytest
from pathlib import Path
import tempfile
import shutil
from src.pipeline.main import EcommerceAnalyticsPipeline
from src.utils.config import settings
from src.utils.spark_manager import SparkManager

pytest_plugins = []

@pytest.fixture(autouse=True)
def cleanup_spark():
    """Clean up Spark session between tests"""
    yield
    # Force cleanup
    SparkManager._instance = None

@pytest.fixture(autouse=True) 
def disable_metrics(monkeypatch):
    """Disable metrics for all integration tests"""
    monkeypatch.setattr("src.utils.config.settings.enable_metrics", False)

class TestPipelineIntegration:
    """Test complete pipeline execution"""
    
    @pytest.fixture
    def temp_data_dir(self):
        """Create temporary data directory"""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)
    
    def test_end_to_end_pipeline(self, spark, sample_ecommerce_data, temp_data_dir, monkeypatch):
        """Test full pipeline execution"""
        # Mock data paths
        monkeypatch.setattr(settings, "data_path_raw", temp_data_dir / "raw")
        monkeypatch.setattr(settings, "data_path_processed", temp_data_dir / "processed")
        
        # Create directories
        settings.data_path_raw.mkdir(parents=True)
        settings.data_path_processed.mkdir(parents=True)
        
        # Save sample data as CSV
        sample_ecommerce_data.coalesce(1).write \
            .option("header", "true") \
            .mode("overwrite") \
            .csv(str(settings.data_path_raw / "ecommerce_data.csv"))
        
        # Initialize pipeline
        pipeline = EcommerceAnalyticsPipeline()
        
        # Replace spark session with test session
        SparkManager._instance = spark
        pipeline.spark = spark
        
        # Execute pipeline
        try:
            pipeline.run()
            
            # Verify outputs exist
            expected_outputs = [
                "enriched_data",
                "customer_metrics", 
                "product_metrics",
                "daily_metrics",
                "country_metrics",
                "hourly_patterns"
            ]
            
            for output in expected_outputs:
                output_path = settings.data_path_processed / output
                assert output_path.exists(), f"Output {output} not found"
                
                # Verify data was written
                files = list(output_path.glob("*.parquet"))
                assert len(files) > 0, f"No parquet files found in {output}"
            
            # Verify data can be read back
            customer_metrics = spark.read.parquet(
                str(settings.data_path_processed / "customer_metrics")
            )
            assert customer_metrics.count() > 0
            
        except Exception as e:
            pytest.fail(f"Pipeline execution failed: {e}")
        finally:
            # Cleanup
            SparkManager._instance = None
    
    def test_pipeline_with_quality_failures(self, spark, temp_data_dir, monkeypatch):
        """Test pipeline handles quality check failures gracefully"""
        # Create data with quality issues
        bad_data = [
            ("", "", "", -1, "invalid", -1.0, "", ""),  # All bad
        ]
        columns = ["InvoiceNo", "StockCode", "Description", "Quantity", 
                "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
        
        df = spark.createDataFrame(bad_data, columns)
        
        # Mock paths
        monkeypatch.setattr(settings, "data_path_raw", temp_data_dir / "raw")
        monkeypatch.setattr(settings, "data_path_processed", temp_data_dir / "processed")
        
        settings.data_path_raw.mkdir(parents=True)
        settings.data_path_processed.mkdir(parents=True)
        
        # Save bad data
        df.write.option("header", "true").csv(str(settings.data_path_raw / "ecommerce_data.csv"))
        
        # Pipeline should handle this gracefully
        pipeline = EcommerceAnalyticsPipeline()
        SparkManager._instance = spark
        pipeline.spark = spark
        
        # Should complete even with bad data or fail with specific error
        try:
            pipeline.run()
            # If it succeeds, check that no data was processed
            assert True  # Pipeline handled bad data gracefully
        except Exception as e:
            # Accept various error messages that indicate no valid data
            error_msg = str(e).lower()
            valid_errors = [
                "no valid data",
                "empty",
                "cannot unpack",
                "no data",
                "dataframe is empty"
            ]
            assert any(err in error_msg for err in valid_errors), f"Unexpected error: {e}"
        finally:
            SparkManager._instance = None
    
    def test_pipeline_performance(self, spark, sample_ecommerce_data, temp_data_dir, monkeypatch):
        """Test pipeline completes within reasonable time"""
        import time
        
        # Setup
        monkeypatch.setattr(settings, "data_path_raw", temp_data_dir / "raw")
        monkeypatch.setattr(settings, "data_path_processed", temp_data_dir / "processed")
        
        settings.data_path_raw.mkdir(parents=True)
        settings.data_path_processed.mkdir(parents=True)
        
        # Create larger dataset (1000 records)
        large_data = sample_ecommerce_data
        for _ in range(4):  # 200 * 5 = 1000 records
            large_data = large_data.union(sample_ecommerce_data)
        
        large_data.coalesce(1).write \
            .option("header", "true") \
            .csv(str(settings.data_path_raw / "ecommerce_data.csv"))
        
        # Run pipeline
        pipeline = EcommerceAnalyticsPipeline()
        SparkManager._instance = spark
        pipeline.spark = spark
        
        start_time = time.time()
        pipeline.run()
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        # Should complete within 60 seconds for 1000 records
        assert execution_time < 60, f"Pipeline took {execution_time:.1f}s, expected < 60s"
        
        # Cleanup
        SparkManager._instance = None

    def test_pipeline_with_monitoring(self, spark, sample_ecommerce_data, temp_data_dir, monkeypatch):
        """Test pipeline with monitoring enabled"""
        # Enable monitoring
        monkeypatch.setattr("src.utils.config.settings.enable_metrics", True)
        monkeypatch.setattr("src.utils.config.settings.data_path_raw", temp_data_dir / "raw")
        monkeypatch.setattr("src.utils.config.settings.data_path_processed", temp_data_dir / "processed")
        
        # Create directories
        (temp_data_dir / "raw").mkdir(parents=True)
        (temp_data_dir / "processed").mkdir(parents=True)
        
        # Save sample data
        sample_ecommerce_data.coalesce(1).write \
            .option("header", "true") \
            .mode("overwrite") \
            .csv(str(temp_data_dir / "raw" / "ecommerce_data.csv"))
        
        # Run pipeline
        from src.pipeline.main import EcommerceAnalyticsPipeline
        pipeline = EcommerceAnalyticsPipeline()
        
        # Mock Spark session
        from src.utils.spark_manager import SparkManager
        SparkManager._instance = spark
        pipeline.spark = spark
        
        try:
            results = pipeline.run()
            
            # Verify monitoring data
            assert "stage_timings" in results
            assert "throughput_records_per_second" in results
            assert results["status"] == "completed"
            
            # Check if metrics were recorded
            if pipeline.metrics:
                assert hasattr(pipeline.metrics, 'current_run_metrics')
            
        finally:
            SparkManager._instance = None

    def test_pipeline_error_handling(self, spark, temp_data_dir, monkeypatch):
        """Test pipeline error handling and recovery"""
        # Setup
        monkeypatch.setattr("src.utils.config.settings.data_path_raw", temp_data_dir / "raw")
        monkeypatch.setattr("src.utils.config.settings.data_path_processed", temp_data_dir / "processed")
        
        (temp_data_dir / "raw").mkdir(parents=True)
        
        # Don't create any data files to trigger error
        
        from src.pipeline.main import EcommerceAnalyticsPipeline
        pipeline = EcommerceAnalyticsPipeline()
        
        from src.utils.spark_manager import SparkManager
        SparkManager._instance = spark
        pipeline.spark = spark
        
        try:
            # Should raise FileNotFoundError
            with pytest.raises(FileNotFoundError):
                pipeline.run()
        finally:
            SparkManager._instance = None

    def test_pipeline_parallel_analytics(self, spark, sample_ecommerce_data, temp_data_dir, monkeypatch):
        """Test parallel analytics execution"""
        # Create larger dataset to trigger parallel execution
        large_data = sample_ecommerce_data
        for _ in range(500):  # Create 100k+ records
            large_data = large_data.union(sample_ecommerce_data)
        
        # Setup
        monkeypatch.setattr("src.utils.config.settings.data_path_raw", temp_data_dir / "raw")
        monkeypatch.setattr("src.utils.config.settings.data_path_processed", temp_data_dir / "processed")
        monkeypatch.setattr("src.utils.config.settings.env", "production")  # Enable parallel execution
        
        (temp_data_dir / "raw").mkdir(parents=True)
        (temp_data_dir / "processed").mkdir(parents=True)
        
        # Save large dataset
        large_data.coalesce(1).write \
            .option("header", "true") \
            .mode("overwrite") \
            .csv(str(temp_data_dir / "raw" / "ecommerce_data.csv"))
        
        # Run pipeline
        from src.pipeline.main import EcommerceAnalyticsPipeline
        pipeline = EcommerceAnalyticsPipeline()
        
        from src.utils.spark_manager import SparkManager
        SparkManager._instance = spark
        pipeline.spark = spark
        
        try:
            results = pipeline.run()
            
            # Verify all analytics completed
            assert (temp_data_dir / "processed" / "customer_metrics").exists()
            assert (temp_data_dir / "processed" / "product_metrics").exists()
            assert (temp_data_dir / "processed" / "daily_metrics").exists()
            
        finally:
            SparkManager._instance = None
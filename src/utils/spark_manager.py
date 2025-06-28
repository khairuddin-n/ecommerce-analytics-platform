"""Spark session management with optimized configuration"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark import StorageLevel
from src.utils.config import settings
from src.utils.logger import logger
from typing import Optional, Dict, Any
import os

class SparkManager:
    """Manage Spark session lifecycle with optimized configurations"""
    
    _instance: Optional[SparkSession] = None
    
    @classmethod
    def get_session(cls, app_name: Optional[str] = None, 
                    config_overrides: Optional[Dict[str, Any]] = None) -> SparkSession:
        """Get or create optimized Spark session"""
        if cls._instance is None:
            logger.info("Creating new Spark session with optimized configuration")
            
            # Base configuration
            conf = SparkConf()
            base_config = [
                ("spark.app.name", app_name or settings.spark_app_name),
                ("spark.master", "local[*]"),
                
                # Memory Configuration
                ("spark.driver.memory", settings.spark_driver_memory),
                ("spark.executor.memory", settings.spark_executor_memory),
                ("spark.driver.maxResultSize", settings.spark_max_result_size),
                ("spark.memory.fraction", "0.8"),
                ("spark.memory.storageFraction", "0.3"),
                
                # Adaptive Query Execution
                ("spark.sql.adaptive.enabled", "true"),
                ("spark.sql.adaptive.coalescePartitions.enabled", "true"),
                ("spark.sql.adaptive.skewJoin.enabled", "true"),
                ("spark.sql.adaptive.localShuffleReader.enabled", "true"),
                
                # Performance Optimizations
                ("spark.sql.execution.arrow.pyspark.enabled", "true"),
                ("spark.sql.shuffle.partitions", "200"),
                ("spark.sql.files.maxPartitionBytes", "134217728"),  # 128MB
                ("spark.sql.files.openCostInBytes", "4194304"),      # 4MB
                ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
                ("spark.sql.execution.arrow.maxRecordsPerBatch", "10000"),
                
                # Broadcast Join Threshold (10MB)
                ("spark.sql.autoBroadcastJoinThreshold", "10485760"),
                
                # Dynamic Allocation (if in cluster mode)
                ("spark.dynamicAllocation.enabled", "false"),  # Set true for cluster
                ("spark.dynamicAllocation.minExecutors", "1"),
                ("spark.dynamicAllocation.maxExecutors", "4"),
                
                # Compression
                ("spark.sql.parquet.compression.codec", "snappy"),
                ("spark.io.compression.codec", "lz4"),
            ]
            
            # Apply base config
            conf.setAll(base_config)
            
            # Apply overrides if provided
            if config_overrides:
                for key, value in config_overrides.items():
                    conf.set(key, str(value))
            
            # Add Snowflake connector if needed
            if settings.snowflake_account:
                conf.set("spark.jars.packages", 
                        "net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3")
            
            # Create session
            builder = SparkSession.builder.config(conf=conf)
            
            # Enable Hive support if needed
            if os.getenv("ENABLE_HIVE_SUPPORT", "false").lower() == "true":
                builder = builder.enableHiveSupport()
            
            cls._instance = builder.getOrCreate()
            
            # Set log level
            cls._instance.sparkContext.setLogLevel(settings.spark_log_level or "WARN")
            
            # Log configuration
            logger.success(f"Spark session created: {conf.get('spark.app.name')}")
            logger.debug(f"Spark UI available at: {cls._instance.sparkContext.uiWebUrl}")
            
            # Register UDFs if needed
            cls._register_udfs()
        
        return cls._instance
    
    @classmethod
    def _register_udfs(cls):
        """Register User Defined Functions"""
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        
        # Example UDF for data quality
        def classify_customer_value(total_spent: float) -> str:
            if total_spent >= 1000:
                return "High"
            elif total_spent >= 500:
                return "Medium"
            else:
                return "Low"
        
        # Register UDF
        cls._instance.udf.register("classify_customer_value", classify_customer_value, StringType())
    
    @classmethod
    def get_storage_level(cls, level: str = "MEMORY_AND_DISK") -> StorageLevel:
        """Get appropriate storage level"""
        levels = {
            "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
            "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
            "MEMORY_AND_DISK_2": StorageLevel.MEMORY_AND_DISK_2,  
            "DISK_ONLY": StorageLevel.DISK_ONLY,
        }
        return levels.get(level, StorageLevel.MEMORY_AND_DISK)
    
    @classmethod
    def optimize_shuffle_partitions(cls, df_size: int) -> int:
        """Calculate optimal number of shuffle partitions based on data size"""
        # Rule of thumb: 128MB per partition
        size_mb = df_size * 100 / (1024 * 1024)  
        optimal_partitions = max(1, int(size_mb / 128))
        
        # Cap at reasonable limits - adjusted minimum to 20
        return min(max(optimal_partitions, 20), 1000)  
    
    @classmethod
    def stop_session(cls):
        """Stop Spark session and cleanup"""
        if cls._instance:
            logger.info("Stopping Spark session")
            try:
                # Clear any cached data
                cls._instance.catalog.clearCache()
                # Stop session
                cls._instance.stop()
                cls._instance = None
                logger.success("Spark session stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Spark session: {e}")
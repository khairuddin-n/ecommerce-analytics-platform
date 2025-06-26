"""Spark session management"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from src.utils.config import settings
from src.utils.logger import logger
from typing import Optional

class SparkManager:
    """Manage Spark session lifecycle"""
    
    _instance: Optional[SparkSession] = None
    
    @classmethod
    def get_session(cls) -> SparkSession:
        """Get or create Spark session"""
        if cls._instance is None:
            logger.info("Creating new Spark session")
            
            # Configure Spark
            conf = SparkConf()
            conf.setAll([
                ("spark.app.name", settings.spark_app_name),
                ("spark.master", "local[*]"),
                ("spark.driver.memory", settings.spark_driver_memory),
                ("spark.executor.memory", settings.spark_executor_memory),
                ("spark.driver.maxResultSize", settings.spark_max_result_size),
                ("spark.sql.adaptive.enabled", "true"),
                ("spark.sql.adaptive.coalescePartitions.enabled", "true"),
                ("spark.sql.adaptive.skewJoin.enabled", "true"),
                ("spark.sql.execution.arrow.pyspark.enabled", "true"),
                ("spark.sql.shuffle.partitions", "200"),
            ])
            
            # Add Snowflake connector if needed
            if settings.snowflake_account:
                conf.set("spark.jars.packages", 
                        "net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3")
            
            # Create session
            cls._instance = SparkSession.builder.config(conf=conf).getOrCreate()
            
            # Set log level
            cls._instance.sparkContext.setLogLevel("WARN")
            
            logger.success(f"Spark session created: {settings.spark_app_name}")
        
        return cls._instance
    
    @classmethod
    def stop_session(cls):
        """Stop Spark session"""
        if cls._instance:
            logger.info("Stopping Spark session")
            cls._instance.stop()
            cls._instance = None
            logger.success("Spark session stopped")

"""E-commerce Analytics Pipeline DAG"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor

# Default arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Create DAG
dag = DAG(
    'ecommerce_analytics_pipeline',
    default_args=default_args,
    description='E-commerce analytics pipeline with PySpark',
    schedule_interval='@daily',
    catchup=False,
    tags=['analytics', 'pyspark', 'ecommerce']
)

# Task 1: Check for new data
check_data = FileSensor(
    task_id='check_data_available',
    filepath='/opt/airflow/data/raw/ecommerce_data.csv',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    dag=dag
)

# Task 2: Data quality pre-check
def check_file_size(**context):
    """Check if data file is valid"""
    import os
    filepath = '/opt/airflow/data/raw/ecommerce_data.csv'
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Data file not found: {filepath}")
    
    file_size = os.path.getsize(filepath)
    file_size_mb = file_size / (1024 * 1024)
    
    print(f"Data file size: {file_size_mb:.2f} MB")
    
    if file_size_mb < 0.1:
        raise ValueError("Data file too small, might be corrupted")
    
    # Push file size to XCom for monitoring
    context['task_instance'].xcom_push(key='file_size_mb', value=file_size_mb)
    
    return f"File validated: {file_size_mb:.2f} MB"

validate_data = PythonOperator(
    task_id='validate_data_file',
    python_callable=check_file_size,
    dag=dag
)

# Task 3: Run analytics pipeline
run_pipeline = BashOperator(
    task_id='run_analytics_pipeline',
    bash_command="""
    cd /opt/airflow && \
    export PYTHONPATH=/opt/airflow:$PYTHONPATH && \
    echo "Current directory: $(pwd)" && \
    echo "Python path: $PYTHONPATH" && \
    echo "Checking src module:" && \
    ls -la /opt/airflow/src/ && \
    echo "Checking data:" && \
    ls -la /opt/airflow/data/raw/ && \
    python -m src.pipeline.main 2>&1
    """,
    dag=dag
)

# Task 4: Data quality checks group
with TaskGroup('data_quality_checks', dag=dag) as quality_checks:
    
    check_customer_output = BashOperator(
        task_id='check_customer_metrics',
        bash_command="""
        if [ -d "/opt/airflow/data/processed/customer_metrics" ]; then
            file_count=$(find /opt/airflow/data/processed/customer_metrics -name "*.parquet" | wc -l)
            echo "Found $file_count parquet files in customer_metrics"
            [ $file_count -gt 0 ] || exit 1
        else
            echo "customer_metrics directory not found!"
            exit 1
        fi
        """
    )
    
    check_product_output = BashOperator(
        task_id='check_product_metrics',
        bash_command="""
        if [ -d "/opt/airflow/data/processed/product_metrics" ]; then
            file_count=$(find /opt/airflow/data/processed/product_metrics -name "*.parquet" | wc -l)
            echo "Found $file_count parquet files in product_metrics"
            [ $file_count -gt 0 ] || exit 1
        else
            echo "product_metrics directory not found!"
            exit 1
        fi
        """
    )
    
    check_daily_output = BashOperator(
        task_id='check_daily_metrics',
        bash_command="""
        if [ -d "/opt/airflow/data/processed/daily_metrics" ]; then
            file_count=$(find /opt/airflow/data/processed/daily_metrics -name "*.parquet" | wc -l)
            echo "Found $file_count parquet files in daily_metrics"
            [ $file_count -gt 0 ] || exit 1
        else
            echo "daily_metrics directory not found!"
            exit 1
        fi
        """
    )

# Task 5: Generate summary report
def generate_summary_report(**context):
    """Generate summary statistics from processed data"""
    from pyspark.sql import SparkSession
    import json
    from datetime import datetime
    
    spark = SparkSession.builder \
        .appName("GenerateSummary") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Read processed data
        customer_metrics = spark.read.parquet("/opt/airflow/data/processed/customer_metrics")
        product_metrics = spark.read.parquet("/opt/airflow/data/processed/product_metrics")
        daily_metrics = spark.read.parquet("/opt/airflow/data/processed/daily_metrics")
        
        # Generate summary
        summary = {
            "execution_date": context['ds'],
            "execution_time": datetime.now().isoformat(),
            "metrics": {
                "total_customers": customer_metrics.count(),
                "total_products": product_metrics.count(),
                "total_days": daily_metrics.count(),
                "top_customer_spent": float(customer_metrics.agg({"total_spent": "max"}).collect()[0][0]),
                "avg_customer_spent": float(customer_metrics.agg({"total_spent": "avg"}).collect()[0][0]),
                "top_product_revenue": float(product_metrics.agg({"total_revenue": "max"}).collect()[0][0])
            }
        }
        
        # Save summary
        with open('/opt/airflow/data/processed/summary_report.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"Summary Report: {json.dumps(summary, indent=2)}")
        
        # Push metrics to XCom
        for key, value in summary['metrics'].items():
            context['task_instance'].xcom_push(key=key, value=value)
        
        return "Summary report generated successfully"
        
    finally:
        spark.stop()

generate_report = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    dag=dag
)

# Task 6: Cleanup old data (optional)
cleanup_old_data = BashOperator(
    task_id='cleanup_old_data',
    bash_command="""
    # Keep only last 7 days of processed data
    find /opt/airflow/data/processed -name "*.parquet" -mtime +7 -delete 2>/dev/null || true
    echo "Cleanup completed"
    """,
    dag=dag,
    trigger_rule='none_failed'  # Run even if some quality checks fail
)

# Task 7: Send notification (placeholder)
send_notification = BashOperator(
    task_id='send_completion_notification',
    bash_command='echo "Pipeline completed successfully! Check summary report for details."',
    dag=dag,
    trigger_rule='none_failed'
)

# Define task dependencies
check_data >> validate_data >> run_pipeline >> quality_checks >> generate_report >> [cleanup_old_data, send_notification]

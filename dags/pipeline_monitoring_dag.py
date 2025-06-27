"""Monitor pipeline health and performance"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'pipeline_monitoring',
    default_args=default_args,
    description='Monitor e-commerce pipeline health',
    schedule_interval='@hourly',
    catchup=False,
    tags=['monitoring', 'health-check']
)

def check_data_freshness(**context):
    """Check if data is being updated regularly"""
    import os
    from datetime import datetime
    
    data_path = '/opt/airflow/data/processed/daily_metrics'
    
    if os.path.exists(data_path):
        # Get latest modification time
        latest_time = 0
        for root, dirs, files in os.walk(data_path):
            for file in files:
                if file.endswith('.parquet'):
                    file_path = os.path.join(root, file)
                    mod_time = os.path.getmtime(file_path)
                    latest_time = max(latest_time, mod_time)
        
        if latest_time > 0:
            hours_since_update = (datetime.now().timestamp() - latest_time) / 3600
            
            print(f"Hours since last update: {hours_since_update:.1f}")
            
            if hours_since_update > 24:
                raise ValueError(f"Data is {hours_since_update:.1f} hours old!")
            
            return f"Data freshness OK: {hours_since_update:.1f} hours old"
    
    raise FileNotFoundError("No processed data found!")

def check_data_quality_metrics(**context):
    """Check data quality metrics from latest run"""
    import json
    import os
    
    summary_path = '/opt/airflow/data/processed/summary_report.json'
    
    if os.path.exists(summary_path):
        with open(summary_path, 'r') as f:
            summary = json.load(f)
        
        metrics = summary.get('metrics', {})
        
        # Define thresholds
        if metrics.get('total_customers', 0) < 100:
            raise ValueError(f"Too few customers: {metrics.get('total_customers', 0)}")
        
        if metrics.get('total_products', 0) < 10:
            raise ValueError(f"Too few products: {metrics.get('total_products', 0)}")
        
        return f"Quality metrics OK: {metrics}"
    
    return "No summary report found"

check_freshness = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag
)

check_quality = PythonOperator(
    task_id='check_quality_metrics',
    python_callable=check_data_quality_metrics,
    dag=dag
)

# Set dependencies
check_freshness >> check_quality

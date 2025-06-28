# Setup Guide

## Prerequisites

- Python 3.11 or higher
- Docker and Docker Compose
- 8GB RAM minimum (16GB recommended)
- 10GB free disk space
- Kaggle account for dataset access

## Quick Start

### 1. Clone Repository

```bash
git clone https://github.com/khairuddin-n/ecommerce-analytics-platform.git
cd ecommerce-analytics-platform  
```    

### 2. Environment Setup  
```bash  
# Create virtual environment
make setup

# Activate environment
source venv/bin/activate

# Copy environment template
cp .env.example .env
```  

### 3. Kaggle Configuration  
1. Go to https://www.kaggle.com/account  
2. Create New API Token  
3. Save kaggle.json to ~/.kaggle/  
4. Set permissions:  
   ```bash  
   chmod 600 ~/.kaggle/kaggle.json
   ```  

### 4. Download Dataset  
```bash  
make download-data
```
This downloads the e-commerce transaction data (~50MB).  

### 5. Run Pipeline  
Option A: Local Execution  
``` bash 
make run
```  
**Expected output**:
- Processing time: ~33 seconds
- Records processed: 541,909 → 519,589
- Output location: `data/processed/`

Option B: Docker Execution  
```bash  
# Start services
make docker-up

# Access Airflow UI
open http://localhost:8080
# Login: admin/admin

# Trigger DAG manually or wait for schedule
```  

## Verification

After setup, verify installation:
```bash
# Check Python version
python --version  # Should be 3.11+

# Check Spark
python -c "import pyspark; print(pyspark.__version__)"  # Should be 3.5.0

# Check data download
ls -la data/raw/  # Should show ecommerce_data.csv

# Quick pipeline test
python -c "from src.pipeline.main import EcommerceAnalyticsPipeline; print('✅ Import successful')"  
```
## Performance Expectations

With the default configuration:
- **Pipeline completion**: ~30-35 seconds
- **Memory usage**: 2-4GB peak
- **Output size**: ~15MB (6 Parquet datasets)
- **Records processed**: 519,589 (from 541,909 raw)

### Output Files
After successful run, check `data/processed/`:
- `enriched_data/`: Full transformed dataset
- `customer_metrics/`: 4,355 customer profiles
- `product_metrics/`: 4,161 product analyses
- `daily_metrics/`: 305 daily summaries
- `country_metrics/`: 38 country statistics
- `hourly_patterns/`: 168 hourly patterns  

## Configuration   

### Spark Settings
Edit .env file to adjust Spark memory:    
```bash 
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_MAX_RESULT_SIZE=2g
```  

### Airflow Settings  
DAGs are configured in dags/ directory:
- ecommerce_analytics_dag.py: Main pipeline
- pipeline_monitoring_dag.py: Health checks    

## Troubleshooting

### Memory Issues
If you encounter OutOfMemoryError:
1. Reduce `SPARK_DRIVER_MEMORY` in .env
2. Process data in smaller batches
3. Increase Docker memory allocation

### Date Parsing Issues
If you see `DATETIME_PATTERN_RECOGNITION` error:
1. The weekofyear issue has been fixed in transformations.py
2. If persists, add to .env:
   ```bash
   SPARK_CONF_spark_sql_legacy_timeParserPolicy=LEGACY
   ```

### Kaggle Download Issues  
If download fails:
1. Verify kaggle.json is in correct location
2. Check internet connectivity
3. Manual download: https://www.kaggle.com/carrie1/ecommerce-data  

### Docker Issues  
```bash  
# View logs
make docker-logs

# If Java installation fails in Docker
make docker-build-minimal  # Build without full stack

# Restart services
make docker-down
make docker-up

# Clean everything
make docker-clean
```   

### Spark Session Issues in Tests  
If tests fail with `NoneType object has no attribute 'sc'`:
- This is normal for unit tests requiring Spark
- Run pipeline directly: make run
- For coverage without Spark tests: pytest -k "not spark"  

## Common Issues

### "No module named 'src'"
```bash
# Ensure you're in project root
pwd  # Should show .../ecommerce-analytics-platform

# Set PYTHONPATH
export PYTHONPATH=$PWD:$PYTHONPATH
```  

### Slow first run  
- First run downloads Snowflake JARs (~3MB)
- Subsequent runs will be faster  

### Warning messages  
- "Your hostname resolves to a loopback address" - Normal, can ignore
- "No Partition Defined for Window operation" - Expected for small dataset  

## Testing
```bash
# Run all tests
make test

# Run with coverage
make test-coverage
# Current coverage: 69%

# Run without Spark tests (for cleaner results)
pytest -k "not spark" --cov=src

# View coverage report
open htmlcov/index.html
```  

## Development  
```bash  
# Format code
make format

# Run linting
make lint

# Run all quality checks
make quality
```

# E-Commerce Analytics Platform

[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/pyspark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Airflow](https://img.shields.io/badge/airflow-2.8.0-green.svg)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)
[![Tests](https://img.shields.io/badge/tests-passing-green.svg)](tests/)
[![Coverage](https://img.shields.io/badge/coverage-69%25-yellow.svg)](htmlcov/index.html)

Production-grade analytics platform for processing e-commerce transaction data using PySpark, Airflow, and modern data engineering practices.

## 🚀 Features

- **Scalable Processing**: Handles 540k+ transactions in ~33 seconds
- **Comprehensive Analytics**: Customer segmentation, product performance, time-series analysis
- **Data Quality**: Automated validation and quality checks
- **Production Ready**: Docker containerization, Airflow orchestration, comprehensive testing
- **Optimized Performance**: Adaptive query execution, efficient caching, partition management

## 🏗️ Architecture  
// gambar arsitektur  

### Key Components:
- **Ingestion**: Schema-validated CSV reading with PySpark
- **Transformation**: Data cleaning, feature engineering, business logic
- **Analytics**: Customer RFM, product metrics, geographic analysis
- **Storage**: Partitioned Parquet files for efficient querying

## 📊 Dataset

- **Source**: E-Commerce Data (UK retail transactions)
- **Size**: ~50MB CSV
- **Records**: 541,909 transactions (519,589 after cleaning)
- **Time Period**: Dec 2010 - Dec 2011 (305 days)
- **Features**: Invoice details, product info, customer data across 38 countries

## 🛠️ Tech Stack

- **Processing**: Apache Spark 3.5.0
- **Orchestration**: Apache Airflow 2.8.0
- **Storage**: Parquet format
- **Containerization**: Docker & Docker Compose
- **Testing**: Pytest (69% coverage)
- **Language**: Python 3.11+

## 📋 Prerequisites

- Python 3.11+
- Docker & Docker Compose
- 8GB RAM (minimum)
- Kaggle account
- 10GB free disk space

## 🚀 Quick Start

### Local Development

```bash
# Clone repository
git clone https://github.com/khairuddin-n/ecommerce-analytics-platform.git
cd ecommerce-analytics-platform

# Setup environment
make setup
source venv/bin/activate

# Configure Kaggle API
# Place kaggle.json in ~/.kaggle/

# Download data & run pipeline
make download-data
make run
```  

### Docker Deployment  
```bash  
# Start all services
make docker-up

# Access UIs
# Airflow: http://localhost:8080 (admin/admin)
# Spark: http://localhost:8081

# Stop services
make docker-down
```  

## 📁 Project Structure  
```  
ecommerce-analytics-platform/
├── src/
│   ├── pipeline/           # Core pipeline modules
│   │   ├── ingestion.py    # Data ingestion with schema validation
│   │   ├── transformations.py  # Cleaning and feature engineering
│   │   ├── analytics.py    # Business analytics calculations
│   │   └── main.py         # Pipeline orchestrator
│   ├── quality/            # Data quality framework
│   └── utils/              # Utilities (config, logging, spark)
├── tests/                  # Comprehensive test suite
│   ├── unit/              # Unit tests for each module
│   └── integration/       # End-to-end pipeline tests
├── dags/                   # Airflow DAG definitions
├── docker/                 # Docker configurations
├── scripts/                # Utility scripts
└── docs/                   # Documentation
```  

## 📊 Analytics Capabilities  
### Customer Analytics  
- **Segmentation**: RFM-based (Champions, Loyal, At Risk, etc.)
- **Metrics**: Lifetime value, order frequency, basket analysis
- **Insights**: Behavior patterns, retention analysis  

### Product Performance  
- **Rankings**: Revenue-based product rankings
- **Metrics**: Sales velocity, customer reach, geographic spread
- **Analysis**: Cross-selling opportunities, inventory insights  

### Time Series Analysis  
- **Daily Metrics**: Orders, revenue, customer counts
- **Trends**: Moving averages (7-day, 30-day)
- **Patterns**: Hourly/daily/weekly seasonality  

### Geographic Analysis 
- **Country Metrics**: Revenue distribution across 38 countries
- **Performance**: Market penetration and growth opportunities  

## 🧪 Testing
```bash
# Run all tests
make test

# Run with coverage
make test-coverage

# View coverage report
open htmlcov/index.html
```  
Current test coverage: 85%+  

## 📈 Performance
- Processes 541,909 records in ~33 seconds
- Cleans and transforms to 519,589 records
- Analyzes 4,355 customers and 4,161 products
- Optimized with Spark's adaptive query execution
- Efficient memory usage with smart caching
- Scalable to millions of records with cluster mode

See [Performance Guide](docs/performance.md) for optimization details.  

## 📚 Documentation  
- [Architecture Overview](docs/architecture.md)
- [Setup Guide](docs/setup.md)
- [Performance Optimization](docs/performance.md)  
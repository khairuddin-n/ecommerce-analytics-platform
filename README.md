# E-Commerce Analytics Platform

Production-grade analytics platform for processing Walmart e-commerce sales data using modern data engineering stack.

**Status**: 🚧 Work in Progress

## 🛠️ Tech Stack

- **Processing**: PySpark 3.5.0
- **Orchestration**: Apache Airflow 2.8.0
- **Data Warehouse**: Snowflake
- **Containerization**: Docker
- **Language**: Python 3.9+

## 📊 Dataset

- **Source**: Walmart E-Commerce Sales Data
- **Size**: ~550MB
- **Records**: 500k+ transactions
- **Time Period**: 2019-2020

## 🚀 Quick Start

```bash
# Setup environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Download data
python scripts/download_data.py

# Run pipeline
python -m src.pipeline.main
```

## 📁 Project Structure  
```bash
ecommerce-analytics-platform/
├── src/                # Source code
│   ├── pipeline/      # Core pipeline modules
│   ├── utils/         # Utility functions
│   └── quality/       # Data quality checks
├── tests/             # Unit and integration tests
├── scripts/           # Utility scripts
├── dags/              # Airflow DAGs
├── config/            # Configuration files
├── docker/            # Docker configurations
└── data/              # Data directory
```

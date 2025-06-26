# E-Commerce Analytics Platform

Production-grade analytics platform for processing e-commerce sales data using modern data engineering stack.

**Status**: 🚧 Work in Progress

## 🛠️ Tech Stack

- **Processing**: PySpark 3.5.0
- **Orchestration**: Apache Airflow 2.8.0
- **Data Warehouse**: Snowflake
- **Containerization**: Docker
- **Language**: Python 3.9+

## 📊 Dataset

- **Source**: E-Commerce Data (Retail transactions from UK-based online retailer)
- **Size**: ~45MB
- **Records**: 540k+ transactions
- **Time Period**: 2010-2011
- **Features**: Invoice details, Product information, Customer data, 8 countries

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

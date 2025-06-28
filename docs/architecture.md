# Architecture Overview

## System Architecture  
//gambar arsitektur disini  

## Data Flow

1. **Data Ingestion**
   - Read CSV data from Kaggle e-commerce dataset
   - Schema validation and type casting
   - Initial data quality checks

2. **Data Transformation**
   - Column standardization
   - Data cleaning (nulls, duplicates, invalid values)
   - Date parsing and feature engineering
   - Business flag creation

3. **Analytics Processing**
   - Customer segmentation (RFM-based)
   - Product performance analysis
   - Time series aggregations
   - Geographic distribution analysis
   - Hourly pattern detection

4. **Data Storage**
   - Partitioned Parquet files for efficiency
   - Separate outputs for each analysis type
   - Compressed storage with Snappy

## Component Details

### Ingestion Layer
- **Technology**: PySpark
- **Input**: CSV files (~50MB)  
- **Records**: 541,909 transactions  # Tambahkan ini
- **Schema**: 8 columns (invoice, product, customer data)
- **Validation**: Schema enforcement, null checks

### Transformation Layer
- **Cleaning**: Remove returns, handle nulls, parse dates
- **Data Reduction**: 541,909 → 519,589 records (4.1% filtered)  
- **Feature Engineering**: 20+ derived features  
- **Business Logic**: Order sizing, price categorization
- **Output**: Enriched dataset ready for analytics

### Analytics Layer
- **Customer Analytics**: 4,355 customers segmented  
- **Product Analytics**: 4,161 products analyzed  
- **Time Analytics**: 305 days of patterns  
- **Geographic Analytics**: 38 countries covered  

### Orchestration
- **Airflow**: Daily scheduled runs
- **Monitoring**: Data freshness and quality checks
- **Error Handling**: Retry logic and alerting  

## Data Statistics

### Input Data
- **Raw Records**: 541,909 transactions
- **File Size**: ~50MB CSV
- **Date Range**: Dec 1, 2010 - Dec 9, 2011 (305 days)

### After Processing
- **Clean Records**: 519,589 (95.9% retention)
- **Unique Customers**: 4,355 (including "Unknown")
- **Unique Products**: 4,161
- **Countries**: 38
- **Processing Time**: ~33 seconds end-to-end

### Output Datasets
1. **enriched_data**: Full transformed dataset
2. **customer_metrics**: 4,355 customer profiles
3. **product_metrics**: 4,161 product performance records
4. **daily_metrics**: 305 daily aggregations
5. **country_metrics**: 38 country summaries
6. **hourly_patterns**: 168 hour×day combinations

## Technology Stack

- **Processing**: Apache Spark 3.5.0
- **Orchestration**: Apache Airflow 2.8.0
- **Storage**: Parquet on local/cloud storage
- **Containerization**: Docker & Docker Compose
- **Language**: Python 3.11+

## Scalability Considerations

1. **Horizontal Scaling**: Add more Spark workers
2. **Vertical Scaling**: Increase driver/executor memory
3. **Partitioning**: Data partitioned by date for efficient queries
4. **Caching**: Frequently accessed data cached in memory

## Performance Optimizations

- Adaptive Query Execution (AQE) enabled
- Broadcast joins for small lookup tables
- Coalesce partitions to reduce file count
- Columnar storage with Parquet
- Predicate pushdown for efficient filtering
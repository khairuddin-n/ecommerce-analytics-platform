# Performance Optimization Guide

## Current Performance Metrics

Based on testing with the e-commerce dataset (541,909 transactions):

| Operation | Time | Throughput |
|-----------|------|------------|
| Data Ingestion | ~2s | 270k records/sec |
| Data Transformation | ~8s | 65k records/sec |
| Customer Analytics | ~1s | 4.3k customers/sec |
| Product Analytics | ~1s | 4.1k products/sec |
| Total Pipeline | ~33s | 16k records/sec |



## Memory Usage

- Driver Memory: 2-4GB
- Executor Memory: 2-4GB
- Total JVM Heap: 4-8GB

## Optimization Techniques

### 1. Spark Configuration

```python
# Adaptive Query Execution
spark.sql.adaptive.enabled = "true"
spark.sql.adaptive.coalescePartitions.enabled = "true"
spark.sql.adaptive.skewJoin.enabled = "true"

# Shuffle Partitions
spark.sql.shuffle.partitions = "200" 

# Arrow Optimization
spark.sql.execution.arrow.pyspark.enabled = "true"  
```  

### 2. Data Partitioning  
- Input data: Single partition (small dataset)
- Processing: Auto-partitioned by Spark
- Output: Coalesced to 4 partitions  

### 3. Caching Strategy  
```python  
# Cache frequently accessed DataFrames
enriched_df.cache()

# Unpersist when done
enriched_df.unpersist()
```  

### 4. Broadcast Joins  
Small lookup tables are automatically broadcast:
- Customer segments
- Product categories
- Country mappings

### 5. Predicate Pushdown  
Filters applied early in the pipeline:  
```python  
# Filter invalid data immediately
df.filter(F.col("quantity") > 0)
```  

## Optimization Results

Current optimizations provide:
- **33% faster** than baseline (from ~50s to ~33s)
- **Stable memory usage** under 4GB
- **Zero data loss** during processing
- **Consistent performance** across runs

Further optimizations possible:
- Partition by date for time-based queries
- Use columnar pruning more aggressively  
- Implement incremental processing
- Add data skipping indices


## Scaling Guidelines

### Small Data (<1M records / <1GB)
- Local mode ✓ 
- 4GB driver memory
- Single executor
- Processing time: ~30-60 seconds

### Medium Data (1-10M records / 1-10GB)
- Local mode with 8GB+ RAM
- Or cluster with 2-4 nodes  
- Increase shuffle partitions to 400
- Processing time: 2-10 minutes

### Large Data (>10M records / >10GB)
- Cluster mode required
- 4+ worker nodes
- 8GB+ per executor
- Consider Delta Lake for updates
- Processing time: 10-60 minutes 

## Monitoring  
### Spark UI  
Access during execution:
- Local: http://localhost:4040
- Docker: http://localhost:8081  

Key metrics to monitor:
- Stage execution time
- Shuffle read/write
- Memory usage
- GC time  

### Performance Bottlenecks

1. **Shuffle Operations**
   - Minimize with proper partitioning
   - Use broadcast joins when possible

2. **Window Functions**
   - Time series calculations require global ordering
   - Consider sampling for large datasets
   - **Warning**: "No Partition Defined for Window operation" - add partitionBy() for production

3. **Memory Pressure**
   - Increase executor memory
   - Reduce cache usage
   - Process in smaller batches

4. **Date Parsing**
   - Legacy date patterns may cause issues
   - Consider using spark.sql.legacy.timeParserPolicy = "LEGACY" for compatibility

## Benchmarking

Run performance tests:
```bash
# Time the pipeline
time python -m src.pipeline.main
# Actual result: ~33 seconds total

# Profile with cProfile  
python -m cProfile -o profile.stats src.pipeline.main

# Analyze profile
python -m pstats profile.stats
```  

## Actual Performance Statistics

From production run:
- **Input Records**: 541,909
- **Output Records**: 519,589 (95.9% retention)
- **Processing Time**: 33 seconds
- **Throughput**: ~16K records/second
- **Memory Peak**: ~3GB

### Output Files Generated:
1. enriched_data: 519,589 records
2. customer_metrics: 4,355 records  
3. product_metrics: 4,161 records
4. daily_metrics: 305 records
5. country_metrics: 38 records
6. hourly_patterns: 168 records  

## Best Practice  
1. **Filter Early**: Remove unnecessary data ASAP
2. **Column Pruning**: Select only needed columns
3. **Partition Wisely**: Balance between too few and too many
4. **Cache Smartly**: Only cache reused DataFrames
5. **Monitor Always**: Use Spark UI and logs

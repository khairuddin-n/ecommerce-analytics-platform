"""Monitoring and metrics collection module"""

from typing import Dict, Any, Optional
from datetime import datetime
import json
import time
from pathlib import Path
from src.utils.logger import logger
from src.utils.config import settings

try:
    from prometheus_client import Counter, Histogram, Gauge, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logger.warning("Prometheus client not available, metrics collection disabled")

class MetricsCollector:
    """Collect and expose pipeline metrics"""
    
    def __init__(self):
        self.enabled = settings.enable_metrics and PROMETHEUS_AVAILABLE
        
        if self.enabled:
            # Define metrics
            self.pipeline_runs = Counter('pipeline_runs_total', 
                                       'Total number of pipeline runs',
                                       ['status'])
            
            self.pipeline_duration = Histogram('pipeline_duration_seconds',
                                             'Pipeline execution duration',
                                             buckets=(10, 30, 60, 120, 300, 600))
            
            self.stage_duration = Histogram('stage_duration_seconds',
                                          'Stage execution duration',
                                          ['stage'])
            
            self.records_processed = Counter('records_processed_total',
                                           'Total records processed')
            
            self.data_quality_score = Gauge('data_quality_score',
                                          'Current data quality score')
            
            self.active_spark_jobs = Gauge('active_spark_jobs',
                                         'Number of active Spark jobs')
            
            # Start metrics server
            try:
                start_http_server(settings.metrics_port)
                logger.info(f"Metrics server started on port {settings.metrics_port}")
            except Exception as e:
                logger.warning(f"Failed to start metrics server: {e}")
                self.enabled = False
        
        # Local metrics storage
        self.metrics_dir = settings.project_root / "metrics"
        self.metrics_dir.mkdir(exist_ok=True, parents=True)
        
        self.current_run_metrics = {}
    
    def record_pipeline_run(self, results: Dict[str, Any]):
        """Record pipeline run metrics"""
        if not self.enabled:
            return
        
        try:
            # Update Prometheus metrics if available
            if hasattr(self, 'pipeline_runs'):
                status = results.get('status', 'unknown')
                self.pipeline_runs.labels(status=status).inc()
                
                if 'total_duration_seconds' in results:
                    self.pipeline_duration.observe(results['total_duration_seconds'])
                
                if 'rows_processed' in results:
                    self.records_processed.inc(results['rows_processed'])
            
            # Always save to local file
            self._save_metrics_to_file(results)
            
        except Exception as e:
            logger.error(f"Failed to record metrics: {e}")
    
    def record_stage_duration(self, stage: str, duration: float):
        """Record stage execution duration"""
        if not self.enabled:
            self.current_run_metrics['stages'] = {}  # Initialize even if disabled for testing
            self.current_run_metrics['stages'][stage] = {
                'duration': duration,
                'timestamp': datetime.now().isoformat()
            }
            return
        
        try:
            if hasattr(self, 'stage_duration'):
                self.stage_duration.labels(stage=stage).observe(duration)
            
            # Store in current run metrics
            if 'stages' not in self.current_run_metrics:
                self.current_run_metrics['stages'] = {}
            
            self.current_run_metrics['stages'][stage] = {
                'duration': duration,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to record stage metrics: {e}")
    
    def record_pipeline_failure(self, stage: str, error: str):
        """Record pipeline failure"""
        if not self.enabled:
            return
        
        try:
            self.pipeline_runs.labels(status='failed').inc()
            
            # Save failure details
            failure_metrics = {
                'status': 'failed',
                'failed_stage': stage,
                'error': error,
                'timestamp': datetime.now().isoformat()
            }
            
            self._save_metrics_to_file(failure_metrics)
            
        except Exception as e:
            logger.error(f"Failed to record failure metrics: {e}")
    
    def update_data_quality_score(self, score: float):
        """Update data quality score"""
        if not self.enabled:
            return
        
        try:
            self.data_quality_score.set(score)
        except Exception as e:
            logger.error(f"Failed to update quality score: {e}")
    
    def _save_metrics_to_file(self, metrics: Dict[str, Any]):
        """Save metrics to JSON file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            metrics_file = self.metrics_dir / f"pipeline_run_{timestamp}.json"
            
            with open(metrics_file, 'w') as f:
                json.dump(metrics, f, indent=2, default=str)
                
        except Exception as e:
            logger.error(f"Failed to save metrics to file: {e}")
    
    def get_historical_metrics(self, days: int = 7) -> Dict[str, Any]:
        """Get historical metrics for analysis"""
        metrics_files = sorted(self.metrics_dir.glob("pipeline_run_*.json"))
        
        # Filter by date
        cutoff_date = datetime.now().timestamp() - (days * 24 * 3600)
        recent_files = [f for f in metrics_files 
                       if f.stat().st_mtime > cutoff_date]
        
        historical_data = []
        for file in recent_files:
            try:
                with open(file, 'r') as f:
                    historical_data.append(json.load(f))
            except Exception as e:
                logger.warning(f"Failed to read metrics file {file}: {e}")
        
        # Calculate summary statistics
        if historical_data:
            durations = [m.get('total_duration_seconds', 0) 
                        for m in historical_data if m.get('status') == 'completed']
            
            summary = {
                'total_runs': len(historical_data),
                'successful_runs': len([m for m in historical_data 
                                       if m.get('status') == 'completed']),
                'failed_runs': len([m for m in historical_data 
                                   if m.get('status') == 'failed']),
                'avg_duration': sum(durations) / len(durations) if durations else 0,
                'min_duration': min(durations) if durations else 0,
                'max_duration': max(durations) if durations else 0
            }
        else:
            summary = {'message': 'No historical data available'}
        
        return summary

class PerformanceProfiler:
    """Profile pipeline performance"""
    
    def __init__(self):
        self.timings = {}
        self.enabled = settings.enable_profiling
    
    def profile_function(self, func_name: str):
        """Decorator to profile function execution"""
        def decorator(func):
            def wrapper(*args, **kwargs):
                if not self.enabled:
                    return func(*args, **kwargs)
                
                start_time = time.time()
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                
                if func_name not in self.timings:
                    self.timings[func_name] = []
                
                self.timings[func_name].append(duration)
                
                if duration > 1.0:  # Log slow operations
                    logger.warning(f"{func_name} took {duration:.2f}s")
                
                return result
            
            return wrapper
        return decorator
    
    def get_summary(self) -> Dict[str, Any]:
        """Get profiling summary"""
        summary = {}
        
        for func_name, durations in self.timings.items():
            summary[func_name] = {
                'calls': len(durations),
                'total_time': sum(durations),
                'avg_time': sum(durations) / len(durations),
                'min_time': min(durations),
                'max_time': max(durations)
            }
        
        return summary
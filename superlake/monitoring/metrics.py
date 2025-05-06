"""Metrics collection functionality for SuperLake."""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

@dataclass
class MetricDefinition:
    """Definition of a metric to collect."""
    name: str
    description: str
    unit: str
    aggregation: str = "count"
    filters: Optional[Dict[str, Any]] = None

class MetricsCollector:
    """Collector for pipeline and data metrics."""
    
    def __init__(self, spark: SparkSession):
        """Initialize metrics collector."""
        self.spark = spark
        self.metrics = {}
        
    def collect_table_metrics(self,
                            df: DataFrame,
                            metrics: List[MetricDefinition]) -> Dict[str, float]:
        """Collect metrics from DataFrame."""
        results = {}
        
        for metric in metrics:
            # Apply filters if specified
            target_df = df
            if metric.filters:
                for col, value in metric.filters.items():
                    target_df = target_df.filter(F.col(col) == value)
                    
            # Calculate metric based on aggregation type
            if metric.aggregation == "count":
                value = target_df.count()
            elif metric.aggregation == "sum":
                value = target_df.select(F.sum(metric.name)).collect()[0][0]
            elif metric.aggregation == "avg":
                value = target_df.select(F.avg(metric.name)).collect()[0][0]
            elif metric.aggregation == "min":
                value = target_df.select(F.min(metric.name)).collect()[0][0]
            elif metric.aggregation == "max":
                value = target_df.select(F.max(metric.name)).collect()[0][0]
            else:
                raise ValueError(f"Unsupported aggregation: {metric.aggregation}")
                
            results[metric.name] = value
            
        return results
    
    def collect_quality_metrics(self,
                              df: DataFrame,
                              columns: Optional[List[str]] = None) -> Dict[str, Dict[str, float]]:
        """Collect data quality metrics."""
        if not columns:
            columns = df.columns
            
        results = {}
        for column in columns:
            col_metrics = {}
            
            # Count nulls
            null_count = df.filter(F.col(column).isNull()).count()
            col_metrics["null_count"] = null_count
            col_metrics["null_percentage"] = (null_count / df.count()) * 100
            
            # Count distinct values
            distinct_count = df.select(column).distinct().count()
            col_metrics["distinct_count"] = distinct_count
            
            # Get basic statistics if numeric
            try:
                stats = df.select(
                    F.min(column).alias("min"),
                    F.max(column).alias("max"),
                    F.avg(column).alias("mean"),
                    F.stddev(column).alias("stddev")
                ).collect()[0].asDict()
                col_metrics.update(stats)
            except:
                pass  # Column is not numeric
                
            results[column] = col_metrics
            
        return results
    
    def collect_performance_metrics(self,
                                  df: DataFrame,
                                  operation: str) -> Dict[str, float]:
        """Collect performance metrics for operation."""
        start_time = datetime.now()
        
        # Force computation to measure performance
        count = df.count()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return {
            f"{operation}_duration_seconds": duration,
            f"{operation}_record_count": count,
            f"{operation}_records_per_second": count / duration if duration > 0 else 0
        }
    
    def collect_storage_metrics(self,
                              table_path: str) -> Dict[str, float]:
        """Collect storage metrics for Delta table."""
        metrics = {}
        
        # Get table details
        details = self.spark.sql(
            f"DESCRIBE DETAIL delta.`{table_path}`"
        ).collect()[0]
        
        metrics["size_bytes"] = details["sizeInBytes"]
        metrics["num_files"] = details["numFiles"]
        
        # Get version history size
        history = self.spark.sql(
            f"DESCRIBE HISTORY delta.`{table_path}`"
        ).collect()
        metrics["num_versions"] = len(history)
        
        return metrics
    
    def save_metrics(self,
                    metrics: Dict[str, Any],
                    table_name: str) -> None:
        """Save metrics to tracking table."""
        # Convert metrics to DataFrame
        metrics_df = self.spark.createDataFrame([
            (datetime.now(), name, float(value))
            for name, value in metrics.items()
        ], ["timestamp", "metric_name", "metric_value"])
        
        # Save to Delta table
        metrics_df.write.format("delta").mode("append").saveAsTable(table_name)
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get all collected metrics."""
        return self.metrics.copy() 
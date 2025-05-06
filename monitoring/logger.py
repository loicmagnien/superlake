"""Logging functionality for SuperLake."""

import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, Dict, Any
from applicationinsights import TelemetryClient

class SuperLogger:
    """Logger for data pipeline operations."""
    
    def __init__(self,
                 name: str = "SuperLake",
                 level: int = logging.INFO,
                 app_insights_key: Optional[str] = None):
        """Initialize logger with configuration."""
        # Set up Python logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # Add console handler if none exists
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            
        # Set up Application Insights if configured
        self.telemetry = None
        if app_insights_key:
            self.telemetry = TelemetryClient(app_insights_key)
            
        # Initialize metrics storage
        self.metrics = {}
        self.current_operation = None
        
    def info(self, message: str) -> None:
        """Log info message."""
        self.logger.info(message)
        if self.telemetry:
            self.telemetry.track_trace(message, severity='INFO')
            
    def warning(self, message: str) -> None:
        """Log warning message."""
        self.logger.warning(message)
        if self.telemetry:
            self.telemetry.track_trace(message, severity='WARNING')
            
    def error(self, message: str, exc_info: bool = True) -> None:
        """Log error message."""
        self.logger.error(message, exc_info=exc_info)
        if self.telemetry:
            self.telemetry.track_trace(message, severity='ERROR')
            
    def metric(self, 
              name: str,
              value: float,
              properties: Optional[Dict[str, Any]] = None) -> None:
        """Log metric value."""
        self.metrics[name] = value
        self.logger.info(f"Metric - {name}: {value}")
        
        if self.telemetry:
            self.telemetry.track_metric(
                name,
                value,
                properties=properties or {}
            )
            
    @contextmanager
    def track_execution(self, operation_name: str):
        """Track execution time of an operation."""
        start_time = datetime.now()
        self.current_operation = operation_name
        self.info(f"Starting operation: {operation_name}")
        
        try:
            yield
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.metric(
                f"{operation_name}_duration_seconds",
                duration,
                {"status": "success"}
            )
            self.info(f"Completed operation: {operation_name} in {duration:.2f}s")
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.metric(
                f"{operation_name}_duration_seconds",
                duration,
                {"status": "failed"}
            )
            self.error(
                f"Failed operation: {operation_name} after {duration:.2f}s - {str(e)}"
            )
            raise
        finally:
            self.current_operation = None
            
    def get_metrics(self) -> Dict[str, float]:
        """Get all collected metrics."""
        return self.metrics.copy()
    
    def reset_metrics(self) -> None:
        """Reset metrics storage."""
        self.metrics.clear()
        
    def flush(self) -> None:
        """Flush all handlers and telemetry."""
        for handler in self.logger.handlers:
            handler.flush()
        if self.telemetry:
            self.telemetry.flush() 
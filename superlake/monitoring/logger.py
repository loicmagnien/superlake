"""Logging functionality for SuperLake."""

import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Optional, Dict, Any
from applicationinsights import TelemetryClient
import threading


class SubNameContextFilter(logging.Filter):
    _thread_local = threading.local()

    def __init__(self, sub_name=None):
        super().__init__()
        self.set_sub_name(sub_name or "")

    @classmethod
    def set_sub_name(cls, sub_name):
        cls._thread_local.sub_name = sub_name or ""

    @classmethod
    def get_sub_name(cls):
        return getattr(cls._thread_local, 'sub_name', "")

    def filter(self, record):
        sub_name = self.get_sub_name()
        record.sub_name_field = f" - {sub_name}" if sub_name else ""
        return True


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
                '%(asctime)s - %(name)s%(sub_name_field)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.sub_name = ""
            self.filter = SubNameContextFilter(self.sub_name)
            handler.addFilter(self.filter)
            self.logger.addHandler(handler)
        else:
            # If handlers exist, add the filter to all handlers
            self.sub_name = ""
            self.filter = SubNameContextFilter(self.sub_name)
            for handler in self.logger.handlers:
                handler.addFilter(self.filter)
        # Prevent the logger from propagating to the root logger
        self.logger.propagate = False

        # Set up Application Insights if configured
        self.telemetry = None
        if app_insights_key:
            self.telemetry = TelemetryClient(app_insights_key)

        # Initialize metrics storage
        self.metrics = {}
        self.current_operation = None

    @contextmanager
    def sub_name_context(self, sub_name: str):
        old_sub_name = SubNameContextFilter.get_sub_name()
        self.set_sub_name(sub_name)
        try:
            yield
        finally:
            self.set_sub_name(old_sub_name)

    def set_sub_name(self, sub_name: str):
        self.sub_name = sub_name
        for handler in self.logger.handlers:
            for f in handler.filters:
                if isinstance(f, SubNameContextFilter):
                    f.set_sub_name(sub_name)

    # For backward compatibility, keep set_pipeline as an alias
    def set_pipeline(self, pipeline_name: str):
        self.set_sub_name(pipeline_name)

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

    def metric(
            self,
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

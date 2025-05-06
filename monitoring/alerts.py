"""Alerting functionality for SuperLake."""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable
from enum import Enum

class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class AlertRule:
    """Definition of an alert rule."""
    name: str
    description: str
    severity: AlertSeverity
    condition: Callable[[Dict[str, float]], bool]
    message_template: str
    threshold: Optional[float] = None

@dataclass
class Alert:
    """Alert instance."""
    rule: AlertRule
    timestamp: datetime
    metrics: Dict[str, float]
    message: str

class AlertManager:
    """Manager for monitoring alerts."""
    
    def __init__(self,
                 rules: Optional[List[AlertRule]] = None,
                 handlers: Optional[Dict[str, Callable[[Alert], None]]] = None):
        """Initialize alert manager."""
        self.rules = rules or []
        self.handlers = handlers or {}
        self.alerts = []
        
    def add_rule(self, rule: AlertRule) -> None:
        """Add alert rule."""
        self.rules.append(rule)
        
    def add_handler(self,
                   name: str,
                   handler: Callable[[Alert], None]) -> None:
        """Add alert handler."""
        self.handlers[name] = handler
        
    def check_alerts(self,
                    metrics: Dict[str, float]) -> List[Alert]:
        """Check metrics against alert rules."""
        current_alerts = []
        
        for rule in self.rules:
            try:
                if rule.condition(metrics):
                    alert = Alert(
                        rule=rule,
                        timestamp=datetime.now(),
                        metrics=metrics,
                        message=rule.message_template.format(**metrics)
                    )
                    current_alerts.append(alert)
                    self.alerts.append(alert)
                    self._handle_alert(alert)
            except Exception as e:
                print(f"Error checking rule {rule.name}: {str(e)}")
                
        return current_alerts
    
    def _handle_alert(self, alert: Alert) -> None:
        """Process alert through registered handlers."""
        for handler in self.handlers.values():
            try:
                handler(alert)
            except Exception as e:
                print(f"Error in alert handler: {str(e)}")
                
    def get_alerts(self,
                  severity: Optional[AlertSeverity] = None,
                  since: Optional[datetime] = None) -> List[Alert]:
        """Get filtered alerts."""
        filtered = self.alerts
        
        if severity:
            filtered = [a for a in filtered if a.rule.severity == severity]
            
        if since:
            filtered = [a for a in filtered if a.timestamp >= since]
            
        return filtered
    
    @staticmethod
    def email_handler(alert: Alert) -> None:
        """Example email alert handler."""
        # Implementation would depend on email service
        print(f"Would send email for alert: {alert.message}")
        
    @staticmethod
    def slack_handler(alert: Alert) -> None:
        """Example Slack alert handler."""
        # Implementation would depend on Slack API
        print(f"Would send Slack message for alert: {alert.message}")
        
    @staticmethod
    def teams_handler(alert: Alert) -> None:
        """Example Microsoft Teams alert handler."""
        # Implementation would depend on Teams API
        print(f"Would send Teams message for alert: {alert.message}")
        
    def create_threshold_rule(self,
                            name: str,
                            metric_name: str,
                            threshold: float,
                            operator: str = ">",
                            severity: AlertSeverity = AlertSeverity.WARNING) -> AlertRule:
        """Create simple threshold-based alert rule."""
        operators = {
            ">": lambda x, t: x > t,
            ">=": lambda x, t: x >= t,
            "<": lambda x, t: x < t,
            "<=": lambda x, t: x <= t,
            "==": lambda x, t: x == t,
            "!=": lambda x, t: x != t
        }
        
        if operator not in operators:
            raise ValueError(f"Unsupported operator: {operator}")
            
        def condition(metrics: Dict[str, float]) -> bool:
            if metric_name not in metrics:
                return False
            return operators[operator](metrics[metric_name], threshold)
            
        return AlertRule(
            name=name,
            description=f"Alert when {metric_name} {operator} {threshold}",
            severity=severity,
            condition=condition,
            message_template=f"{metric_name} is {operator} {threshold}: {{" + metric_name + "}}",
            threshold=threshold
        ) 
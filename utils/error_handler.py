"""
Enhanced error handling and retry logic for document processing.
"""

import logging
import time
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timedelta
import traceback
import json


logger = logging.getLogger(__name__)


class ErrorType(Enum):
    """Classification of error types."""
    NETWORK_ERROR = "network_error"
    TIMEOUT_ERROR = "timeout_error"
    VALIDATION_ERROR = "validation_error"
    PROCESSING_ERROR = "processing_error"
    STORAGE_ERROR = "storage_error"
    AUTHENTICATION_ERROR = "authentication_error"
    RATE_LIMIT_ERROR = "rate_limit_error"
    UNKNOWN_ERROR = "unknown_error"


class ErrorSeverity(Enum):
    """Error severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class ErrorInfo:
    """Detailed error information."""
    error_type: ErrorType
    severity: ErrorSeverity
    message: str
    details: Optional[Dict] = None
    timestamp: datetime = None
    retry_count: int = 0
    max_retries: int = 3
    retry_delay: float = 1.0
    exponential_backoff: bool = True
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()
    
    def to_dict(self) -> Dict:
        """Convert error info to dictionary."""
        return {
            'error_type': self.error_type.value,
            'severity': self.severity.value,
            'message': self.message,
            'details': self.details,
            'timestamp': self.timestamp.isoformat(),
            'retry_count': self.retry_count,
            'max_retries': self.max_retries,
            'retry_delay': self.retry_delay,
            'exponential_backoff': self.exponential_backoff
        }


class ErrorHandler:
    """Enhanced error handler with retry logic and classification."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.error_history: List[ErrorInfo] = []
        self.error_callbacks: Dict[ErrorType, List[Callable]] = {}
        
        # Default retry configurations
        self.retry_configs = {
            ErrorType.NETWORK_ERROR: {'max_retries': 3, 'delay': 2.0, 'backoff': True},
            ErrorType.TIMEOUT_ERROR: {'max_retries': 2, 'delay': 5.0, 'backoff': True},
            ErrorType.RATE_LIMIT_ERROR: {'max_retries': 5, 'delay': 10.0, 'backoff': True},
            ErrorType.PROCESSING_ERROR: {'max_retries': 1, 'delay': 1.0, 'backoff': False},
            ErrorType.VALIDATION_ERROR: {'max_retries': 0, 'delay': 0.0, 'backoff': False},
            ErrorType.AUTHENTICATION_ERROR: {'max_retries': 1, 'delay': 1.0, 'backoff': False},
            ErrorType.STORAGE_ERROR: {'max_retries': 2, 'delay': 3.0, 'backoff': True},
            ErrorType.UNKNOWN_ERROR: {'max_retries': 1, 'delay': 1.0, 'backoff': False}
        }
    
    def classify_error(self, error: Exception) -> ErrorType:
        """
        Classify error type based on exception.
        
        Args:
            error: Exception to classify
            
        Returns:
            ErrorType enum value
        """
        error_str = str(error).lower()
        error_type_name = type(error).__name__.lower()
        
        # Network-related errors
        if any(keyword in error_str for keyword in ['connection', 'network', 'dns', 'socket']):
            return ErrorType.NETWORK_ERROR
        
        # Timeout errors
        if any(keyword in error_str for keyword in ['timeout', 'timed out']):
            return ErrorType.TIMEOUT_ERROR
        
        # Rate limit errors
        if any(keyword in error_str for keyword in ['rate limit', 'too many requests', '429']):
            return ErrorType.RATE_LIMIT_ERROR
        
        # Authentication errors
        if any(keyword in error_str for keyword in ['unauthorized', 'forbidden', 'authentication', '401', '403']):
            return ErrorType.AUTHENTICATION_ERROR
        
        # Validation errors
        if any(keyword in error_str for keyword in ['validation', 'invalid', 'malformed']):
            return ErrorType.VALIDATION_ERROR
        
        # Storage errors
        if any(keyword in error_str for keyword in ['storage', 'database', 'file not found', 'permission denied']):
            return ErrorType.STORAGE_ERROR
        
        # Processing errors
        if any(keyword in error_str for keyword in ['processing', 'parse', 'format']):
            return ErrorType.PROCESSING_ERROR
        
        # Check exception types
        if 'timeout' in error_type_name:
            return ErrorType.TIMEOUT_ERROR
        elif 'connection' in error_type_name:
            return ErrorType.NETWORK_ERROR
        elif 'value' in error_type_name or 'type' in error_type_name:
            return ErrorType.VALIDATION_ERROR
        
        return ErrorType.UNKNOWN_ERROR
    
    def determine_severity(self, error_type: ErrorType, error: Exception) -> ErrorSeverity:
        """
        Determine error severity.
        
        Args:
            error_type: Classified error type
            error: Original exception
            
        Returns:
            ErrorSeverity enum value
        """
        severity_map = {
            ErrorType.AUTHENTICATION_ERROR: ErrorSeverity.CRITICAL,
            ErrorType.VALIDATION_ERROR: ErrorSeverity.HIGH,
            ErrorType.PROCESSING_ERROR: ErrorSeverity.HIGH,
            ErrorType.NETWORK_ERROR: ErrorSeverity.MEDIUM,
            ErrorType.TIMEOUT_ERROR: ErrorSeverity.MEDIUM,
            ErrorType.STORAGE_ERROR: ErrorSeverity.MEDIUM,
            ErrorType.RATE_LIMIT_ERROR: ErrorSeverity.LOW,
            ErrorType.UNKNOWN_ERROR: ErrorSeverity.HIGH
        }
        
        return severity_map.get(error_type, ErrorSeverity.MEDIUM)
    
    def create_error_info(self, error: Exception, context: Dict = None) -> ErrorInfo:
        """
        Create detailed error information.
        
        Args:
            error: Exception that occurred
            context: Additional context information
            
        Returns:
            ErrorInfo object
        """
        error_type = self.classify_error(error)
        severity = self.determine_severity(error_type, error)
        
        # Get retry configuration
        retry_config = self.retry_configs.get(error_type, {})
        
        # Create detailed error information
        details = {
            'exception_type': type(error).__name__,
            'traceback': traceback.format_exc(),
            'context': context or {}
        }
        
        error_info = ErrorInfo(
            error_type=error_type,
            severity=severity,
            message=str(error),
            details=details,
            max_retries=retry_config.get('max_retries', 1),
            retry_delay=retry_config.get('delay', 1.0),
            exponential_backoff=retry_config.get('backoff', False)
        )
        
        # Add to history
        self.error_history.append(error_info)
        
        # Keep only last 1000 errors
        if len(self.error_history) > 1000:
            self.error_history = self.error_history[-1000:]
        
        return error_info
    
    def should_retry(self, error_info: ErrorInfo) -> bool:
        """
        Determine if operation should be retried.
        
        Args:
            error_info: Error information
            
        Returns:
            True if should retry
        """
        return error_info.retry_count < error_info.max_retries
    
    def get_retry_delay(self, error_info: ErrorInfo) -> float:
        """
        Calculate retry delay.
        
        Args:
            error_info: Error information
            
        Returns:
            Delay in seconds
        """
        if not error_info.exponential_backoff:
            return error_info.retry_delay
        
        # Exponential backoff with jitter
        import random
        base_delay = error_info.retry_delay
        backoff_delay = base_delay * (2 ** error_info.retry_count)
        jitter = random.uniform(0.1, 0.5) * backoff_delay
        
        return backoff_delay + jitter
    
    def retry_operation(
        self,
        operation: Callable,
        *args,
        context: Dict = None,
        **kwargs
    ) -> Any:
        """
        Execute operation with retry logic.
        
        Args:
            operation: Function to execute
            *args: Operation arguments
            context: Additional context
            **kwargs: Operation keyword arguments
            
        Returns:
            Operation result
            
        Raises:
            Exception: If all retries are exhausted
        """
        last_error_info = None
        
        while True:
            try:
                result = operation(*args, **kwargs)
                
                # Log successful retry if there was a previous error
                if last_error_info:
                    self.logger.info(f"Operation succeeded after {last_error_info.retry_count} retries")
                
                return result
                
            except Exception as e:
                error_info = self.create_error_info(e, context)
                
                # Update retry count if this is a retry
                if last_error_info and last_error_info.error_type == error_info.error_type:
                    error_info.retry_count = last_error_info.retry_count + 1
                
                self.logger.error(f"Operation failed: {error_info.message} (attempt {error_info.retry_count + 1})")
                
                # Check if we should retry
                if not self.should_retry(error_info):
                    self.logger.error(f"Max retries ({error_info.max_retries}) exhausted for {error_info.error_type.value}")
                    
                    # Call error callbacks
                    self._call_error_callbacks(error_info)
                    
                    raise e
                
                # Wait before retry
                delay = self.get_retry_delay(error_info)
                self.logger.info(f"Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
                
                last_error_info = error_info
    
    def _call_error_callbacks(self, error_info: ErrorInfo) -> None:
        """
        Call registered error callbacks.
        
        Args:
            error_info: Error information
        """
        callbacks = self.error_callbacks.get(error_info.error_type, [])
        
        for callback in callbacks:
            try:
                callback(error_info)
            except Exception as e:
                self.logger.error(f"Error callback failed: {e}")
    
    def register_error_callback(self, error_type: ErrorType, callback: Callable) -> None:
        """
        Register error callback for specific error type.
        
        Args:
            error_type: Error type to handle
            callback: Callback function
        """
        if error_type not in self.error_callbacks:
            self.error_callbacks[error_type] = []
        
        self.error_callbacks[error_type].append(callback)
        self.logger.info(f"Registered error callback for {error_type.value}")
    
    def get_error_statistics(self) -> Dict:
        """
        Get error statistics.
        
        Returns:
            Error statistics dictionary
        """
        if not self.error_history:
            return {
                'total_errors': 0,
                'error_types': {},
                'severity_distribution': {},
                'recent_errors': []
            }
        
        # Count by error type
        error_types = {}
        for error in self.error_history:
            error_type = error.error_type.value
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        # Count by severity
        severity_distribution = {}
        for error in self.error_history:
            severity = error.severity.value
            severity_distribution[severity] = severity_distribution.get(severity, 0) + 1
        
        # Get recent errors (last 10)
        recent_errors = [
            error.to_dict() for error in self.error_history[-10:]
        ]
        
        return {
            'total_errors': len(self.error_history),
            'error_types': error_types,
            'severity_distribution': severity_distribution,
            'recent_errors': recent_errors
        }
    
    def get_error_trends(self, hours: int = 24) -> Dict:
        """
        Get error trends over time.
        
        Args:
            hours: Hours to look back
            
        Returns:
            Error trends dictionary
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        recent_errors = [
            error for error in self.error_history
            if error.timestamp > cutoff_time
        ]
        
        if not recent_errors:
            return {
                'time_period_hours': hours,
                'total_errors': 0,
                'error_rate': 0.0,
                'most_common_error': None,
                'trend': 'stable'
            }
        
        # Calculate error rate (errors per hour)
        error_rate = len(recent_errors) / hours
        
        # Find most common error type
        error_type_counts = {}
        for error in recent_errors:
            error_type = error.error_type.value
            error_type_counts[error_type] = error_type_counts.get(error_type, 0) + 1
        
        most_common_error = max(error_type_counts.items(), key=lambda x: x[1])[0] if error_type_counts else None
        
        # Simple trend analysis (compare first and second half)
        midpoint = len(recent_errors) // 2
        first_half = recent_errors[:midpoint]
        second_half = recent_errors[midpoint:]
        
        if len(second_half) > len(first_half) * 1.2:
            trend = 'increasing'
        elif len(second_half) < len(first_half) * 0.8:
            trend = 'decreasing'
        else:
            trend = 'stable'
        
        return {
            'time_period_hours': hours,
            'total_errors': len(recent_errors),
            'error_rate': error_rate,
            'most_common_error': most_common_error,
            'trend': trend,
            'error_type_counts': error_type_counts
        }
    
    def clear_error_history(self) -> int:
        """
        Clear error history.
        
        Returns:
            Number of errors cleared
        """
        count = len(self.error_history)
        self.error_history.clear()
        self.logger.info(f"Cleared {count} errors from history")
        return count
    
    def export_error_log(self, filename: str = None) -> str:
        """
        Export error log to file.
        
        Args:
            filename: Output filename
            
        Returns:
            Filename of exported log
        """
        if filename is None:
            filename = f"error_log_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        
        export_data = {
            'export_timestamp': datetime.utcnow().isoformat(),
            'total_errors': len(self.error_history),
            'errors': [error.to_dict() for error in self.error_history]
        }
        
        with open(filename, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        self.logger.info(f"Exported {len(self.error_history)} errors to {filename}")
        return filename


# Global error handler instance
error_handler = ErrorHandler()
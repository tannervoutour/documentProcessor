import time
import logging
from typing import Callable, Any, Optional, Dict
from dataclasses import dataclass
from enum import Enum
from threading import Lock
import functools

logger = logging.getLogger(__name__)

class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, blocking requests
    HALF_OPEN = "half_open"  # Testing if service recovered

@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker"""
    failure_threshold: int = 5          # Number of failures before opening
    recovery_timeout: int = 60          # Seconds before attempting recovery
    success_threshold: int = 3          # Successes needed to close from half-open
    timeout: int = 30                   # Request timeout in seconds
    expected_exceptions: tuple = (Exception,)  # Exceptions that count as failures

class CircuitBreakerStats:
    """Statistics for circuit breaker"""
    
    def __init__(self):
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.circuit_opened_count = 0
        self.last_failure_time = None
        self.last_success_time = None
        self.current_consecutive_failures = 0
        self.current_consecutive_successes = 0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        if self.total_requests == 0:
            return 0.0
        return (self.successful_requests / self.total_requests) * 100
    
    @property
    def failure_rate(self) -> float:
        """Calculate failure rate percentage"""
        if self.total_requests == 0:
            return 0.0
        return (self.failed_requests / self.total_requests) * 100

class CircuitBreaker:
    """
    Circuit breaker implementation for protecting external service calls.
    Prevents cascading failures by temporarily blocking requests to failing services.
    """
    
    def __init__(self, name: str, config: CircuitBreakerConfig = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.stats = CircuitBreakerStats()
        self.last_failure_time = None
        self.lock = Lock()
        self.logger = logging.getLogger(f"{__name__}.{name}")
        
        self.logger.info(f"Circuit breaker '{name}' initialized with config: {self.config}")
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            CircuitBreakerOpenException: When circuit is open
            Original exception: When function fails
        """
        with self.lock:
            self.stats.total_requests += 1
            
            # Check if circuit should be opened
            if self.state == CircuitState.CLOSED:
                if self.stats.current_consecutive_failures >= self.config.failure_threshold:
                    self._open_circuit()
            
            # Check if circuit should move to half-open
            elif self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._half_open_circuit()
            
            # Block requests when circuit is open
            if self.state == CircuitState.OPEN:
                self.logger.warning(f"Circuit breaker '{self.name}' is OPEN, blocking request")
                raise CircuitBreakerOpenException(f"Circuit breaker '{self.name}' is open")
        
        # Execute the function
        try:
            start_time = time.time()
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            self._record_success(execution_time)
            return result
            
        except self.config.expected_exceptions as e:
            self._record_failure(e)
            raise
        except Exception as e:
            # Unexpected exceptions don't count as circuit breaker failures
            self.logger.warning(f"Unexpected exception in circuit breaker '{self.name}': {e}")
            raise
    
    def _record_success(self, execution_time: float):
        """Record successful execution"""
        with self.lock:
            self.stats.successful_requests += 1
            self.stats.last_success_time = time.time()
            self.stats.current_consecutive_failures = 0
            self.stats.current_consecutive_successes += 1
            
            # If we're in half-open state, check if we should close
            if self.state == CircuitState.HALF_OPEN:
                if self.stats.current_consecutive_successes >= self.config.success_threshold:
                    self._close_circuit()
            
            self.logger.debug(f"Circuit breaker '{self.name}' recorded success (execution time: {execution_time:.2f}s)")
    
    def _record_failure(self, exception: Exception):
        """Record failed execution"""
        with self.lock:
            self.stats.failed_requests += 1
            self.stats.last_failure_time = time.time()
            self.stats.current_consecutive_successes = 0
            self.stats.current_consecutive_failures += 1
            
            self.logger.warning(f"Circuit breaker '{self.name}' recorded failure: {exception}")
            
            # If we're in half-open state, go back to open
            if self.state == CircuitState.HALF_OPEN:
                self._open_circuit()
    
    def _open_circuit(self):
        """Open the circuit breaker"""
        self.state = CircuitState.OPEN
        self.last_failure_time = time.time()
        self.stats.circuit_opened_count += 1
        self.logger.error(f"Circuit breaker '{self.name}' opened after {self.stats.current_consecutive_failures} consecutive failures")
    
    def _half_open_circuit(self):
        """Move circuit to half-open state"""
        self.state = CircuitState.HALF_OPEN
        self.stats.current_consecutive_successes = 0
        self.logger.info(f"Circuit breaker '{self.name}' moved to HALF_OPEN state")
    
    def _close_circuit(self):
        """Close the circuit breaker"""
        self.state = CircuitState.CLOSED
        self.stats.current_consecutive_failures = 0
        self.stats.current_consecutive_successes = 0
        self.logger.info(f"Circuit breaker '{self.name}' closed after {self.config.success_threshold} consecutive successes")
    
    def _should_attempt_reset(self) -> bool:
        """Check if we should attempt to reset the circuit"""
        if self.last_failure_time is None:
            return True
        
        time_since_failure = time.time() - self.last_failure_time
        return time_since_failure >= self.config.recovery_timeout
    
    def get_state(self) -> CircuitState:
        """Get current circuit state"""
        return self.state
    
    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics"""
        return {
            'name': self.name,
            'state': self.state.value,
            'total_requests': self.stats.total_requests,
            'successful_requests': self.stats.successful_requests,
            'failed_requests': self.stats.failed_requests,
            'success_rate': self.stats.success_rate,
            'failure_rate': self.stats.failure_rate,
            'circuit_opened_count': self.stats.circuit_opened_count,
            'current_consecutive_failures': self.stats.current_consecutive_failures,
            'current_consecutive_successes': self.stats.current_consecutive_successes,
            'last_failure_time': self.stats.last_failure_time,
            'last_success_time': self.stats.last_success_time,
            'config': {
                'failure_threshold': self.config.failure_threshold,
                'recovery_timeout': self.config.recovery_timeout,
                'success_threshold': self.config.success_threshold,
                'timeout': self.config.timeout
            }
        }
    
    def reset(self):
        """Manually reset circuit breaker to closed state"""
        with self.lock:
            self.state = CircuitState.CLOSED
            self.stats.current_consecutive_failures = 0
            self.stats.current_consecutive_successes = 0
            self.last_failure_time = None
            self.logger.info(f"Circuit breaker '{self.name}' manually reset")

class CircuitBreakerOpenException(Exception):
    """Exception raised when circuit breaker is open"""
    pass

def circuit_breaker(name: str, config: CircuitBreakerConfig = None):
    """
    Decorator for applying circuit breaker to functions.
    
    Args:
        name: Name of the circuit breaker
        config: Circuit breaker configuration
        
    Example:
        @circuit_breaker('external_api', CircuitBreakerConfig(failure_threshold=3))
        def call_external_api():
            # API call implementation
            pass
    """
    breaker = CircuitBreaker(name, config)
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return breaker.call(func, *args, **kwargs)
        
        # Attach circuit breaker to function for access to stats
        wrapper._circuit_breaker = breaker
        return wrapper
    
    return decorator

class CircuitBreakerManager:
    """
    Global manager for circuit breakers.
    Provides centralized access to all circuit breakers and their statistics.
    """
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._circuit_breakers = {}
        return cls._instance
    
    def get_circuit_breaker(self, name: str, config: CircuitBreakerConfig = None) -> CircuitBreaker:
        """Get or create a circuit breaker"""
        if name not in self._circuit_breakers:
            self._circuit_breakers[name] = CircuitBreaker(name, config)
        return self._circuit_breakers[name]
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all circuit breakers"""
        return {name: breaker.get_stats() for name, breaker in self._circuit_breakers.items()}
    
    def reset_all(self):
        """Reset all circuit breakers"""
        for breaker in self._circuit_breakers.values():
            breaker.reset()
        logger.info("All circuit breakers reset")
    
    def get_circuit_breaker_names(self) -> list:
        """Get names of all registered circuit breakers"""
        return list(self._circuit_breakers.keys())

# Global circuit breaker manager instance
circuit_breaker_manager = CircuitBreakerManager()
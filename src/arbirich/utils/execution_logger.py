"""
Enhanced debug logging specifically for execution flows.
"""

import functools
import logging
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Optional

# Create a dedicated logger for execution flows
logger = logging.getLogger("src.arbirich.core.trading.execution")

# Track execution stats
_execution_stats = {"counts": {}, "durations": {}, "errors": {}, "last_execution": None}

# Configuration
_detailed_logging = False
_trace_data = False
_log_performance = True


def configure_execution_logging(
    detailed: bool = False, trace_data: bool = False, performance: bool = True, level: str = "DEBUG"
):
    """Configure execution logging parameters"""
    global _detailed_logging, _trace_data, _log_performance

    _detailed_logging = detailed
    _trace_data = trace_data
    _log_performance = performance

    # Set logger level
    level_map = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING, "ERROR": logging.ERROR}
    logger.setLevel(level_map.get(level.upper(), logging.DEBUG))

    logger.info(
        f"Execution logging configured: detailed={detailed}, trace_data={trace_data}, "
        f"performance={performance}, level={level}"
    )


def log_execution_start(name: str, data: Optional[Dict[str, Any]] = None) -> None:
    """Log the start of an execution step"""
    if not _detailed_logging:
        return

    # Initialize stats for this execution
    _execution_stats["counts"][name] = _execution_stats["counts"].get(name, 0) + 1
    _execution_stats["last_execution"] = name

    # Log the execution start
    msg = f"⚡ Starting execution: {name}"
    if _trace_data and data:
        # Limit data size in logs
        safe_data = {
            k: str(v)[:100] + "..." if isinstance(v, str) and len(str(v)) > 100 else v for k, v in data.items()
        }
        msg += f" | Data: {safe_data}"

    logger.debug(msg)


def log_execution_end(
    name: str, success: bool = True, result: Optional[Any] = None, duration_ms: Optional[float] = None
) -> None:
    """Log the end of an execution step"""
    if not _detailed_logging:
        return

    # Record stats
    if name in _execution_stats["durations"]:
        _execution_stats["durations"][name].append(duration_ms if duration_ms is not None else 0)
    else:
        _execution_stats["durations"][name] = [duration_ms if duration_ms is not None else 0]

    # Log completion
    status = "✅" if success else "❌"
    msg = f"{status} Completed execution: {name}"

    if _log_performance and duration_ms is not None:
        msg += f" | Duration: {duration_ms:.2f}ms"

    if _trace_data and result is not None:
        # Safely truncate result data
        result_str = str(result)
        if len(result_str) > 100:
            result_str = result_str[:100] + "..."
        msg += f" | Result: {result_str}"

    log_fn = logger.debug if success else logger.warning
    log_fn(msg)


def log_execution_error(name: str, error: Exception, context: Optional[Dict[str, Any]] = None) -> None:
    """Log an execution error with context"""
    # Always log errors regardless of detailed logging setting
    # Record stats
    _execution_stats["errors"][name] = _execution_stats["errors"].get(name, 0) + 1

    # Generate detailed error message
    error_type = type(error).__name__
    msg = f"🔥 Execution error in {name}: {error_type}: {str(error)}"

    if context:
        # Safely include context data
        safe_context = {
            k: str(v)[:100] + "..." if isinstance(v, str) and len(str(v)) > 100 else v for k, v in context.items()
        }
        msg += f" | Context: {safe_context}"

    logger.error(msg, exc_info=True)


@contextmanager
def execution_step(name: str, data: Optional[Dict[str, Any]] = None):
    """Context manager for tracking execution steps with timing"""
    start_time = time.time()
    success = False
    result = None

    try:
        log_execution_start(name, data)
        yield
        success = True
    except Exception as e:
        log_execution_error(name, e, data)
        raise
    finally:
        duration_ms = (time.time() - start_time) * 1000
        log_execution_end(name, success, result, duration_ms)


def trace_execution(func: Callable) -> Callable:
    """Decorator to trace function execution for debugging"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Get function name and module
        name = f"{func.__module__}.{func.__name__}"

        # Extract simple argument representation for logging
        if _trace_data:
            # Safely extract arguments for logging
            arg_data = {}
            for i, arg in enumerate(args):
                arg_data[f"arg{i}"] = str(arg)[:50] if hasattr(arg, "__str__") else type(arg).__name__
            for k, v in kwargs.items():
                arg_data[k] = str(v)[:50] if hasattr(v, "__str__") else type(v).__name__
        else:
            arg_data = None

        start_time = time.time()
        success = False
        result = None

        try:
            log_execution_start(name, arg_data)
            result = func(*args, **kwargs)
            success = True
            return result
        except Exception as e:
            log_execution_error(name, e, arg_data)
            raise
        finally:
            duration_ms = (time.time() - start_time) * 1000
            log_execution_end(name, success, result if _trace_data else None, duration_ms)

    return wrapper


def get_execution_stats() -> Dict[str, Any]:
    """Get execution statistics for monitoring"""
    return {
        "counts": dict(_execution_stats["counts"]),
        "avg_durations": {k: sum(v) / len(v) if v else 0 for k, v in _execution_stats["durations"].items()},
        "error_counts": dict(_execution_stats["errors"]),
        "last_execution": _execution_stats["last_execution"],
    }


def reset_execution_stats() -> None:
    """Reset execution statistics"""
    global _execution_stats
    _execution_stats = {"counts": {}, "durations": {}, "errors": {}, "last_execution": None}
    logger.debug("Execution statistics reset")

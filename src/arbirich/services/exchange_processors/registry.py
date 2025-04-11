import importlib
import inspect
import logging
import os
import pkgutil
import threading
from typing import Dict, Type

from src.arbirich.config.config import ALL_EXCHANGES, EXCHANGES

logger = logging.getLogger(__name__)

# Dictionary to store processor classes
_processor_registry: Dict[str, Type] = {}

# Global lock for processor registry operations
_registry_lock = threading.RLock()

# Registry of active processors by type and ID
_active_processors: Dict[str, Dict[str, any]] = {
    "websocket": {},  # WebSocket processors
    "rest": {},  # REST API processors
    "custom": {},  # Custom processors
}

# Global shutdown flag specific to processors
_processors_shutting_down = False


def register_processor(processor_type: str, processor_id: str, processor_instance: any) -> bool:
    """
    Register a processor in the global registry.
    Returns True if registered successfully, False if shutdown in progress or already exists.
    """
    with _registry_lock:
        # First check if system or processors are shutting down
        if _processors_shutting_down:
            logger.info(f"Refusing to register processor {processor_id} - system is shutting down")
            return False

        # Ensure the processor type exists in registry
        if processor_type not in _active_processors:
            _active_processors[processor_type] = {}

        # Check if processor already exists
        if processor_id in _active_processors[processor_type]:
            logger.debug(f"Processor {processor_id} already registered")
            return False

        # Register the processor
        _active_processors[processor_type][processor_id] = processor_instance
        logger.debug(f"Registered {processor_type} processor: {processor_id}")
        return True


def deregister_processor(processor_type: str, processor_id: str) -> bool:
    """
    Remove a processor from the registry.
    Returns True if it was found and removed.
    """
    with _registry_lock:
        if processor_type not in _active_processors:
            return False

        if processor_id not in _active_processors[processor_type]:
            return False

        del _active_processors[processor_type][processor_id]
        logger.debug(f"Deregistered {processor_type} processor: {processor_id}")
        return True


def is_processor_registered(processor_type: str, processor_id: str) -> bool:
    """Check if a processor is registered."""
    with _registry_lock:
        return processor_type in _active_processors and processor_id in _active_processors[processor_type]


def get_registered_processors(processor_type: str = None) -> Dict[str, Dict[str, any]]:
    """Get a copy of the registry for a specific type or all types."""
    with _registry_lock:
        if processor_type:
            return {processor_type: _active_processors.get(processor_type, {}).copy()}
        else:
            return {k: v.copy() for k, v in _active_processors.items()}


def set_processors_shutting_down(shutting_down: bool = True) -> None:
    """
    Set the processor-specific shutdown flag.
    This allows fine-grained control over processor shutdown separate from system shutdown.
    """
    global _processors_shutting_down
    with _registry_lock:
        _processors_shutting_down = shutting_down
        logger.info(f"Processor shutdown flag set to {shutting_down}")


def are_processors_shutting_down() -> bool:
    """Check if processor-specific shutdown is in progress."""
    with _registry_lock:
        return _processors_shutting_down


def register(exchange_name):
    """
    Decorator to register a processor class for a specific exchange.

    Parameters:
        exchange_name: The name of the exchange this processor handles
    """

    def decorator(processor_class):
        # Register with exact exchange name from parameter
        _processor_registry[exchange_name] = processor_class
        logger.info(f"Registered processor for exchange: {exchange_name}")
        return processor_class

    return decorator


def get_processor_class(exchange_name):
    """
    Get the processor class for a specific exchange.

    Parameters:
        exchange_name: The name of the exchange

    Returns:
        The processor class or None if not found
    """
    processor_class = _processor_registry.get(exchange_name)

    if processor_class is None:
        # Try case-insensitive match as fallback
        exchange_name_lower = exchange_name.lower()
        for name, cls in _processor_registry.items():
            if name.lower() == exchange_name_lower:
                logger.info(f"Found processor for {exchange_name} using case-insensitive match to {name}")
                return cls

    return processor_class


def list_exchange_modules():
    """
    List all potential exchange processor modules in the package.
    Helps troubleshoot registration issues.
    """
    import src.arbirich.services.exchange_processors as pkg

    module_dir = os.path.dirname(pkg.__file__)
    logger.info(f"Exchange processor directory: {module_dir}")

    existing_files = [f for f in os.listdir(module_dir) if f.endswith(".py")]
    logger.info(f"Python files in directory: {existing_files}")

    return [
        name
        for _, name, is_pkg in pkgutil.iter_modules(pkg.__path__)
        if not is_pkg
        and name
        not in ["registry", "processor_factory", "base_processor", "__init__", "default_processor", "processor_mapping"]
    ]


def register_all_processors():
    """
    Discover and register all processor classes in the package,
    filtering to only those exchanges defined in EXCHANGES config.
    """
    # Log and check registered exchanges
    configured_exchanges = list(EXCHANGES.keys() if isinstance(EXCHANGES, dict) else EXCHANGES)
    all_exchanges = list(ALL_EXCHANGES.keys() if isinstance(ALL_EXCHANGES, dict) else ALL_EXCHANGES)

    logger.info(f"Registering exchange processors for configured exchanges: {configured_exchanges}")
    logger.info(f"All available exchanges: {all_exchanges}")

    # Get all potential exchange modules
    available_modules = list_exchange_modules()
    logger.info(f"Available exchange modules: {available_modules}")

    # Check for exchange name mismatches
    for exchange in configured_exchanges:
        exchange_lower = exchange.lower()
        if exchange not in available_modules and f"{exchange_lower}_processor" not in available_modules:
            logger.warning(f"No processor module found for exchange: {exchange}")
            logger.warning(f"Looked for modules named '{exchange}' or '{exchange_lower}_processor'")

    # First, we'll import all the processor modules to trigger the decorators
    import src.arbirich.services.exchange_processors as pkg

    for _, name, is_pkg in pkgutil.iter_modules(pkg.__path__):
        # Skip certain modules
        if not is_pkg and name not in [
            "registry",
            "processor_factory",
            "base_processor",
            "__init__",
            "default_processor",
            "proccessor_mapping",
        ]:
            # Try to check if this module matches any configured exchange
            module_matches_exchange = False
            exchange_match = None

            # Check if module name directly matches an exchange name
            if name in configured_exchanges:
                module_matches_exchange = True
                exchange_match = name
            # Check if module name is a processor for a configured exchange
            else:
                for exchange in configured_exchanges:
                    # Try potential naming patterns
                    if (
                        name == f"{exchange}_processor"
                        or name == exchange.lower()
                        or name == f"{exchange.lower()}_processor"
                    ):
                        module_matches_exchange = True
                        exchange_match = exchange
                        break

            if module_matches_exchange or name in all_exchanges:
                try:
                    # Import the module to trigger the register decorators
                    module = importlib.import_module(f"src.arbirich.services.exchange_processors.{name}")
                    logger.info(f"Successfully imported processor module: {name}")

                    # Check if this module has any classes with the register decorator
                    has_processor = False
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        if inspect.isclass(attr) and attr.__module__ == module.__name__:
                            has_processor = True

                    if not has_processor and exchange_match:
                        logger.warning(f"Module {name} imported but no processor class registered for {exchange_match}")

                except Exception as e:
                    logger.error(f"Error importing processor module {name}: {e}")
            else:
                logger.debug(f"Skipping exchange processor module {name} (not in configured exchanges)")

    # Log registered processors
    if _processor_registry:
        for exchange, processor_class in _processor_registry.items():
            logger.info(f"Processor for {exchange}: {processor_class.__name__}")
    else:
        logger.warning("No processors were registered. This might indicate an import issue.")

        # Provide a more helpful message
        logger.warning("Troubleshooting steps:")
        logger.warning("1. Check that processor files exist in the src/arbirich/services/exchange_processors directory")
        logger.warning("2. Ensure processor files use @register decorator with the correct exchange name")
        logger.warning("3. Verify exchange names in EXCHANGES config match the names used in @register")
        logger.warning("4. Check for import errors in the processor modules")

    logger.info(f"Registered processors: {list(_processor_registry.keys())}")

    return _processor_registry


def shutdown_all_processors() -> None:
    """
    Initiate shutdown for all registered processors.
    This should be called before system shutdown to ensure clean termination.
    """
    # Set the shutdown flag first to prevent new registrations
    set_processors_shutting_down(True)

    logger.info("Beginning shutdown of all registered processors")

    # Get a snapshot of the registry to avoid modification during iteration
    registry_snapshot = get_registered_processors()

    # Count of processors that were shutdown
    shutdown_count = 0

    # Shutdown each processor type
    for processor_type, processors in registry_snapshot.items():
        logger.info(f"Shutting down {len(processors)} {processor_type} processors")

        for processor_id, processor in processors.items():
            try:
                logger.debug(f"Shutting down {processor_type} processor: {processor_id}")

                # Call stop or close method if available
                if hasattr(processor, "stop"):
                    processor.stop()
                elif hasattr(processor, "close"):
                    processor.close()
                elif hasattr(processor, "shutdown"):
                    processor.shutdown()

                # Deregister the processor
                deregister_processor(processor_type, processor_id)
                shutdown_count += 1

            except Exception as e:
                logger.error(f"Error shutting down {processor_type} processor {processor_id}: {e}")

    logger.info(f"Shutdown completed for {shutdown_count} processors")


# Perform registration when module is imported
# Adding this at the end ensures that the functions are defined before we try to use them
registered_processors = register_all_processors()

import asyncio
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Type

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from arbirich.core.state.system_state import is_system_shutting_down
from src.arbirich.core.trading.flows.bytewax_flows.common.redis_utils import get_redis_client
from src.arbirich.core.trading.flows.bytewax_flows.common.shutdown_utils import (
    handle_component_shutdown,
    is_force_kill_set,
    mark_force_kill,
)
from src.arbirich.services.exchange_processors.registry import (
    are_processors_shutting_down,
    deregister_processor,
    is_processor_registered,
    register_processor,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

_processor_states = {}
_processor_state_lock = threading.RLock()

_processor_startup_lock = threading.RLock()
_startup_disabled = False

PROCESSOR_STATE_ACTIVE = "active"
PROCESSOR_STATE_STOPPING = "stopping"
PROCESSOR_STATE_STOPPED = "stopped"

# Add a global lock to prevent race conditions during shutdown
_shutdown_lock = threading.Lock()

_active_consumers = {}  # Track active consumers by exchange and symbol


def reset_processor_states():
    """Reset processor states during system restart"""
    global _processor_states
    with _processor_state_lock:
        _processor_states.clear()
    logger.info("Processor states reset")


def stop_processor(exchange, symbol):
    """Stop a processor with state tracking"""
    processor_id = f"{exchange}:{symbol}"

    with _processor_state_lock:
        # Check current state
        current_state = _processor_states.get(processor_id)

        # Avoid redundant stops
        if current_state == PROCESSOR_STATE_STOPPING or current_state == PROCESSOR_STATE_STOPPED:
            logger.debug(f"Ignoring redundant stop event for {processor_id}")
            return False

        # Mark as stopping
        _processor_states[processor_id] = PROCESSOR_STATE_STOPPING

    # Now do the actual stopping
    try:
        # Your existing stop logic here
        logger.info(f"Stopping websocket consumer for {processor_id}")

        # After processor is stopped successfully
        with _processor_state_lock:
            _processor_states[processor_id] = PROCESSOR_STATE_STOPPED
        return True
    except Exception as e:
        logger.error(f"Error stopping processor {processor_id}: {e}")
        return False


def disable_processor_startup():
    """Disable creation of new processors during shutdown"""
    global _startup_disabled
    with _processor_startup_lock:
        _startup_disabled = True
    logger.info("Processor startup disabled during shutdown")


def reset_processor_startup():
    """Re-enable creation of processors after restart"""
    global _startup_disabled
    with _processor_startup_lock:
        _startup_disabled = False
    logger.info("Processor startup re-enabled")


def start_websocket_consumer(exchange: str, symbol: str):
    """
    Start a WebSocket consumer for the given exchange and symbol.
    """
    consumer_id = f"{exchange}:{symbol}"

    # Check shutdown flags first - even before acquiring the lock
    if is_system_shutting_down() or are_processors_shutting_down() or is_force_kill_set("ingestion"):
        logger.info(f"Skipping processor creation for {consumer_id} - system shutting down (early check)")
        return

    with _shutdown_lock:
        logger.debug(f"Attempting to start consumer for {consumer_id}. Shutdown flag: {is_system_shutting_down()}")

        # Double check shutdown flag inside the lock
        if (
            is_system_shutting_down()
            or are_processors_shutting_down()
            or _startup_disabled
            or is_force_kill_set("ingestion")
        ):
            logger.info(f"Skipping processor creation for {consumer_id} - system shutting down")
            return

        # Check if the consumer is already active
        if consumer_id in _active_consumers:
            logger.info(f"WebSocket consumer for {consumer_id} is already running")
            return

        # Attempt to register in the processor registry first
        if not register_processor("websocket", consumer_id, None):
            logger.info(f"Skipping processor creation for {consumer_id} - registration failed")
            return

        # At this point we know the system is not shutting down and we can proceed
        try:
            logger.info(f"Starting WebSocket consumer for {consumer_id}")
            # ...existing code to start the WebSocket consumer...
            _active_consumers[consumer_id] = True
            logger.debug(f"Consumer for {consumer_id} started successfully. Active consumers: {_active_consumers}")
        except Exception as e:
            logger.error(f"Error starting WebSocket consumer for {consumer_id}: {e}")
            # Clean up on failure
            _active_consumers.pop(consumer_id, None)
            deregister_processor("websocket", consumer_id)
            logger.debug(f"Active consumers after failure: {_active_consumers}")


def stop_all_consumers():
    """
    Stop all active WebSocket consumers.
    """
    with _shutdown_lock:
        logger.debug(f"Stopping all consumers. Active consumers before stopping: {_active_consumers}")
        try:
            logger.info("Stopping all WebSocket consumers...")
            for consumer_id in list(_active_consumers.keys()):
                try:
                    # Stop the consumer
                    logger.info(f"Stopping consumer for {consumer_id}")
                    # ...existing code to stop the consumer...
                    del _active_consumers[consumer_id]  # Remove from active consumers
                    logger.debug(f"Consumer for {consumer_id} stopped successfully.")
                except Exception as e:
                    logger.error(f"Error stopping consumer for {consumer_id}: {e}")
            logger.info("All WebSocket consumers stopped")
            logger.debug(f"Active consumers after stopping: {_active_consumers}")
        except Exception as e:
            logger.error(f"Error stopping WebSocket consumers: {e}")


def mark_ingestion_force_kill():
    """Mark ingestion flow for force kill"""
    mark_force_kill("ingestion")
    logger.warning("Ingestion force kill flag set - all processors will be stopped")

    # Stop all consumers/processors as part of force kill
    try:
        stop_all_consumers()
        reset_processor_states()
        disable_processor_startup()
    except Exception as e:
        logger.error(f"Error during ingestion force kill cleanup: {e}")


class WebsocketPartition(StatefulSourcePartition):
    """
    A Bytewax partition for handling websocket connections to exchanges.
    Each partition handles one exchange+product pair.
    """

    def __init__(self, exchange: str, product: str, processor_class: Type, stop_event=None):
        self.exchange = exchange
        self.product = product
        self.processor_class = processor_class
        self.processor = None
        self.redis_client = get_redis_client("ingestion")  # Use shared Redis client
        self.last_activity = time.time()
        self.error_backoff = 1
        self.max_backoff = 30
        self.running = False
        self.queue = asyncio.Queue()
        self.loop = None
        self.stop_event = stop_event
        self.already_notified = False  # Track if this component has been notified of shutdown
        logger.info(f"Initialized WebsocketPartition for {exchange}:{self.product}")

    def _get_or_create_event_loop(self):
        """Get the current event loop or create a new one if none exists."""
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    def next_batch(self):
        """
        Get the next batch of data from the websocket.
        This method runs in the Bytewax worker thread.
        """
        # Only check stop events if we haven't been notified
        if not self.already_notified:
            self.handle_stop_event(f"{self.exchange}:{self.product}")

        # Add explicit early check for system-wide shutdown
        if is_system_shutting_down() or are_processors_shutting_down() or is_force_kill_set("ingestion"):
            if self.running:
                logger.info(f"Shutting down active websocket for {self.exchange}:{self.product} due to system shutdown")
                self.running = False
            return []

        try:
            # Ensure websocket coroutine is running
            if not self.running:
                # Double-check shutdown status before starting
                if is_system_shutting_down() or are_processors_shutting_down() or is_force_kill_set("ingestion"):
                    logger.info(f"Skipping websocket start for {self.exchange}:{self.product} - system shutting down")
                    return []

                self.loop = self._get_or_create_event_loop()

                # Start the websocket processor - but check the result
                processor_init_result = self.loop.run_until_complete(self._initialize_processor())

                # If processor initialization returned None (due to shutdown), don't proceed
                if processor_init_result is None:
                    logger.info(f"Websocket processor initialization aborted for {self.exchange}:{self.product}")
                    return []

                # Only set running=True if we should proceed
                self.running = True

                # Start the consumer in the background
                asyncio.run_coroutine_threadsafe(self._consume_websocket(), self.loop)

            # Check for data in the queue (non-blocking)
            try:
                # Use loop.run_until_complete to handle asyncio calls from a non-async context
                order_book = self.loop.run_until_complete(asyncio.wait_for(self.queue.get(), 0.1))
                self.last_activity = time.time()
                self.error_backoff = 1  # Reset backoff on successful message
                return [(self.exchange, self.product, order_book)]
            except asyncio.TimeoutError:
                # No data available yet, return empty batch
                return []

        except Exception as e:
            # Use exponential backoff for error retries
            logger.error(f"Error in next_batch for {self.exchange}:{self.product}: {e}")
            time.sleep(min(self.error_backoff, self.max_backoff))
            self.error_backoff = min(self.error_backoff * 2, self.max_backoff)
            # Try to reconnect if necessary
            self.running = False
            return []

    async def _initialize_processor(self):
        """Initialize the websocket processor."""
        try:
            processor_id = f"{self.exchange}:{self.product}"

            # Check BOTH shutdown flags outside of any locks
            if is_system_shutting_down() or are_processors_shutting_down() or is_force_kill_set("ingestion"):
                logger.info(f"Skipping processor creation for {processor_id} - system shutting down")
                self.running = False  # Explicitly mark as not running
                return None  # Return None to indicate failure

            # Then check registry to avoid duplicates
            if is_processor_registered("websocket", processor_id):
                logger.info(f"Processor for {processor_id} already registered, skipping initialization")
                self.running = False
                return None  # Return None to indicate failure

            # Only proceed after checking startup lock
            with _processor_startup_lock:
                if (
                    _startup_disabled
                    or is_system_shutting_down()
                    or are_processors_shutting_down()
                    or is_force_kill_set("ingestion")
                ):
                    logger.info(f"Skipping processor creation for {processor_id} - startup disabled")
                    self.running = False
                    return None  # Return None to indicate failure

            # Now we're sure we can proceed
            logger.info(f"Initializing processor for {processor_id}")
            self.processor = self.processor_class(
                exchange=self.exchange,
                product=self.product,
                subscription_type="snapshot",
                use_rest_snapshot=False,
            )

            # Register the processor
            if not register_processor("websocket", processor_id, self.processor):
                logger.warning(f"Failed to register processor {processor_id}, may already be registered")
                return None  # Return None to indicate failure

            # Return True to indicate success
            return True

        except Exception as e:
            logger.error(f"Error initializing processor for {self.exchange}:{self.product}: {e}")
            self.running = False
            # Return None to indicate failure
            return None

    async def _consume_websocket(self):
        """
        Consume data from the websocket processor and put it in the queue.
        This runs as a background task in the event loop.
        """
        processor_id = f"{self.exchange}:{self.product}"
        try:
            # Double-check at the start of consumption - BEFORE any logging
            if is_system_shutting_down() or are_processors_shutting_down() or is_force_kill_set("ingestion"):
                logger.info(f"Aborting consumer for {processor_id} - system shutting down")
                self.running = False
                return

            # Only log start if we're actually going to start
            logger.info(f"Starting websocket consumer for {processor_id}")

            # Start consuming
            async for order_book in self.processor.run():
                # Check for shutdown on EACH message
                if is_system_shutting_down() or are_processors_shutting_down() or is_force_kill_set("ingestion"):
                    logger.info(f"Consumer {processor_id} detected shutdown, stopping")
                    break

                # Only check stop events if we haven't been notified
                if not self.already_notified:
                    self.handle_stop_event(processor_id)

                # Process the message
                await self.queue.put(order_book)
        except Exception as e:
            logger.error(f"Error in websocket consumer for {processor_id}: {e}")
        finally:
            # Clean up no matter what happened
            self.running = False
            deregister_processor("websocket", processor_id)
            logger.info(f"Websocket consumer for {processor_id} stopped")

    def snapshot(self):
        """Get the partition state for recovery.

        This method is required by StatefulSourcePartition abstract class.
        """
        # Return a simple state dictionary or None
        return {"exchange": self.exchange, "product": self.product, "last_activity": self.last_activity}

    def restore(self, state):
        """Restore the partition state.

        This method is required by StatefulSourcePartition abstract class.
        """
        # Restore state from the snapshot if needed
        if state and isinstance(state, dict):
            self.last_activity = state.get("last_activity", time.time())

    def handle_stop_event(self, component_id):
        """Handle a stop event with deduplication"""
        # Skip if already notified
        if self.already_notified:
            return

        # Use common component shutdown handler
        if handle_component_shutdown("ingestion", component_id):
            self.already_notified = True

    def force_stop(self):
        """Forcefully stop this WebSocket partition."""
        processor_id = f"{self.exchange}:{self.product}"
        logger.info(f"Forcefully stopping WebSocket partition for {processor_id}")
        self.running = False

        if hasattr(self, "processor") and self.processor:
            try:
                if hasattr(self.processor, "stop"):
                    self.processor.stop()
                # Deregister from the registry
                deregister_processor("websocket", processor_id)
            except Exception as e:
                logger.error(f"Error stopping processor for {processor_id}: {e}")


@dataclass
class MultiExchangeSource(FixedPartitionedSource):
    """
    A Bytewax source that creates partitions for multiple exchanges and products.
    """

    def __init__(self, exchanges_and_pairs, processor_factory, stop_event=None):
        """
        Initialize the multi-exchange source.

        Args:
            exchanges_and_pairs: Dict mapping exchange names to lists of asset pairs
            processor_factory: Function to get processor class for an exchange
            stop_event: Optional threading.Event for stopping the source
        """
        self.exchanges_and_pairs = exchanges_and_pairs
        self.processor_factory = processor_factory
        self.stop_event = stop_event
        self.logger = logging.getLogger(__name__)

    def list_parts(self) -> List[Tuple[str, str]]:
        """
        List all exchange+product partitions.
        """
        parts = []
        for exchange, products in self.exchanges_and_pairs.items():
            for product in products:
                parts.append(f"{product}_{exchange}")

        logger.info(f"List of partitions: {parts}")
        return parts

    def build_part(self, step_id: str, for_key: str, resume_state: Any) -> Optional[StatefulSourcePartition]:
        """
        Build a partition for a specific exchange+product pair.
        """
        try:
            # Parse exchange and product from the key
            parts = for_key.split("_")
            if len(parts) >= 2:
                product = parts[0]
                exchange = parts[1]
            else:
                logger.error(f"Invalid partition key format: {for_key}")
                return None

            logger.info(f"Building partition for key: {for_key}")

            # Get the processor class for this exchange
            processor_class = self.processor_factory(exchange)

            # Make sure we're returning a proper StatefulSourcePartition instance
            result = WebsocketPartition(exchange, product, processor_class, stop_event=self.stop_event)

            # Extra check to ensure we're returning the correct type
            if not isinstance(result, StatefulSourcePartition):
                logger.error(f"Build part result is not a StatefulSourcePartition: {type(result)}")
                return None

            return result
        except Exception as e:
            logger.error(f"Error building partition for {for_key}: {e}")
            return None

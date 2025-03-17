import asyncio
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from src.arbirich.flows.execution.execution_sink import execute_trade
from src.arbirich.flows.execution.execution_source import RedisExecutionSource
from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Shared resources
redis_client = RedisService()
executor = ThreadPoolExecutor(max_workers=1)
_stop_event = threading.Event()


def build_flow():
    """
    Build the execution flow.
    This flow uses a custom Redis source that yields trade opportunities,
    applies the execute_trade operator, and outputs the result.
    """
    logger.info("Building execution flow...")
    flow = Dataflow("execution")

    source = RedisExecutionSource()
    stream = op.input("redis_input", flow, source)
    logger.debug("Input stream created from RedisExecutionSource.")

    executed = op.map("execute_trade", stream, execute_trade)
    logger.debug("Applied execute_trade operator on stream.")

    op.output("stdout", executed, StdOutSink())
    logger.info("Output operator attached (StdOutSink).")
    return flow


def run_flow_with_timeout(flow, timeout=None):
    """Run the Bytewax flow with a timeout and stop capability."""
    global _stop_event
    _stop_event.clear()

    def _run_flow():
        try:
            logger.info("Starting Bytewax flow")
            cli_main(flow, workers_per_process=1)
            logger.info("Bytewax flow completed normally")
        except Exception as e:
            logger.error(f"Error in Bytewax flow execution: {e}")
        finally:
            logger.info("Bytewax flow thread exiting")

    # Start the flow in a separate thread
    flow_thread = threading.Thread(target=_run_flow, daemon=True)
    flow_thread.start()

    # Monitor the thread and handle stop events
    start_time = time.time()
    try:
        while flow_thread.is_alive():
            if _stop_event.is_set():
                logger.info("Stop event detected, breaking out of flow monitor loop")
                break

            if timeout and (time.time() - start_time > timeout):
                logger.info(f"Flow timeout of {timeout}s reached")
                break

            # Sleep briefly to avoid high CPU usage
            time.sleep(0.1)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received in flow monitor")
    except Exception as e:
        logger.error(f"Error in flow monitor: {e}")
    finally:
        # Ensure resources are cleaned up
        logger.info("Cleaning up execution flow resources")
        try:
            # We can't directly stop Bytewax, but we can clean up our resources
            redis_client.close()
            logger.info("Redis client closed")
        except Exception as e:
            logger.error(f"Error closing Redis client: {e}")

    # Let caller know if thread is still running
    return not flow_thread.is_alive()


async def run_execution_flow(strategy_name=None):
    """
    Run the execution flow in a controlled way that can be properly cancelled.

    Args:
        strategy_name: Optional strategy name for specialized execution flow
    """
    global _stop_event

    try:
        logger.info(f"Starting execution flow{' for ' + strategy_name if strategy_name else ''}...")
        flow = build_flow()
        logger.info("Execution flow built successfully")

        # Run the flow in the executor to avoid blocking the event loop
        future = executor.submit(run_flow_with_timeout, flow)

        # Monitor the future and handle cancellation
        while not future.done():
            try:
                # Check periodically if we should continue
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                logger.info("Execution flow task received cancellation signal")
                _stop_event.set()  # Signal the flow to stop
                logger.info("Waiting for flow to stop (max 5 seconds)...")

                # Give the flow a chance to stop gracefully
                try:
                    await asyncio.wait_for(asyncio.to_thread(future.result), 5.0)
                    logger.info("Flow stopped gracefully")
                except asyncio.TimeoutError:
                    logger.warning("Flow did not stop within timeout period")
                except Exception as e:
                    logger.error(f"Error waiting for flow to stop: {e}")

                # Re-raise the cancellation to let caller know we're cancelled
                raise

        # Flow completed normally
        result = future.result()
        logger.info(f"Execution flow completed with result: {result}")
        return result

    except asyncio.CancelledError:
        logger.info("Execution flow cancelled")
        raise
    except Exception as e:
        logger.exception(f"Error in execution flow: {e}")
        return False
    finally:
        logger.info("Execution flow shutdown complete")


def stop_execution_flow():
    """Signal the execution flow to stop."""
    global _stop_event
    _stop_event.set()
    logger.info("Stop signal sent to execution flow")


# Expose the flow for CLI usage.
flow = build_flow()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    asyncio.run(run_execution_flow())

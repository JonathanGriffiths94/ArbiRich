import logging
from typing import Any, Dict, Optional, Union

from src.arbirich.services.execution.execution_service import ExecutionService

# Set up logger
logger = logging.getLogger(__name__)

# Global service instance
_execution_service = None


async def get_execution_service(method_type: str = "parallel", config: Optional[Dict] = None) -> ExecutionService:
    """
    Get or create a shared ExecutionService instance.

    Args:
        method_type: The execution method type to use
        config: Configuration for the execution service

    Returns:
        An initialized ExecutionService instance
    """
    global _execution_service

    if _execution_service is None:
        logger.info(f"🔧 Creating new execution service with method {method_type}")
        _execution_service = ExecutionService(method_type=method_type, config=config or {})
        await _execution_service.initialize()
    else:
        logger.debug(f"♻️ Reusing existing execution service with method {method_type}")

    return _execution_service


async def execute_trade(
    trade_data: Union[Dict[str, Any], Any],
    position_size: float = None,
    method_type: str = "parallel",
    config: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Execute a trade using the shared execution service.

    Args:
        trade_data: Either a dictionary with trade information or a trade opportunity object
        position_size: The size/volume of the position to take
        method_type: The execution method type to use
        config: Configuration for the execution service

    Returns:
        The result of the trade execution
    """
    logger.info(f"🚀 Executing trade with method {method_type}")

    # Import TradeOpportunity for type conversion if needed
    from src.arbirich.models import TradeOpportunity

    # Convert dictionary to TradeOpportunity if needed
    if isinstance(trade_data, dict):
        try:
            trade_data = TradeOpportunity(**trade_data)
        except Exception as e:
            logger.error(f"Failed to convert trade data to TradeOpportunity: {e}")
            return {"success": False, "error": str(e)}

    # Use the ExecutionService directly
    from src.arbirich.services.execution.execution_service import ExecutionService

    # Use the new static method
    return await ExecutionService.execute_opportunity(trade_data, position_size)

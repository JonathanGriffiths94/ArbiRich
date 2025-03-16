"""
Configuration definitions for arbitrage strategies.
Each strategy has its own configuration parameters.
"""

from typing import Any, Dict, List, Tuple

# Define all available strategies
ALL_STRATEGIES: Dict[str, Dict[str, Any]] = {
    "basic_arbitrage": {
        "type": "basic",  # Use the basic strategy
        "starting_capital": 10000.0,
        "min_spread": 0.0001,  # 0.01%
        "threshold": 0.0001,  # Lower threshold for testing
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("BTC", "USDT")],
        "additional_info": {"description": "Basic arbitrage strategy looking for price differences between exchanges"},
    },
    "ETH_arbitrage": {
        "type": "basic",
        "starting_capital": 5000.0,
        "min_spread": 0.0002,  # 0.02%
        "threshold": 0.0002,  # Lower threshold for testing
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("ETH", "USDT")],
        "additional_info": {"description": "Specialized arbitrage strategy for ETH/USDT"},
    },
    "multi_asset_arbitrage": {
        "type": "basic",
        "starting_capital": 20000.0,
        "min_spread": 0.00015,  # 0.015%
        "threshold": 0.00015,  # Lower threshold for testing
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("BTC", "USDT"), ("ETH", "USDT"), ("SOL", "USDT")],
        "additional_info": {
            "description": "Multi-asset arbitrage strategy that monitors opportunities across several pairs"
        },
    },
    "mid_price_arbitrage": {
        "type": "mid_price",  # Use the mid-price strategy
        "starting_capital": 5000.0,
        "min_spread": 0.0001,  # 0.01%
        "threshold": 0.0001,  # Lower threshold for testing
        "min_depth": 5,  # Specific to mid_price strategy
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("BTC", "USDT"), ("ETH", "USDT")],
        "additional_info": {"description": "Mid-price arbitrage strategy for highly liquid pairs"},
    },
    # Example of a volume-weighted strategy
    "volume_weighted_arbitrage": {
        "type": "volume_weighted",
        "starting_capital": 15000.0,
        "min_spread": 0.0003,  # 0.03%
        "threshold": 0.0003,
        "weight_factor": 0.75,  # How much to weight volume in calculations
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("BTC", "USDT")],
        "additional_info": {"description": "Volume-weighted arbitrage that considers order depth"},
    },
}


def get_all_strategy_names() -> List[str]:
    """Get a list of all strategy names."""
    return list(ALL_STRATEGIES.keys())


def get_strategy_config(strategy_name: str) -> Dict[str, Any]:
    """
    Get configuration for a specific strategy.

    Parameters:
        strategy_name: Name of the strategy

    Returns:
        Configuration dictionary for the strategy
    """
    return ALL_STRATEGIES.get(strategy_name, {})


def get_active_strategies(selection: str) -> Dict[str, Dict[str, Any]]:
    """
    Get a dictionary of active strategies based on selection.

    Parameters:
        selection: Comma-separated list of strategy names

    Returns:
        Dictionary of active strategies {name: config}
    """
    strategy_names = [name.strip() for name in selection.split(",")]
    active_strategies = {}

    for name in strategy_names:
        if name in ALL_STRATEGIES:
            active_strategies[name] = ALL_STRATEGIES[name]
        else:
            print(f"Warning: Strategy '{name}' not found in ALL_STRATEGIES, ignoring.")

    # If no valid strategies were selected, use the first available strategy
    if not active_strategies:
        first_strategy = next(iter(ALL_STRATEGIES.keys()))
        print(f"No valid strategies selected, defaulting to '{first_strategy}'")
        active_strategies[first_strategy] = ALL_STRATEGIES[first_strategy]

    return active_strategies


def get_unique_pairs(strategies: Dict[str, Dict[str, Any]]) -> List[Tuple[str, str]]:
    """Extract unique pairs from a set of strategies."""
    pairs = []
    for config in strategies.values():
        for pair in config.get("pairs", []):
            if pair not in pairs:
                pairs.append(pair)
    return pairs


def get_unique_exchanges(strategies: Dict[str, Dict[str, Any]]) -> List[str]:
    """Extract unique exchanges from a set of strategies."""
    exchanges = []
    for config in strategies.values():
        for exchange in config.get("exchanges", []):
            if exchange not in exchanges:
                exchanges.append(exchange)
    return exchanges

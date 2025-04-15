import argparse
import json
import logging
import sys
from typing import Any, Dict, List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("strategy_inspector")

from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

# Import strategy-related modules
from src.arbirich.config.config import get_all_strategy_names, get_strategy_config
from src.arbirich.core.trading.strategy.strategy_factory import clear_strategy_cache, get_strategy


def inspect_existing_strategy(strategy_name: str) -> Dict[str, Any]:
    """Inspect an existing strategy from configuration"""
    # Get the strategy's configuration
    config = get_strategy_config(strategy_name)
    if not config:
        logger.error(f"Configuration not found for strategy: {strategy_name}")
        return {}

    # Clear strategy cache to ensure fresh instance
    clear_strategy_cache()

    # Create the strategy instance using the real configuration
    strategy = get_strategy(strategy_name)
    if not strategy:
        logger.error(f"Failed to create strategy: {strategy_name}")
        return {}

    strategy_type = config.get("type", "UNKNOWN")

    # Get strategy details
    strategy_details = {
        "name": strategy_name,
        "type": strategy_type,
        "config": config,
        "instance": strategy,
        "attributes": {},
        "methods": [],
    }

    # Collect strategy attributes and methods
    for attr_name in dir(strategy):
        if attr_name.startswith("_"):
            continue

        attr = getattr(strategy, attr_name)
        if callable(attr):
            strategy_details["methods"].append(attr_name)
        else:
            try:
                # Try to get the attribute value (might fail if it's a property that raises)
                strategy_details["attributes"][attr_name] = str(attr)
            except Exception:
                strategy_details["attributes"][attr_name] = "<unable to read>"

    return strategy_details


def print_strategy_details(details: Dict[str, Any]) -> None:
    """Print details about a strategy instance"""
    if not details:
        return

    print("\n" + "=" * 80)
    print(f"STRATEGY: {details['name']} ({details['type']})")
    print("=" * 80)

    print("\nATTRIBUTES:")
    print("-" * 40)
    for name, value in details["attributes"].items():
        print(f"{name}: {value}")

    print("\nMETHODS:")
    print("-" * 40)
    for method in sorted(details["methods"]):
        print(f"- {method}()")

    print("\nCONFIG:")
    print("-" * 40)
    print(json.dumps(details["config"], indent=2))
    print("\n")


def filter_strategies_by_type(strategy_names: List[str], strategy_type: str) -> List[str]:
    """Filter strategy names by type"""
    if strategy_type == "ALL":
        return strategy_names

    filtered_names = []
    for name in strategy_names:
        config = get_strategy_config(name)
        if config and config.get("type") == strategy_type:
            filtered_names.append(name)

    return filtered_names


def main():
    """Main function to inspect strategies"""
    parser = argparse.ArgumentParser(description="Inspect ArbiRich trading strategies")
    parser.add_argument(
        "--type",
        choices=["BASIC", "MID_PRICE", "VWAP", "LIQUIDITY_ADJUSTED", "ALL"],
        default="ALL",
        help="Strategy type to inspect",
    )
    parser.add_argument("--name", help="Specific strategy name to inspect")
    args = parser.parse_args()

    print("\nARBIRICH STRATEGY INSPECTOR")
    print("=========================\n")

    # Get all strategy names from configuration
    all_strategy_names = get_all_strategy_names()

    if not all_strategy_names:
        print("No strategies found in configuration files.")
        return

    # If a specific name is provided, only inspect that strategy
    if args.name:
        if args.name in all_strategy_names:
            try:
                details = inspect_existing_strategy(args.name)
                print_strategy_details(details)
            except Exception as e:
                logger.error(f"Error inspecting strategy '{args.name}': {e}", exc_info=True)
        else:
            print(f"Strategy '{args.name}' not found in configuration.")
        return

    # Otherwise filter by type and inspect all matching strategies
    filtered_names = filter_strategies_by_type(all_strategy_names, args.type)

    if not filtered_names:
        print(f"No strategies of type '{args.type}' found in configuration.")
        return

    for name in filtered_names:
        try:
            details = inspect_existing_strategy(name)
            print_strategy_details(details)
        except Exception as e:
            logger.error(f"Error inspecting strategy '{name}': {e}", exc_info=True)

    # Summary
    print("\nSTRATEGY SUMMARY:")
    print("=" * 80)
    print(f"Total strategies: {len(all_strategy_names)}")
    print(f"Strategies of type '{args.type}': {len(filtered_names)}")
    print("Names: " + ", ".join(filtered_names))


if __name__ == "__main__":
    main()

import asyncio
import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional

from src.arbirich.core.config.validator import (
    BasicStrategyConfig,
    ConfigValidator,
    DetectionComponentConfig,
    ExecutionComponentConfig,
    IngestionComponentConfig,
    MidPriceStrategyConfig,
    ReportingComponentConfig,
)

# Singleton instance and lock
_trading_service_instance = None
_instance_lock = threading.RLock()


def get_trading_service():
    """Get or create the singleton TradingService instance"""
    global _trading_service_instance
    with _instance_lock:
        if _trading_service_instance is None:
            _trading_service_instance = TradingService()
        return _trading_service_instance


class TradingService:
    """
    Main trading service that coordinates all trading components
    """

    def __new__(cls):
        """Ensure only one instance exists"""
        global _trading_service_instance
        with _instance_lock:
            if _trading_service_instance is None:
                _trading_service_instance = super(TradingService, cls).__new__(cls)
            return _trading_service_instance

    def __init__(self):
        # Only initialize once
        if hasattr(self, "logger"):
            return

        self.logger = logging.getLogger(__name__)
        self._initialized = False

        # Components
        self.components = {}

        # Strategy registry
        self.strategies = {}

        # Database connection
        self.db = None

    async def initialize(self, db=None):
        """Initialize the trading system with data from the database"""
        try:
            self.logger.info("TradingService initialization started")
            self._initialized = True

            # Set up database connection
            await self._initialize_db(db)

            # Initialize components
            await self._initialize_components()

            # Load strategies
            await self._load_strategies()

            # Initialize timing and status fields
            self.start_time = None
            self.stop_time = None
            self.stop_reason = None
            self.emergency_stop = False
            self.active_pairs = []
            self.active_exchanges = []

            self.logger.info(f"Loaded {len(self.strategies)} strategies")
            self.logger.info("TradingService initialization completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error initializing trading service: {e}", exc_info=True)
            self._initialized = False
            return False

    async def _initialize_db(self, db=None):
        """Initialize database connection"""
        try:
            # If no db is provided, try to create one
            if db is None:
                self.logger.info("No database provided, creating new connection")
                from src.arbirich.services.database.database_service import DatabaseService

                self.db = DatabaseService()
            else:
                self.db = db

        except Exception as e:
            self.logger.error(f"Error initializing database: {e}", exc_info=True)
            raise

    async def _initialize_components(self):
        """Initialize trading components"""
        try:
            # Check if component modules exist
            components_found = True
            try:
                # Use absolute imports to ensure modules are found correctly
                import sys

                self.logger.info(f"Python path: {sys.path}")

                # Try importing with explicit paths
                from src.arbirich.core.trading.components.detection import DetectionComponent
                from src.arbirich.core.trading.components.execution import ExecutionComponent
                from src.arbirich.core.trading.components.ingestion import IngestionComponent
                from src.arbirich.core.trading.components.reporting import ReportingComponent

                self.logger.info("Successfully imported all component modules")
            except ImportError as e:
                components_found = False
                self.logger.warning(f"Trading component modules not found: {e}. Creating stub components.")

            # If components weren't found, create stub component classes
            if not components_found:
                from src.arbirich.core.trading.components.base import Component

                class StubComponent(Component):
                    """Stub component for use when actual components aren't available"""

                    async def start(self) -> bool:
                        self.active = True
                        self.logger.info(f"Stub {self.name} component started")
                        return True

                    async def stop(self) -> bool:
                        self.active = False
                        self.logger.info(f"Stub {self.name} component stopped")
                        return True

                    def get_status(self):
                        return {"active": self.active, "stub": True, "name": self.name}

                class IngestionComponent(StubComponent):
                    pass

                class DetectionComponent(StubComponent):
                    pass

                class ExecutionComponent(StubComponent):
                    pass

                class ReportingComponent(StubComponent):
                    pass

            # Create components with default configs
            self.components = {
                "ingestion": IngestionComponent("ingestion", self._get_component_config("ingestion")),
                "detection": DetectionComponent("detection", self._get_component_config("detection")),
                "execution": ExecutionComponent("execution", self._get_component_config("execution")),
                "reporting": ReportingComponent("reporting", self._get_component_config("reporting")),
            }

            self.logger.info(f"Initialized {len(self.components)} components")

        except Exception as e:
            self.logger.error(f"Error initializing components: {e}", exc_info=True)
            # Initialize to empty dictionary to avoid NoneType errors
            self.components = {}
            raise

    def _get_component_config(self, component_name: str) -> Dict[str, Any]:
        """Get configuration for a component"""
        # This would typically load from a config file or database
        # For now, return default configs
        configs = {
            "ingestion": {"update_interval": 1.0, "buffer_size": 100},
            "detection": {"scan_interval": 0.5, "opportunity_threshold": 0.001},
            "execution": {
                "listen_interval": 0.1,
                "max_concurrent_executions": 3,
                "execution_timeout": 10.0,
                "dry_run": False,  # Set to True for testing without real trades
            },
            "reporting": {"persistence_interval": 60, "health_check_interval": 300, "report_generation_interval": 3600},
        }

        config = configs.get(component_name, {})

        # Validate the config using Pydantic models
        try:
            if component_name == "detection":
                return DetectionComponentConfig(**config).model_dump()
            elif component_name == "execution":
                return ExecutionComponentConfig(**config).model_dump()
            elif component_name == "reporting":
                return ReportingComponentConfig(**config).model_dump()
            elif component_name == "ingestion":
                return IngestionComponentConfig(**config).model_dump()
            else:
                return config
        except Exception as e:
            self.logger.warning(f"Error validating {component_name} config: {e}. Using raw config instead.")
            return config

    async def _load_strategies(self):
        """Load strategies from the database."""
        try:
            # Get active strategies from the database service
            if not hasattr(self.db, "get_active_strategies"):
                raise AttributeError("DatabaseService does not have 'get_active_strategies' method")

            strategies = self.db.get_active_strategies()

            if not strategies:
                self.logger.warning("No active strategies found in database")
                return []

            loaded_strategies = []
            for strategy in strategies:
                try:
                    strategy_id = str(strategy.id)
                    # Preprocess strategy data
                    if hasattr(strategy, "additional_info") and isinstance(strategy.additional_info, str):
                        import json

                        try:
                            strategy.additional_info = json.loads(strategy.additional_info)
                        except json.JSONDecodeError:
                            strategy.additional_info = {}

                    # Create a dict to track this strategy
                    self.strategies[strategy_id] = {
                        "id": strategy_id,
                        "name": strategy.name,
                        "active": strategy.is_active,
                        "config": strategy.additional_info or {},
                        "task": None,
                        "instance": None,
                    }

                    self.logger.info(f"Loaded strategy: {strategy.name} (ID: {strategy.id})")
                    loaded_strategies.append(strategy)
                except Exception as e:
                    self.logger.error(f"Error processing strategy {strategy.name}: {e}")
                    continue

            return loaded_strategies
        except Exception as e:
            self.logger.warning(f"Error loading strategies from database: {e}", exc_info=True)
            return []

    async def get_status(self) -> Dict[str, Any]:
        """Get the current status of all system components"""
        component_status = {}

        # Get status from each component
        for name, component in self.components.items():
            component_status[name] = {
                "active": getattr(component, "active", False),
                "status": component.get_status() if hasattr(component, "get_status") else {},
            }

        # Get strategy status
        strategy_status = {}
        active_strategy_names = []
        for strategy_id, strategy in self.strategies.items():
            strategy_status[strategy_id] = {
                "name": strategy["name"],
                "active": strategy["active"],
                "running": strategy["task"] is not None and not strategy["task"].done() if strategy["task"] else False,
            }
            if strategy["active"]:
                active_strategy_names.append(strategy["name"])

        # Collect active pairs and exchanges
        active_pairs = getattr(self, "active_pairs", [])
        active_exchanges = getattr(self, "active_exchanges", [])

        # Get current active pairs and exchanges from strategies if not already set
        if not active_pairs or not active_exchanges:
            active_pairs = []
            active_exchanges = []

            # Get pairs and exchanges from active strategies
            for strategy_id, strategy in self.strategies.items():
                if strategy["active"]:
                    try:
                        from src.arbirich.config.config import get_strategy_config

                        config = get_strategy_config(strategy["name"])
                        if config:
                            # Add exchanges to active list if not already there
                            for exchange in config.get("exchanges", []):
                                if exchange not in active_exchanges:
                                    active_exchanges.append(exchange)

                            # Add pairs to active list if not already there
                            for pair in config.get("pairs", []):
                                if pair not in active_pairs:
                                    active_pairs.append(pair)
                    except Exception as e:
                        self.logger.warning(f"Error getting config for strategy {strategy['name']}: {e}")

            # Store these for future reference
            self.active_pairs = active_pairs
            self.active_exchanges = active_exchanges

        # Get timing information with proper formatting
        trading_start_time = getattr(self, "start_time", None)
        trading_start_time_str = datetime.fromtimestamp(trading_start_time).isoformat() if trading_start_time else None

        trading_stop_time = getattr(self, "stop_time", None)
        trading_stop_time_str = datetime.fromtimestamp(trading_stop_time).isoformat() if trading_stop_time else None

        trading_stop_reason = getattr(self, "stop_reason", None)
        emergency_stop = getattr(self, "emergency_stop", False) if trading_stop_time else None

        # Overall system status
        overall_active = all(status["active"] for status in component_status.values())

        return {
            "overall": overall_active,
            "components": component_status,
            "strategies": strategy_status,
            "active_pairs": active_pairs,
            "active_exchanges": active_exchanges,
            "start_time": trading_start_time_str,
            "stop_time": trading_stop_time_str,
            "stop_reason": trading_stop_reason,
            "emergency_stop": emergency_stop,
        }

    async def start_all(self, activate_strategies=False) -> bool:
        """Start all system components"""
        try:
            self.logger.info("Starting all system components")
            self.start_time = time.time()
            self.stop_time = None
            self.stop_reason = None
            self.emergency_stop = False

            # If requested, activate strategies in database
            if activate_strategies:
                self.logger.info("Activating all strategies in database")
                await self._activate_all_strategies_in_db()

            # Start components in order
            success = True
            for component_name in ["reporting", "execution", "detection", "ingestion"]:
                if not await self.start_component(component_name):
                    success = False
                    self.logger.error(f"Failed to start component: {component_name}")

            if success:
                self.logger.info("All system components started successfully")
            else:
                self.logger.warning("Some components failed to start")

            return success

        except Exception as e:
            self.logger.error(f"Error starting all components: {e}", exc_info=True)
            return False

    async def stop_all(self, emergency=False, reason="user_requested") -> bool:
        """Stop all system components with extra cleanup"""
        self.logger.info(f"Stopping all system components. Emergency: {emergency}, Reason: {reason}")

        # Record stop information
        self.stop_time = time.time()
        self.stop_reason = reason
        self.emergency_stop = emergency

        # Set system-wide shutdown flag
        try:
            from arbirich.core.state.system_state import mark_system_shutdown

            mark_system_shutdown(True)
            self.logger.info("System-wide shutdown flag set")
        except Exception as e:
            self.logger.error(f"Error setting system shutdown flag: {e}")

        # Stop components in reverse order
        success = True
        for component_name in ["ingestion", "detection", "execution", "reporting"]:
            if not await self.stop_component(component_name):
                success = False
                self.logger.error(f"Failed to stop component: {component_name}")

        # Give components time to clean up
        self.logger.info("Waiting for components to finish cleanup...")
        await asyncio.sleep(2.5)

        # Clean up any orphaned processes
        self._kill_orphaned_processes()

        # Close Redis connections
        try:
            from src.arbirich.services.redis.redis_service import RedisService

            await RedisService.close_all_connections()
        except Exception as e:
            self.logger.error(f"Error closing Redis connections: {e}")

        # Reset component states
        self._reset_all_component_states()
        self.logger.info("All component states reset to inactive")

        if success:
            self.logger.info("All system components stopped successfully")
        else:
            self.logger.warning("Some components failed to stop cleanly")

        return success

    def _reset_all_component_states(self):
        """Reset all component states to inactive"""
        # Reset component states
        for component_name, component in self.components.items():
            component.active = False
            if hasattr(component, "task"):
                component.task = None

        # Reset strategy states
        for strategy_id in self.strategies:
            self.strategies[strategy_id]["active"] = False
            self.strategies[strategy_id]["task"] = None
            self.strategies[strategy_id]["instance"] = None

        self.logger.info("All component and strategy states reset to inactive")

    async def restart_all_components(self) -> Dict[str, Any]:
        """Restart all trading components with clean state"""
        self.logger.info("Restarting all trading components with clean state...")

        # First stop everything
        await self.stop_all()

        # Reset system state
        try:
            from arbirich.core.state.system_state import mark_system_shutdown, reset_notification_state

            mark_system_shutdown(False)
            reset_notification_state()
        except Exception as e:
            self.logger.error(f"Error resetting system state: {e}")

        # Re-initialize service
        self.logger.info("Refreshing trading service state from database...")
        self.strategies = {}  # Clear existing strategy data
        await self.initialize()  # Re-initialize (will reload strategies from DB)

        # Reset Redis connections
        try:
            from src.arbirich.services.redis.redis_service import reset_redis_pool

            reset_redis_pool()
            self.logger.info("Redis connection pools reset")
        except Exception as e:
            self.logger.error(f"Error resetting Redis pools: {e}")

        # Wait a moment to ensure clean state
        await asyncio.sleep(1)

        # Now start everything fresh
        self.logger.info("Starting all components with fresh state...")
        success = await self.start_all()

        self.logger.info("All components restarted")
        return {"status": "restarted" if success else "partial_restart", "components": list(self.components.keys())}

    async def start_component(self, component_name: str) -> bool:
        """Start a specific component"""
        if component_name not in self.components:
            self.logger.error(f"Unknown component: {component_name}")
            return False

        component = self.components[component_name]
        if component.active:
            self.logger.info(f"Component {component_name} is already active")
            return True

        self.logger.info(f"Starting component: {component_name}")

        try:
            success = await component.start()
            return success
        except Exception as e:
            self.logger.error(f"Error starting component {component_name}: {e}", exc_info=True)
            return False

    async def stop_component(self, component_name: str) -> bool:
        """Stop a specific component"""
        if component_name not in self.components:
            self.logger.error(f"Unknown component: {component_name}")
            return False

        component = self.components[component_name]
        if not component.active:
            self.logger.info(f"Component {component_name} is not active")
            return True

        self.logger.info(f"Stopping component: {component_name}")

        try:
            success = await component.stop()
            return success
        except Exception as e:
            self.logger.error(f"Error stopping component {component_name}: {e}", exc_info=True)
            component.active = False
            if hasattr(component, "task"):
                component.task = None
            return False

    async def start_strategy(self, strategy_id: str) -> bool:
        """Start a specific strategy"""
        # First try to find by ID
        if strategy_id in self.strategies:
            strategy = self.strategies[strategy_id]
        else:
            # Try to find by name as a fallback
            strategy = None
            for s_id, s_info in self.strategies.items():
                if s_info.get("name") == strategy_id:
                    strategy = s_info
                    strategy_id = s_id
                    self.logger.info(f"Found strategy by name: {strategy_id}")
                    break

            if not strategy:
                self.logger.error(f"Unknown strategy: {strategy_id}")
                return False

        if strategy["active"] and strategy["task"] and not strategy["task"].done():
            self.logger.info(f"Strategy {strategy['name']} is already active")
            return True

        self.logger.info(f"Starting strategy: {strategy['name']} (ID: {strategy_id})")

        try:
            # Update database
            if hasattr(self.db, "execute") and asyncio.iscoroutinefunction(self.db.execute):
                await self.db.execute("UPDATE strategies SET is_active = TRUE WHERE id = :id", {"id": strategy_id})
            else:
                # Fallback to SQLAlchemy
                import sqlalchemy as sa

                with self.db.engine.connect() as conn:
                    conn.execute(sa.text("UPDATE strategies SET is_active = TRUE WHERE id = :id"), {"id": strategy_id})
                    conn.commit()

            # Create strategy instance if needed
            if not strategy["instance"]:
                strategy["instance"] = await self._create_strategy(
                    strategy_id, strategy["name"], strategy.get("config", {})
                )

            if not strategy["instance"]:
                self.logger.error(f"Failed to create strategy instance for {strategy['name']}")
                return False

            # Start the strategy
            if hasattr(strategy["instance"], "start") and asyncio.iscoroutinefunction(strategy["instance"].start):
                await strategy["instance"].start()
                strategy["active"] = True

                # Update active pairs and exchanges
                try:
                    from src.arbirich.config.config import get_strategy_config

                    config = get_strategy_config(strategy["name"])
                    if config:
                        # Add exchanges to active list if not already there
                        for exchange in config.get("exchanges", []):
                            if exchange not in getattr(self, "active_exchanges", []):
                                self.active_exchanges = getattr(self, "active_exchanges", []) + [exchange]

                        # Add pairs to active list if not already there
                        for pair in config.get("pairs", []):
                            if pair not in getattr(self, "active_pairs", []):
                                self.active_pairs = getattr(self, "active_pairs", []) + [pair]
                except Exception as e:
                    self.logger.warning(f"Error updating active pairs/exchanges for strategy {strategy['name']}: {e}")

                return True
            else:
                self.logger.error(f"Strategy {strategy['name']} has no start method")
                return False

        except Exception as e:
            self.logger.error(f"Error starting strategy {strategy['name']}: {e}", exc_info=True)
            return False

    async def stop_strategy(self, strategy_id: str) -> bool:
        """Stop a specific strategy"""
        if strategy_id not in self.strategies:
            self.logger.error(f"Unknown strategy: {strategy_id}")
            return False

        strategy = self.strategies[strategy_id]
        if not strategy["active"]:
            self.logger.info(f"Strategy {strategy['name']} is not active")
            return True

        self.logger.info(f"Stopping strategy: {strategy['name']} (ID: {strategy_id})")

        try:
            # Update database
            if hasattr(self.db, "execute") and asyncio.iscoroutinefunction(self.db.execute):
                await self.db.execute("UPDATE strategies SET is_active = FALSE WHERE id = :id", {"id": strategy_id})
            else:
                # Fallback to SQLAlchemy
                import sqlalchemy as sa

                with self.db.engine.connect() as conn:
                    conn.execute(sa.text("UPDATE strategies SET is_active = FALSE WHERE id = :id"), {"id": strategy_id})
                    conn.commit()

            # Stop the strategy instance
            if (
                strategy["instance"]
                and hasattr(strategy["instance"], "stop")
                and asyncio.iscoroutinefunction(strategy["instance"].stop)
            ):
                await strategy["instance"].stop()

            strategy["active"] = False

            # Update active pairs and exchanges list by recalculating
            self._recalculate_active_resources()

            return True

        except Exception as e:
            self.logger.error(f"Error stopping strategy {strategy['name']}: {e}", exc_info=True)
            strategy["active"] = False
            return False

    def _recalculate_active_resources(self):
        """Recalculate active pairs and exchanges based on currently active strategies"""
        active_pairs = []
        active_exchanges = []

        for strategy_id, strategy in self.strategies.items():
            if strategy["active"]:
                try:
                    from src.arbirich.config.config import get_strategy_config

                    config = get_strategy_config(strategy["name"])
                    if config:
                        # Add exchanges to active list if not already there
                        for exchange in config.get("exchanges", []):
                            if exchange not in active_exchanges:
                                active_exchanges.append(exchange)

                        # Add pairs to active list if not already there
                        for pair in config.get("pairs", []):
                            if pair not in active_pairs:
                                active_pairs.append(pair)
                except Exception as e:
                    self.logger.warning(f"Error getting config for strategy {strategy['name']}: {e}")

        # Update stored values
        self.active_pairs = active_pairs
        self.active_exchanges = active_exchanges

    async def _create_strategy(self, strategy_id: str, strategy_name: str, config: Dict[str, Any]) -> Optional[Any]:
        """Create a strategy instance based on configuration"""
        try:
            from src.arbirich.core.trading.strategy.types.basic import BasicArbitrage
            from src.arbirich.core.trading.strategy.types.mid_price import MidPriceArbitrage

            strategy_type = config.get("type")

            errors = ConfigValidator.validate_strategy_config(strategy_type, config)
            if errors:
                self.logger.error(f"Invalid configuration for strategy {strategy_name}: {errors}")
                return None

            if strategy_type == "basic":
                validated_config = BasicStrategyConfig(**config).model_dump()
                return BasicArbitrage(strategy_id=strategy_id, name=strategy_name, config=validated_config)
            elif strategy_type == "mid_price":
                validated_config = MidPriceStrategyConfig(**config).model_dump()
                return MidPriceArbitrage(strategy_id=strategy_id, name=strategy_name, config=validated_config)
            else:
                self.logger.warning(f"Unknown strategy type: {strategy_type}")
                return None

        except Exception as e:
            self.logger.error(f"Error creating strategy {strategy_name}: {e}", exc_info=True)
            return None

    def _kill_orphaned_processes(self):
        """Kill any orphaned bytewax processes that didn't shut down properly"""
        try:
            import os
            import signal
            import subprocess

            self.logger.info("Checking for orphaned bytewax processes")

            # Find all Python processes running bytewax
            result = subprocess.run(["ps", "-ef"], capture_output=True, text=True)
            killed_count = 0

            for line in result.stdout.splitlines():
                # Look for python processes that contain both "bytewax" and "arbirich"
                if "python" in line and "bytewax" in line and "arbirich" in line:
                    parts = line.split()
                    if len(parts) > 1:
                        try:
                            pid = int(parts[1])
                            # Don't kill our own process or parent processes
                            if pid != os.getpid() and pid != os.getppid():
                                self.logger.info(f"Killing orphaned bytewax process: {pid}")
                                # Use SIGKILL for guaranteed termination
                                os.kill(pid, signal.SIGKILL)
                                killed_count += 1
                        except (ValueError, ProcessLookupError) as e:
                            self.logger.error(f"Failed to parse or kill process from line: {line}, error: {e}")

            if killed_count > 0:
                self.logger.warning(f"Killed {killed_count} orphaned bytewax processes")
            else:
                self.logger.info("No orphaned bytewax processes found")

        except Exception as e:
            self.logger.error(f"Error checking for orphaned processes: {e}", exc_info=True)

    async def _activate_all_strategies_in_db(self):
        """Activate all strategies in the database"""
        try:
            if hasattr(self.db, "execute") and asyncio.iscoroutinefunction(self.db.execute):
                await self.db.execute("UPDATE strategies SET is_active = TRUE")
            else:
                # Fallback to SQLAlchemy
                import sqlalchemy as sa

                with self.db.engine.connect() as conn:
                    conn.execute(sa.text("UPDATE strategies SET is_active = TRUE"))
                    conn.commit()

            self.logger.info("Activated all strategies in database")
            return True
        except Exception as e:
            self.logger.error(f"Error activating strategies in database: {e}", exc_info=True)
            return False

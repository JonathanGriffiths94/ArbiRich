import logging
from typing import Any, Callable, Optional

from bytewax.outputs import Sink

logger = logging.getLogger(__name__)


class LoggingSink(Sink):
    """
    A custom sink for Bytewax that logs outputs instead of writing to stdout.

    This can be configured to use different log levels or formatting functions.
    """

    def __init__(
        self, name: str = "default", log_level: int = logging.DEBUG, formatter: Optional[Callable[[Any], str]] = None
    ):
        """
        Initialize the logging sink.

        Args:
            name: A name for this sink, used in logs
            log_level: The logging level to use
            formatter: Optional function to format items before logging
        """
        self.name = name
        self.log_level = log_level
        self.formatter = formatter or (lambda x: f"{x}")

    def write(self, item):
        """Log the item at the configured level."""
        logger.log(self.log_level, f"[{self.name}] {self.formatter(item)}")

    def close(self):
        """Log that the sink is being closed."""
        logger.debug(f"Closing {self.name} logging sink")


class NullSink(Sink):
    """A sink that completely discards all output."""

    def write(self, item):
        """Discard the item."""
        pass

    def close(self):
        """No-op close method."""
        pass


class ConditionalSink(Sink):
    """
    A sink that decides where to send output based on a condition.

    This allows dynamically routing items to different sinks.
    """

    def __init__(self, primary_sink, alternate_sink=None, condition: Callable[[Any], bool] = lambda _: True):
        """
        Initialize the conditional sink.

        Args:
            primary_sink: The default sink to use when condition is True
            alternate_sink: The sink to use when condition is False
            condition: Function that evaluates whether to use primary sink
        """
        self.primary_sink = primary_sink
        self.alternate_sink = alternate_sink or NullSink()
        self.condition = condition

    def write(self, item):
        """Write item to appropriate sink based on condition."""
        if self.condition(item):
            self.primary_sink.write(item)
        else:
            self.alternate_sink.write(item)

    def close(self):
        """Close both sinks."""
        self.primary_sink.close()
        self.alternate_sink.close()

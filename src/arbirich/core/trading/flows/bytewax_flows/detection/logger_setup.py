import logging
import sys


def setup_detection_logger():
    """
    Set up properly configured loggers for the detection module.
    Fixes formatting issues and ensures consistent logging.
    """
    # Configure root logger if not already configured
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        # Fix the formatter string - there was a '%s' instead of 's' in the name format
        handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

    # Configure detection sink logger
    detection_sink_logger = logging.getLogger("src.arbirich.core.trading.flows.bytewax_flows.detection.detection_sink")
    detection_sink_logger.setLevel(logging.INFO)

    # Configure detection process logger
    detection_process_logger = logging.getLogger(
        "src.arbirich.core.trading.flows.bytewax_flows.detection.detection_process"
    )
    detection_process_logger.setLevel(logging.INFO)

    # Configure detection source logger
    detection_source_logger = logging.getLogger(
        "src.arbirich.core.trading.flows.bytewax_flows.detection.detection_source"
    )
    detection_source_logger.setLevel(logging.INFO)

    return {
        "detection_sink": detection_sink_logger,
        "detection_process": detection_process_logger,
        "detection_source": detection_source_logger,
    }

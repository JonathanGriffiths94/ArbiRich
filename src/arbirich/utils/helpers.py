import logging

from src.arbirich.config.config import EXCHANGE_CONFIGS, EXCHANGES, PAIRS

logger = logging.getLogger(__name__)


def build_exchanges_dict() -> dict:
    """
    Builds dictionary of exchanges with a list of formatted symbols to
    build partitions for data processing
    """
    exchanges_dict = {}
    for exch in EXCHANGES:
        config = EXCHANGE_CONFIGS.get(exch)
        if not config:
            continue
        delimiter = "-"
        formatted_assets = [delimiter.join(asset) for asset in PAIRS]
        exchanges_dict[exch] = formatted_assets
    return exchanges_dict

from arbirich.market_data_service import MarketDataService


class ArbitrageScanner:
    def __init__(self):
        # Use MarketDataService for all Redis interactions
        self.market_data_service = MarketDataService()

    def process_opportunity(self, opportunity):
        """
        Compare the current prices across exchanges and, if an arbitrage opportunity is detected,
        publish a trade signal.
        """
        exchanges = ["Binance", "Bybit", "KuCoin"]
        pair = "BTC/USDT"
        opportunities = []

        # Get prices for the given pair from all exchanges
        prices = {
            exchange: self.market_data_service.get_price(exchange, pair)
            for exchange in exchanges
        }

        for buy_exchange in exchanges:
            for sell_exchange in exchanges:
                if (
                    buy_exchange != sell_exchange
                    and prices.get(buy_exchange)
                    and prices.get(sell_exchange)
                ):
                    buy_price = prices[buy_exchange]["price"]
                    sell_price = prices[sell_exchange]["price"]
                    spread = (sell_price - buy_price) / buy_price

                    if spread > 0.001:  # Threshold for opportunity
                        trade_signal = {
                            "buy_exchange": buy_exchange,
                            "sell_exchange": sell_exchange,
                            "pair": pair,
                            "buy_price": buy_price,
                            "sell_price": sell_price,
                            "spread": spread,
                        }
                        self.publish_trade_signal(trade_signal)
                        opportunities.append(trade_signal)

        return opportunities

    def publish_trade_signal(self, trade_signal):
        """
        Publishes a trade signal (opportunity) using the MarketDataService.
        """
        self.market_data_service.publish_trade_opportunity(trade_signal)
        logger.info(f"Published arbitrage opportunity: {trade_signal}")

    def listen_for_opportunities(self):
        """
        Subscribes to the 'trade_opportunity' channel using the MarketDataService and processes incoming opportunities.
        """

        def callback(data):
            try:
                # 'data' is already JSON-decoded by MarketDataService.subscribe_to_trade_opportunities
                logger.info(f"Received trade opportunity: {data}")
                self.process_opportunity(data)
            except Exception as e:
                logger.error(f"Error processing trade opportunity: {e}")

        logger.info("Subscribed to 'trade_opportunity' channel via MarketDataService.")
        self.market_data_service.subscribe_to_trade_opportunities(callback)

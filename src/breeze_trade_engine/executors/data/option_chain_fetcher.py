import logging
from breeze_trade_engine.executors.base_executor import BaseExecutor, Singleton


class OptionChainDataFetcher(BaseExecutor, Singleton):

    def _init(self, name, start_time, end_time, interval):
        super().__init__(name, start_time, end_time, interval)
        self.logger = logging.getLogger(__name__)

    # TODO: Think of subscription and notification mechanism for subscribers

    def process_day_begin(self):
        # TODO: Add day begin initialization logic here
        self.logger.info("Day begin logic executed.")

    def process_day_end(self):
        # TODO: Add day end cleanup logic here (move csv to paraquet, and delete csv)
        self.logger.info("Day end logic executed.")

    def process_event(self):
        # TODO: Add main processing logic here for each tick or data arrival in livefeed
        # in Option chain data fetcher, this will be the logic to fetch the option chain data
        # using functions like get_nexy_nifty_expiry_date() get_option_chain()
        pass

    def get_next_nifty_expiry_date():
        """
        Use this function to return the next weekly Nifty expiry date
        Take into account the holidays
        """
        pass

    def get_option_chain():
        """
        Use this function to return the live option chain data for a given symbol and expirty date
        at current time
        All breeze calls to be abstracted in common package
        """
        pass

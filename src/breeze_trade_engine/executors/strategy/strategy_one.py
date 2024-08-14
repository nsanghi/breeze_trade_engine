import logging
from breeze_trade_engine.common.file_utils import FileWriterMixin
from breeze_trade_engine.executors.base_executor import BaseExecutor, Singleton


class StrategyOneExecutor(Singleton, BaseExecutor, FileWriterMixin):

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
        # In strategy this will be arrival of new data from live feed or market data fetch request/response
        # each tick logic to calculate VS, VSSMA, trade signal amd place order
        # also calculate delta hedging and place order
        # think of logic to store orders in flight and at rest
        # think of another standalong utlitity or process to periodically poll risk mgmt signals
        pass

    ### Other functions can be added here if needed for historical data fetching

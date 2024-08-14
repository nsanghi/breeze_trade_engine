import logging
from breeze_trade_engine.common.file_utils import FileWriterMixin
from breeze_trade_engine.executors.base_executor import BaseExecutor, Singleton


class LiveFeed(Singleton, BaseExecutor, FileWriterMixin):

    def _init(self, name, start_time, end_time, interval):
        BaseExecutor.__init__(self, name, start_time, end_time, interval)
        # FileWriterMixin.__init__(self)
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
        # connecting to breeze feed needs to be abstracted to common package
        pass

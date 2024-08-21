import asyncio
from datetime import datetime
import logging
import os
from breeze_trade_engine.common.date_utils import get_next_weekly_expiry
from breeze_trade_engine.executors.async_base_executor import (
    Singleton,
    AsyncBaseExecutor,
    Subscriber,
)
from breeze_trade_engine.common.file_utils import write_to_csv, write_to_parquet
from breeze_trade_engine.provider.breeze import BreezeData

MAX_CONSECUTIVE_FAILURES = 30


class OptionChainDataFetcher(Singleton, AsyncBaseExecutor, Subscriber):

    def __init__(self, name, start_time, end_time, interval):
        AsyncBaseExecutor.__init__(self, name, start_time, end_time, interval)
        self.logger = logging.getLogger(__name__)
        self.consecutive_failures = 0
        self.file = None
        self.conn = BreezeData()

    # TODO: Write test cases for this class also check actual data fetch
    # TODO: Think of subscription and notification mechanism for subscribers

    async def process_day_begin(self):
        # implemented the abstract method
        # refresh bezze connection
        self.conn.refresh()
        # Create a filename for the CSV file based on the current date
        today = datetime.now().strftime("%Y-%m-%d")
        data_path = os.environ.get("DATA_PATH")
        self.file = f"{data_path}/chain/nifty_chain_{today}.csv"
        self.logger.info("process_day_begin executed.")

    async def process_day_end(self):
        # Write the csv to parquet and delete csv
        write_to_parquet(
            self.file, self.logger, delete_csv=True
        )  # not made async as it is day end
        self.file = None  # remove reference to today's file
        self.logger.info("process_day_end executed.")

    async def process_event(self):
        # implemented the abstract method
        # Main processing logic to fire live quote fetch from Breeze and write to csv
        self.handle_consecutive_failures()
        quotes = await self._get_option_chain()
        if quotes and len(quotes) > 0:
            # await asyncio.get_running_loop().run_in_executor(
            #     self.executor, write_to_csv, quotes, self.file, self.logger
            # )
            # replaced above with more modern idiomaic way
            await asyncio.to_thread(
                write_to_csv, quotes, self.file, self.logger
            )

            await self.notify_subscribers("default", quotes)
        else:
            self.logger.error("No quotes fetched.")
            self.consecutive_failures += 1

    async def _get_option_chain(self):
        """
        Use this function to return the live option chain data for a given symbol
        and expirty date at current time
        All breeze calls to be abstracted in common package
        """
        expiry_date = get_next_weekly_expiry()
        # loop = asyncio.get_running_loop()
        # return await loop.run_in_executor(
        #     self.executor, self.conn.get_option_chain, expiry_date
        # )
        # replaced above with modern idiomatic way
        return await asyncio.to_thread(self.conn.get_option_chain, expiry_date)

    # Function to handle consecutive API call failures
    def handle_consecutive_failures(self):
        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            self.logger.critical(
                f"{MAX_CONSECUTIVE_FAILURES} consecutive API calls failed. Halting program."
            )
            self.stop()

from datetime import datetime
import logging
import os
from dotenv import load_dotenv
from breeze_connect import BreezeConnect
from breeze_trade_engine.common.date_utils import get_next_weekly_expiry
from breeze_trade_engine.executors.async_base_executor import AsyncBaseExecutor, Singleton
from breeze_trade_engine.common.file_utils import FileWriterMixin

MAX_CONSECUTIVE_FAILURES = 30

LOG_LEVEL=logging.INFO
class OptionChainDataFetcher(Singleton, AsyncBaseExecutor, FileWriterMixin):

    def __init__(self, name, start_time, end_time, interval):
        AsyncBaseExecutor.__init__(self, name, start_time, end_time, interval)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(LOG_LEVEL)
        app_key = os.environ.get("BREEZE_APP_KEY")
        # Initialize SDK
        self.breeze = BreezeConnect(api_key=app_key)
        self.logger.info(f"Breeze SDK initialized.{self.breeze}")
        self.consecutive_failures = 0
        self.file = None
        
    # TODO: Write test cases for this class also check actual data fetch
    # TODO: Think of subscription and notification mechanism for subscribers

    async def process_day_begin(self):
        # implemented the abstract method
        # refresh bezze connection
        await self._refresh_breeze_connection()
        # Create a filename for the CSV file based on the current date
        today = datetime.now().strftime("%Y-%m-%d")
        self.file = f"{os.environ.get("DATA_PATH")}/nifty_chain_{today}.csv"
        self.logger.info("Day begin logic executed.")

    async def process_day_end(self):
        # Write the csv to parquet and delete csv
        self.write_to_parquet(delete_csv=True)
        self.file = None # remove reference to today's file
        self.logger.info("Day end logic executed.")

    async def process_event(self):
        # implemented the abstract method
        # Main processing logic to fire live quote fetch from Breeze
        self.handle_consecutive_failures()
        quotes = await self._get_option_chain()
        if quotes and len(quotes) > 0:
            await self.write_to_csv(quotes)
            self.notify_subscribers(quotes)
        

    async def _get_option_chain(self):
        """
        Use this function to return the live option chain data for a given symbol 
        and expirty date at current time 
        All breeze calls to be abstracted in common package
        """
        quote_time = datetime.now()  # stamp the time the quotes were fetched
        expiry_date = get_next_weekly_expiry()

        quotes = []
        quotes_call = await self._get_chain_quotes(expiry_date, "call")
        quotes_put = await self._get_chain_quotes(expiry_date, "put")
        if quotes_call:
            quotes = quotes + quotes_call
        if quotes_put:
            quotes = quotes + quotes_put

        # Add the quote_time timestamp to the quotes
        for quote in quotes:
            quote["quote_time"] = quote_time
        return quotes

    async def _refresh_breeze_connection(self):
        self.consecutive_failures = 0  # reset the consecutive failures on day begin
        load_dotenv()  # refresh the environment variables from .env file
        secret_key = os.environ.get("BREEZE_SECRET_KEY")
        session_token = os.environ.get("BREEZE_SESSION_TOKEN")
        self.logger.info(f"Secret key: {secret_key}")
        self.logger.info(f"Session token: {session_token}")
        if session_token:
            try:
                self.breeze.generate_session(
                    api_secret=secret_key, session_token=session_token
                )
                self.logger.info("Connected to Breeze.")
            except Exception as e:
                self.logger.error(f"Error connecting. {e}")
                self.logger.critical("Stopping the fetch.")
                self.stop()
        else:
            self.logger.critical("Missing environment variable BREEZE_SESSION_TOKEN.")
            self.logger.critical("Stopping the fetch.")
            self.stop()

    # call the breeze api to get quotes for a given expiry and right
    # filter out zero rows
    async def _get_chain_quotes(self, expiry_date, right):
        self.logger.info(f"Fetching quotes for {right} with expiry {expiry_date[:10]}.")
        try:
            data = self.breeze.get_option_chain_quotes(
                stock_code="NIFTY",
                exchange_code="NFO",
                product_type="options",
                expiry_date=expiry_date,
                right=right,
            )
            if data and data["Success"]:
                self.consecutive_failures = 0
                # TODO: find better check to remove zero rows
                non_zero_quotes = [
                    q
                    for q in data["Success"]
                    if int(q["best_bid_quantity"]) + int(q["best_offer_quantity"]) > 0
                ]
                self.logger.info(f"Quotes fetched: {len(non_zero_quotes)}")
                return non_zero_quotes
            else:
                self.consecutive_failures += 1
                self.logger.critical(
                    f"API call never fired. Check if you have an active session."
                )
                return None
        except Exception as e:
            self.consecutive_failures += 1
            self.logger.error(f"API call failed: {e}")
            return None

    # Function to handle consecutive API call failures
    def handle_consecutive_failures(self):
        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            self.logger.critical(
                f"{MAX_CONSECUTIVE_FAILURES} consecutive API calls failed. Halting program."
            )
            self.stop()

import logging
import os
from datetime import datetime
from dotenv import load_dotenv
from breeze_connect import BreezeConnect
from breeze_trade_engine.executors.async_base_executor import Singleton

MAX_CONSECUTIVE_FAILURES = 30

LOG_LEVEL = logging.DEBUG


class BreezeData(Singleton):

    def __init__(self):
        self.breeze = None
        self.consecutive_failures = 0
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(LOG_LEVEL)
        try:
            self.connect()
        except Exception as e:
            pass

    def refresh(self):
        """
        Reconnect to breeze
        """
        self.connect()

    def connect(self):
        """
        Connect to Breeze
        """

        load_dotenv()  # refresh the environment variables from .env file
        self.consecutive_failures = 0  # reset the consecutive failure
        app_key = os.environ.get("BREEZE_APP_KEY")
        # Initialize SDK
        self.breeze = BreezeConnect(api_key=app_key)
        self.logger.info("Breeze SDK initialized.")
        self.logger.debug(f"self.breeze:{self.breeze}")
        secret_key = os.environ.get("BREEZE_SECRET_KEY")
        session_token = os.environ.get("BREEZE_SESSION_TOKEN")
        self.logger.debug(f"Secret key: {secret_key}")
        self.logger.debug(f"Session token: {session_token}")
        if session_token:
            try:
                self.breeze.generate_session(
                    api_secret=secret_key, session_token=session_token
                )
                self.logger.info("Connected to Breeze.")
            except Exception as e:
                self.logger.error(
                    f"Error connecting. No Fetching will happen",
                    "Check enviroment file for correct breeze settings",
                    "The error is{e}",
                )
                self.breeze = None
        else:
            self.logger.critical(
                "Missing environment variable BREEZE_SESSION_TOKEN. No fetching will happen"
            )
            self.breeze = None

    # call the breeze api to get quotes for a given expiry and right
    # filter out zero rows
    def get_option_chain(
        self,
        expiry_date,
        symbol="NIFTY",
        product_type="options",
        exchange_code="NFO",
        rights=None,
    ):
        if not self.breeze:
            self.logger.error("Breeze connection not established.")
            return None
        # hard coding time to 330PM. Should not matter though
        expiry_date = expiry_date.strftime("%Y-%m-%dT15:30:00.000Z")
        quote_time = datetime.now().replace(
            second=0, microsecond=0
        )  # set second and microsecond to 0
        if rights == None:
            rights = ["Call", "Put"]
        elif isinstance(rights, str):
            rights = [rights]

        chain = []
        for right in rights:
            self.logger.info(
                f"Fetching quotes for {right} with expiry {expiry_date}."
            )
            try:
                data = self.breeze.get_option_chain_quotes(
                    stock_code=symbol,
                    exchange_code=exchange_code,
                    product_type=product_type,
                    expiry_date=expiry_date,
                    right=right,
                )
                if data and data["Success"]:
                    self.consecutive_failures = 0
                    non_zero_quotes = [
                        q
                        for q in data["Success"]
                        if int(q["best_bid_quantity"])
                        + int(q["best_offer_quantity"])
                        > 0
                    ]
                    chain = chain + non_zero_quotes
                    self.logger.info(f"Quotes fetched: {len(non_zero_quotes)}")
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

                # Add the quote_time timestamp to the quotes
        for quote in chain:
            quote["quote_time"] = quote_time
        return chain

    # call the breeze api to get OHLCV data
    def get_ohlcv_data(
        self,
        from_date,
        to_date,
        stock_code="NIFTY",
        exchange_code="NSE",
        interval="1minute",
    ):

        if not self.breeze:
            self.logger.error("Breeze connection not established.")
        from_date = datetime.strftime(from_date, "%Y-%m-%dT%H:%M:00.000Z")
        to_date = datetime.strftime(to_date, "%Y-%m-%dT%H:%M:00.000Z")

        try:
            data = self.breeze.get_historical_data_v2(
                interval=interval,
                from_date=from_date,
                to_date=to_date,
                stock_code=stock_code,
                exchange_code=exchange_code,
            )
            if data and data["Success"]:
                self.consecutive_failures = 0
                return data["Success"]
        except Exception as e:
            self.consecutive_failures += 1
            self.logger.error(f"API call failed: {e}")
            return None

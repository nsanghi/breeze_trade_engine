import asyncio
import logging
from datetime import datetime, timedelta
import os
from breeze_trade_engine.common.file_utils import write_to_csv, write_to_parquet
import pandas as pd
from breeze_trade_engine.common.option_utils import (
    insert_iv_greeks,
    calculate_rv,
    filter_for_ATM,
)
from breeze_trade_engine.executors.async_base_executor import (
    AsyncBaseExecutor,
    Singleton,
)
from breeze_trade_engine.provider.breeze import BreezeData


class StrategyOneExecutor(Singleton, AsyncBaseExecutor):

    def __init__(self, name, start_time, end_time, interval):
        AsyncBaseExecutor.__init__(name, start_time, end_time, interval)
        self.logger = logging.getLogger(__name__)
        self.conn = BreezeData()
        self.vol_series = None
        self.WINDOW_MINS = 30

    # TODO: Think of subscription and notification mechanism for subscribers

    async def process_day_begin(self):
        # day begin initialization logic here
        await self.conn.refresh()
        # Create a filename for the CSV file based on the current date
        today = datetime.now().strftime("%Y-%m-%d")
        self.file = f"{os.environ.get("DATA_PATH")}/vol_series/nifty_vol_series_{today}.csv"
        # Define the column names
        columns = ['datetime', "expiry_date", 'spot_price', 'iv', 'rv', 'spread', 'sma_spread']
        # Create an empty dataframe
        self.vol_series = pd.DataFrame({col: [] for col in columns})
        self.logger.info("Day begin logic executed.")


    async def process_day_end(self):
        # Day end cleanup logic here (move csv to paraquet, and delete csv)
        write_to_parquet(self.file, self.logger, delete_csv=True)
        self.file = None # remove reference to today's file
        self.vol_series = None # remove reference to today's vol series
        self.logger.info("Day end logic executed.")


    async def process_event(self):
        # TODO: Add main processing logic
        # This is to do something every `interval` seconds
        # example would be delta hedging
        # portfolio profit and loss calculation
        pass

    async def process_notification(self, data):
        # Types of Notifications
        # 1. New option chain data
        # Do post processing of data of chain and make trade decision, placing order as required

        # convert to pandas df
        df = pd.DataFrame(data)
        # Filter for ATM options
        df = filter_for_ATM(df)
        # calculate iv and greeks
        df["price"] = (df["best_bid_price"] + df["best_offer_price"]) / 2
        df = insert_iv_greeks(df)
        # calculate spread
        rv = self._get_rv()
        iv = df["iv"].mean(skipna=True) # two rows - ATM put and call
        spread = iv - rv
        # calculate sma spread
        sma_spread = (self.vol_series["spread"].tail(self.WINDOW_MINS-1).values.sum()
                      +spread)/self.WINDOW_MINS
        new_row = {
            "datetime": df["quote_time"].iloc[0],
            "expiry_date": df["expiry_date"].iloc[0],
            "spot_price": df["spot_price"].mean(skipna=True),
            "iv": iv,
            "rv": rv,
            "spread": spread,
            "sma_spread": sma_spread,
        }
        self.vol_series = self.vol_series.append(new_row, ignore_index=True)
        # we now fire the csv write in a separate task and not wait for it
        asyncio.create_task(write_to_csv(self.vol_series, self.file, self.logger, mode="w"))
        # we also fire the trade decision logic in a separate task and not wait for it
        asyncio.create_task(self._make_trade_decision(df))
 
    async def _make_trade_decision(self, df):
        # TODO: Add trade decision logic here
        """
        calculate the trade decision based on the spread and sma spread crossover
        """
                    
    def _get_rv(self):
        # calculate rv
        # fetch close price, calculate rv, iv, and greeks
        to_date = datetime.now().now.replace(second=0, microsecond=0)
        LOOK_BACK_MINS = (
            self.WINDOW_MINS + 5
        )  # keeping 5 as buffer for a window of 30 mins
        from_date = to_date - timedelta(minutes=LOOK_BACK_MINS)
        ohlc_data = self.conn.get_ohlcv_data(from_date, to_date)
        df_ohlcv = pd.DataFrame(ohlc_data)
        if ohlc_data and len(ohlc_data) > 0:
            rv = calculate_rv(
                df_ohlcv, window=self.WINDOW_MINS
            )
        else:
            rv = self.vol_series["rv"].iloc[-1] # for safety store last rv value
        return rv


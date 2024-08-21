import asyncio
from calendar import Calendar
import logging
from datetime import datetime, timedelta
import os
from breeze_trade_engine.common.date_utils import is_expiry_day
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
    Subscriber,
)
from breeze_trade_engine.provider.breeze import BreezeData


class StrategyOneExecutor(Singleton, AsyncBaseExecutor, Subscriber):

    def __init__(self, name, start_time, end_time, interval):
        AsyncBaseExecutor.__init__(name, start_time, end_time, interval)
        self.logger = logging.getLogger(__name__)
        self.conn = BreezeData()
        self.vol_series = None
        # below are strategy specific parameters
        self.WINDOW_MINS = 30
        self.SMA_LOOKBACK = 5
        self.CROSSOVER_THRESHOLD = 0.02  # 2% margin
        self.unwind_day = Calendar.THURSDAY
        self.unwind_time = datetime.strptime("15:15", "%H:%M").time()

    # TODO: Think of subscription and notification mechanism for subscribers

    async def process_day_begin(self):
        # day begin initialization logic here
        await self.conn.refresh()
        # Create a filename for the CSV file based on the current date
        today = datetime.now().strftime("%Y-%m-%d")
        data_path = os.environ.get("DATA_PATH")
        self.file = f"{data_path}/vol_series/nifty_vol_series_{today}.csv"
        # Define the column names
        columns = [
            "datetime",
            "expiry_date",
            "spot_price",
            "iv",
            "rv",
            "spread",
            "sma_spread",
        ]
        # Create an empty dataframe
        self.vol_series = pd.DataFrame({col: [] for col in columns})
        self.logger.info("Day begin logic executed.")

    async def process_day_end(self):
        # Day end cleanup logic here (move csv to paraquet, and delete csv)
        await asyncio.to_thread(
            write_to_parquet, self.file, self.logger, delete_csv=True
        )
        if is_expiry_day():
            await self._do_unwind()  # TODO: additional check for safety
        self.file = None  # remove reference to today's file
        self.vol_series = None  # remove reference to today's vol series
        self.logger.info("Day end logic executed.")

    async def process_event(self):
        # TODO: Add main processing logic
        # This is to do something every `interval` seconds
        # example would be delta hedging
        # portfolio profit and loss calculation
        if is_expiry_day() and datetime.now().time() >= self.unwind_time:
            # TODO: unwind before expiry
            await self._do_unwind()
        elif not is_expiry_day() or (
            is_expiry_day() and datetime.now().time() < self.unwind_time
        ):
            # TODO: delta hedge
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
        rv = await self._get_rv()
        iv = df["iv"].mean(skipna=True)  # two rows - ATM put and call
        spread = iv - rv
        # calculate sma spread
        sma_spread = (
            self.vol_series["spread"].tail(self.SMA_LOOKBACK - 1).values.sum()
            + spread
        ) / self.SMA_LOOKBACK
        crossover = sma_spread - spread
        new_row = {
            "datetime": df["quote_time"].iloc[0],
            "expiry_date": df["expiry_date"].iloc[0],
            "spot_price": df["spot_price"].mean(skipna=True),
            "iv": iv,
            "rv": rv,
            "spread": spread,
            "sma_spread": sma_spread,
            "crossover": crossover,
        }
        self.vol_series = self.vol_series.append(new_row, ignore_index=True)
        # await asyncio.get_running_loop().run_in_executor(
        #     self.executor,
        #     write_to_csv,
        #     self.vol_series,
        #     self.file,
        #     self.logger,
        #     "w",
        # )
        # above is async version of write_to_csv
        # write_to_csv(self.vol_series, self.file, self.logger, mode="w")
        # first coverted sync I/O to async I/O using above function and
        # then used to_thread to run it in a separate thread in more modern idiomatic way
        await asyncio.to_thrad(
            write_to_csv,
            self.vol_series,
            self.file,
            self.logger,
            "w",
        )

        # we also fire the trade decision logic
        # send it the original api data - not required but no harm
        await self._make_trade_decision(df)

    async def _make_trade_decision(self, df):
        """
        calculate the trade decision based on the spread and sma spread crossover
        """
        pass
        # This is a placeholder for the trade decision logic
        # Unwind before expiry should be triggered in process_event method
        crossover = self.vol_series["crossover"].iloc[-1]
        # TODO: implement logic to get current holding either from exchange
        # or from a local cache
        current_holding = ...
        if crossover > self.CROSSOVER_THRESHOLD and current_holding <= 0:
            # TODO: Sell Q straddles
            pass
        elif crossover < -self.CROSSOVER_THRESHOLD and current_holding >= 0:
            # TODO: sqaure off straddles as well as hedges
            pass

    async def _get_rv(self):
        # calculate rv
        # fetch close price, calculate rv, iv, and greeks
        to_date = datetime.now().now.replace(second=0, microsecond=0)
        LOOK_BACK_MINS = (
            self.WINDOW_MINS + 5
        )  # keeping 5 as buffer for a window of 30 mins
        from_date = to_date - timedelta(minutes=LOOK_BACK_MINS)
        ohlc_data = await asyncio.to_thread(
            self.conn.get_ohlcv_data, from_date, to_date
        )
        df_ohlcv = pd.DataFrame(ohlc_data)
        if ohlc_data and len(ohlc_data) > 0:
            rv = calculate_rv(df_ohlcv, window=self.WINDOW_MINS)
        else:
            rv = self.vol_series["rv"].iloc[
                -1
            ]  # for safety store last historical rv value
        return rv

    async def _do_unwind(self):
        # TODO: Unwind before expiry
        # This is to be called from process_event
        # This is a placeholder for the unwind logic
        pass
        # Unwind the position before expiry

    async def _delta_hedge(self):
        # TODO: Delta hedge
        # This is a placeholder for the delta hedge logic
        pass

    async def _place_order(self):
        # TODO: Place order
        # 1)  think of making all breeze api calls async using httpx or aiohttp
        #     but do after successful implementation of the strategy using given api
        # 2)  Logic to place order would be to insert a new row. Optionally find a parent oder
        #     and update the current_order's parent_order_field.
        #  -- We may need to get latest price and make decision on what price to place
        #     the order, calculate margin and see if there is enough margin money.
        #  -- If not then a logic to signal/handle things
        # 3)  breeze api also returns a parent_order_id which I am not sure how to use.
        #     In any case we will need a new field  to store the parent_order_id to keep our own tracking
        # 4)  Also think of a way to track order status using the order_id in live_feed executor
        # 5)  Additional thing to think about is how to handle order failures, partial fills, etc.
        # 6)  Also think of a way to track the current holding in the strategy executor
        #     with P&L and report
        #
        pass

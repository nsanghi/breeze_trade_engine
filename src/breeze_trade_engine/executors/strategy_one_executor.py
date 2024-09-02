import asyncio
from datetime import datetime
import logging
import os
from pathlib import Path
import sys
from typing import Dict
from breeze_trade_engine.common.date_utils import get_weekly_expiry_date
from breeze_trade_engine.data_store import AsyncRollingDataFrame, TradeMaster
from breeze_trade_engine.executors import (
    AsyncBaseExecutor,
    Singleton,
    Subscriber,
)
from breeze_trade_engine.provider.breeze import BreezeData


DATA_FEEDS = {
    "nifty_1sec_ohlcv": {
        "config": {
            "exchange_code": "NSE",
            "stock_code": "NIFTY",
            "interval": "1second",
        },
        "topic": "feed_nifty_1sec_ohlcv",
        "keep_history": False,
    },
    "nifty_1min_ohlcv": {
        "config": {
            "exchange_code": "NSE",
            "stock_code": "NIFTY",
            "interval": "1minute",
        },
        "topic": "feed_nifty_1min_ohlcv",
        "keep_history": True,
    },
    "option_quote": {
        "config": {
            "exchange_code": "NFO",
            "stock_code": "NIFTY",
            "product_type": "options",
            "expiry_date": "<DYNAMIC>",
            "strike_price": "<DYNAMIC>",
            "right": "<DYNAMIC>",
            "get_exchange_quotes": True,
            "get_market_depth": False,
        },
        "topic": "feed_option_quote",
        "keep_history": False,
    },
}

OREDR_FEED = {
    "config": {"get_order_notification": True},
    "topic": "feed_order_update",
    "keep_history": True,
}

TIMERS = {
    "fetch_nifty_chain": {
        "type": "data",
        "interval_seconds": 10,
        "method_name": "fetch_nifty_chain_per_min",
        "df_name": "nifty_chain",
        "topic": "timer_nifty_chain_per_min",
        "keep_history": True,
    },
    "fetch_margin": {
        "type": "data",
        "interval_seconds": 60,
        "method_name": "fetch_margin",
        "df_name": "margin",
        "topic": "timer_margin",
        "keep_history": True,
    },
    "fetch_order_updates": {
        "type": "order",
        "interval_seconds": 60,
        "method_name": "fetch_order_updates",
        "topic": "timer_order_updates",
        "keep_history": True,
    },
    "_internal_update_strike_watch": {
        "type": "internal",
        "interval_seconds": 600,
        "method_name": "update_strike_watch",
        "topic": "timer_strike_watch",
        "keep_history": False,
    },
}

SUBSCRIPTION_TOPICS = (
    [
        timer_config["topic"]
        for timer_config in TIMERS.values()
        if timer_config["type"] != "internal"
    ]
    + [feed["topic"] for feed in DATA_FEEDS.values()]
    + [OREDR_FEED["topic"]]
)


class StrategyOneDataManager(Singleton, AsyncBaseExecutor, Subscriber):

    def __init__(
        self,
        name="vssma_data",
        start_time="09:15",
        end_time="15:30",
        interval=None,
        timers=TIMERS.copy(),
    ):
        AsyncBaseExecutor.__init__(
            self, name, start_time, end_time, interval, timers
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Connecting to Breeze for Subscription feeds")
        self.ws_conn = BreezeData(ws_connect_flag=True)
        self.logger.info("Connecting to Breeze for HTTP requests")
        self.http_conn = BreezeData()
        self._data_feeds: Dict[str, object] = dict()
        self.strikes_watching = None
        self.data_frames: Dict[str, AsyncRollingDataFrame] = dict()
        self.trade_master = None

    async def process_day_begin(self):
        # refresh bezze connection
        self.ws_conn.refresh()
        self.http_conn.refresh()
        self.ws_conn.breeze.on_ticks = self.process_feed
        self.expiry_date = get_weekly_expiry_date()
        # reestablish the callback
        #refresh atm strikes
        self.strikes_watching = await self.get_atm_strikes()
        self._add_all_data_feeds()
        self._init_data_frames()
        self.logger.info("Day begin logic executed.")
        self.trade_master = TradeMaster(
            Path(os.environ.get("DATA_PATH")) / "trade_store", "trade_store.csv"
        )
        # self.trade_master.load_open()  # TODO: Check and implement TradeMaster later
        # self.strikes_watching = self.trade_master.get_strike_prices()

    async def process_day_end(self):
        self._remove_all_feeds()
        self._close_data_frames()
        self.trade_master.save()  # TODO: Check and implement TradeMaster later
        self.trade_master = None
        self.logger.info("Day end logic executed.")

    def _add_all_data_feeds(self):
        for feed_type, feed in DATA_FEEDS.items():
            # for option_quote feed, we need to subscribe to multiple feeds
            # but dataframe will be single
            if feed_type == "option_quote" and self.strikes_watching:
                for right in ["Call", "Put"]:
                    for strike_price in self.strikes_watching:
                        config = dict(feed["config"]) | {
                            "strike_price": str(strike_price),
                            "expiry_date": self.expiry_date.strftime(
                                "%d-%b-%Y"
                            ),
                            "right": right,
                        }  # merge two dictionaries
                        feed_name = self._feed_name(
                            feed_type, right, strike_price
                        )
                        self._subscribe_feed(feed_name, config)
            else:
                config = feed["config"]
                feed_name = feed_type
                self._subscribe_feed(feed_type, config)
        feed_type = "order_update"
        config = OREDR_FEED["config"]
        self._subscribe_feed(feed_type, config)

    def _feed_name(self, feed_name, right, strike_price):
        feed_name = f"{feed_name}_{right}_{strike_price}"
        return feed_name

    def _remove_all_feeds(self):
        for feed_name, feed_value in self._data_feeds.items():
            self._unsubscribe_feed(feed_name, feed_value)

    def _subscribe_feed(self, feed_name, config):
        self.logger.debug(f"Subscribing to {feed_name} with config: {config}")
        try:
            self.ws_conn.breeze.subscribe_feeds(**config)
            self._data_feeds[feed_name] = config
            self.logger.info(f"Subscribed to feed: {feed_name}")
        except Exception as e:
            self.logger.error(f"Error: {e}")
            sys.exit(1)

    def _unsubscribe_feed(self, feed_name, config):
        self.ws_conn.breeze.unsubscribe_feeds(**config)
        self.logger.info(f"Unsubscribed from feed: {feed_name}")

    def _init_data_frames(self):
        today = datetime.now().strftime("%Y-%m-%d")
        file_name = f"{today}_data.csv"
        for feed_name, feed_config in DATA_FEEDS.items():
            # while we subscribe to multiple feeds for option_quote,
            # we create single data frame only for option_quote
            keep_history = feed_config["keep_history"]
            folder_name = f"{os.environ.get("DATA_PATH")}/{feed_name}"
            self.data_frames[feed_name] = AsyncRollingDataFrame(
                folder_name, file_name, keep_history
            )
        # Create data frames for data timers only
        # timers with type "internal" are for internal use only
        # timers with type "order" are for order updates to be handled by a different class
        for _, timer_config in self.timers.items():
            if timer_config["type"] != "data":
                continue
            df_name = timer_config["df_name"]
            keep_history = timer_config["keep_history"]
            folder_name = Path(os.environ.get("DATA_PATH")) / df_name
            self.data_frames[df_name] = AsyncRollingDataFrame(
                folder_name, file_name, keep_history
            )
        # TODO: add a data_frame for internal use to track rv, iv, and spread

    def _close_data_frames(self):
        for df_name, rolling_df in self.data_frames.items():
            rolling_df.close()
            del self.data_frames[df_name]

    def _modify_strikes_watch(self, new_strikes, add_only=False):
        """
        Modify the strikes watching list.
        Assumes that the new_strikes and self.strikes_watching are lists of strings.
        """
        current_strikes = set(self.strikes_watching)
        new_strikes = set(new_strikes)
        add_strikes = new_strikes - current_strikes
        if not add_only:
            remove_strikes = current_strikes - new_strikes
        else:
            remove_strikes = set()

        for strike_price in remove_strikes:
            for right in ["Call", "Put"]:
                feed_name = self._feed_name("option_quote", right, strike_price)
                config = self._data_feeds[feed_name]
                self._unsubscribe_feed(feed_name, config)
                del self._data_feeds[feed_name]
        for strike_price in add_strikes:
            for right in ["Call", "Put"]:
                feed_name = self._feed_name("option_quote", right, strike_price)
                feed = DATA_FEEDS["option_quote"]
                config = dict(feed["config"]) | {
                    "strike_price": str(strike_price),
                    "expiry_date": self.expiry_date.strftime("%d-%b-%Y"),
                    "right": right,
                }  # merge two dictionaries
                feed_name = self._feed_name(feed_name, strike_price)
                self._subscribe_feed(feed_name, config)

        self.strikes_watching = (self.strikes_watching | add_strikes) - remove_strikes

    async def process_event(self, **kwargs):
        # TODO: this class is not using the default timer, so this method is not needed
        # check and remove it from this class
        self.logger.info(f"Processing event for {self.name}.")
        result = None  # Placeholder for api call
        topic = "default" if "topic" not in kwargs else kwargs["topic"]
        self.notify_subscribers(topic, result)
        

    async def process_notification(self, topic, event):
        # This class does not subscribe to any external notifications
        # so no need to implement this method in this class
        pass

    def process_feed(self, ticks):
        # if executor not in running state, do not process the ticks
        try:
            if not self.running:
                return
            topic = None
            match ticks:
                case {"interval": "1second", "stock_code": "NIFTY"}:
                    feed_name = "nifty_1sec_ohlcv"
                case {"interval": "1minute", "stock_code": "NIFTY"}:
                    feed_name = "nifty_1min_ohlcv"
                case {"stock_name": "NIFTY 50"}:
                    feed_name = "option_quote"
                case {"orderDate": _}:
                    feed_name = "order_update"
                case _:
                    print("No match found")
                    return
            # send data to respective rolling data frame
            if feed_name in self.data_frames:
                df = self.data_frames[feed_name]
                self.event_loop.create_task(df.add_data(ticks))
            # notify subscribers
            if feed_name in DATA_FEEDS:
                topic = DATA_FEEDS[feed_name]["topic"]
            elif feed_name == "order_update":
                topic = OREDR_FEED["topic"]
            self.event_loop.create_task(self.notify_subscribers(topic, ticks))
        except Exception as e:
            self.logger.error(f"Error processing feed: {e}")

    async def _process_and_notify(self, data, **kwargs):
        
        self.logger.debug(f"Processing data: {len(data)}. Additional kwargs: {kwargs}")
        if data and len(data) > 0:
            if "df_name" in kwargs:
                df_name = kwargs["df_name"]
            else:
                df_name = "default"
            df = self.data_frames[df_name]
            self.logger.debug(f"Adding data to {df_name}: {len(data)}")
            asyncio.create_task(df.add_data(data))
            topic = kwargs.get("topic", "default")
            asyncio.create_task(self.notify_subscribers(topic, data))

    # TODO: think and see where to update the iv, rv, spread
    async def fetch_nifty_chain_per_min(self, **kwargs):
        """
        Called by the timer to fetch NIFTY option chain.
        """
        self.logger.debug("Fetching NIFTY option chain. Additional kwargs: {kwargs}")
        data = await asyncio.to_thread(
            self.http_conn.get_option_chain, self.expiry_date)
        
        self.logger.debug(f"Option chain fetched: {len(data)}")
        await self._process_and_notify(data, **kwargs)

    async def fetch_order_updates(self, **kwargs):
        """
        Called by the timer to fetch order updates.
        """
        self.logger.info("Fetching order updates.")
        updates = await asyncio.to_thread(
            self.http_conn.get_order_updates  # TODO: Implement this method in BreezeData
        )
        await self._process_and_notify(updates, **kwargs)

    async def fetch_margin(self, **kwargs):
        """
        Called by the timer to fetch margin data.
        """
        self.logger.info("Fetching margin data.")
        margin = await asyncio.to_thread(
            self.http_conn.get_margin  # TODO: Implement this method in BreezeData
        )
        await self._process_and_notify(margin, **kwargs)

    async def update_strike_watch(self, **kwargs):
        """
        Called by the timer to update the strikes watching list.
        """
        new_strikes = self.trade_master.get_strike_prices()
        atm_strikes = await self.get_atm_strikes()
        self._modify_strikes_watch(new_strikes | atm_strikes)

    async def get_atm_strikes(self):
        # TODO: implement this
        return set([
            25000,
            24950,
            24900,
            25050,
            25100,
        ])


class StrategyOneExecutionManager(Singleton, AsyncBaseExecutor, Subscriber):

    def __init__(
        self,
        name="vssma_strategy",
        start_time="09:15",
        end_time="15:30",
        interval=60,
        timers=None,
    ):
        AsyncBaseExecutor.__init__(
            self, name, start_time, end_time, interval, timers
        )
        self.logger = logging.getLogger(__name__)

    # TODO: In main code havw this class subscribe to various topics as
    # specified in the SUBSCRIPTION_TOPICS list

    async def process_day_begin(self):
        # TODO: Add day begin initialization logic here
        # refresh bezze connection
        self.logger.info("Day begin logic executed.")

    async def process_day_end(self):
        # TODO: Add day end cleanup logic here (move csv to paraquet, and delete csv)
        self.logger.info("Day end logic executed.")

    async def process_notification(self, topic, event):
        method_name = f"on_{topic}"
        self.logger.debug(
            f"Notification received for topic: {topic}. Calling method: {method_name}"
        )
        if self._is_valid_async_method(method_name):
            async_func = getattr(self, method_name)
            await async_func(event)

    async def process_event(self):
        # This class may do something on its own defined timer
        # If so, implement the logic here TODO:
        pass

    async def on_feed_nifty_1sec_ohlcv(self, ticks):
        # TODO: implement actual logic
        print(f"Processing on_feed_nifty_1sec_ohlcv:{ticks}")

    async def on_feed_nifty_1min_ohlcv(self, ticks):
        # TODO: implement actual logic
        print(f"Processing on_feed_nifty_1min_ohlcv:{ticks}")

    async def on_feed_option_quote(self, ticks):
        # TODO: implement actual logic
        print(f"Processing on_feed_option_quote:{ticks}")

    async def on_feed_order_update(self, ticks):
        # TODO: implement actual logic
        print(f"Processing on_feed_order_update:{ticks}")

    async def on_timer_nifty_chain_per_min(self, ticks):
        # TODO: implement actual logic
        print(f"Processing on_timer_nifty_chain_per_min:{ticks}")

    async def on_timer_margin(self, ticks):
        # TODO: implement actual logic
        print(f"Processing on_timer_margin:{ticks}")

    async def on_timer_order_updates(self, ticks):
        # TODO: implement actual logic
        print(f"Processing on_timer_order_updates:{ticks}")

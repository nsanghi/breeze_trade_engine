import asyncio
from datetime import datetime
import logging
import os
from pathlib import Path
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
    "topic": "feed_order_update",
    "keep_history": True,
}

TIMERS = [
    {
        "type": "data",
        "interval_seconds": 60,
        "method_name": "fetch_nifty_chain_per_min",
        "df_name": "nifty_chain",
        "topic": "timer_nifty_chain_per_min",
        "keep_history": True,
    },
    {
        "type": "data",
        "interval_seconds": 60,
        "method_name": "fetch_margin",
        "df_name": "margin",
        "topic": "timer_margin",
        "keep_history": True,
    },
    {
        "type": "order",
        "interval_seconds": 60,
        "method_name": "fetch_order_updates",
        "topic": "timer_order_updates",
        "keep_history": True,
    },
    {
        "type": "internal",
        "interval_seconds": 600,
        "method_name": "update_strike_watch",
        "topic": "timer_strike_watch",
        "keep_history": True,
    },
]

SUBSCRIPTION_TOPICS = (
    [timer["topic"] for timer in TIMERS if timer["type"] != "internal"]
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
        self.consecutive_failures = 0
        self.file = None
        self.ws_conn = BreezeData(ws_connect_flag=True)
        self.http_conn = BreezeData()
        self.data_feeds = dict()
        self.strikes_watching = None
        self.data_frames: {str, AsyncRollingDataFrame} = dict()
        self.trade_master = None

    async def process_day_begin(self):
        # refresh bezze connection
        self.ws_conn.refresh()
        self.http_conn.refresh()
        self.ws_conn.breeze.on_ticks = self.process_feed
        self.expiry_date = get_weekly_expiry_date()
        # reestablish the callback
        self._add_all_data_feeds()
        self._init_data_frames()
        self.logger.info("Day begin logic executed.")
        self.trade_master = TradeMaster(
            Path(os.environ.get("DATA_PATH") / "trade_store", "trade_store.csv")
        )
        self.trade_master.load_open()  # TODO: Check and implement TradeMaster later
        self.strikes_watching = self.trade_master.get_strike_prices()

    async def process_day_end(self):
        self._close_data_frames()
        self._remove_all_feeds()
        self.trade_master.save()  # TODO: Check and implement TradeMaster later
        self.trade_master = None
        self.logger.info("Day end logic executed.")

    def _add_all_data_feeds(self):
        for feed_key in DATA_FEEDS:
            # for option_trade feed, we need to subscribe to multiple feeds
            if feed_key == "option_trade" and self.strikes_watching:
                for right in ["Call", "Put"]:
                    for strike_price in self.strikes_watching:
                        config = DATA_FEEDS["option_quote"]["config"].copy() | {
                            "strike_price": strike_price,
                            "expiry_date": self.expiry_date,
                            "right": right,
                        }  # merge two dictionaries
                        feed_name = f"{feed_key}_{strike_price}"
                        self._subscribe_feed(feed_name, config)
            else:
                config = DATA_FEEDS[feed_key]["config"].copy()
                self._subscribe_feed(feed_key, config)
        feed_key = "order_update"
        config = dict()
        self._subscribe_feed(feed_key, config)

    def _remove_all_feeds(self):
        for feed_name, feed_value in self.data_feeds.items():
            self._unsubscribe_feed(feed_name, feed_value)

    def _subscribe_feed(self, feed_name, config):
        self.ws_conn.breeze.subscribe_feeds(**config)
        self.data_feeds[feed_name] = config
        self.logger.info(f"Subscribed to feed: {feed_name}")

    def _unsubscribe_feed(self, feed_name, item):
        self.ws_conn.breeze.unsubscribe_feeds(**item)
        self.logger.info(f"Unsubscribed from feed: {feed_name}")

    def _init_data_frames(self):
        today = datetime.now().strftime("%Y-%m-%d")
        file_name = f"{today}_data.csv"
        for feed_key in DATA_FEEDS:
            # while we subscribe to multiple feeds for option_trade,
            # we create single data frame only for option_trade
            keep_history = DATA_FEEDS[feed_key]["keep_history"]
            folder_name = Path(os.environ.get("DATA_PATH") / feed_key)
            self.data_frames[feed_key] = AsyncRollingDataFrame(
                folder_name, file_name, keep_history
            )
        # Create data frames for data timers only
        # timers with type "internal" are for internal use only
        # timers with type "order" are for order updates to be handled by a different class
        for timer in self.timers:
            if timer["type"] != "data":
                continue
            df_name = timer["df_name"]
            keep_history = timer["keep_history"]
            folder_name = Path(os.environ.get("DATA_PATH") / df_name)
            self.data_frames[df_name] = AsyncRollingDataFrame(
                folder_name, file_name, keep_history
            )
        # add a data_frame for internal use to track rv, iv, and spread

    def _close_data_frames(self):
        for feed_key in self.data_frames:
            self.data_frames[feed_key].close()
            del self.data_frames[feed_key]

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
            feed_name = f"option_trade_{strike_price}"
            self._unsubscribe_feed(feed_name, self.data_feeds[feed_name])
            del self.data_feeds[feed_name]
        for strike_price in add_strikes:
            feed_value = DATA_FEEDS["option_trade"].copy()
            feed_value["strike_price"] = strike_price
            feed_value["expiry_date"] = self.expiry_date
            feed_name = f"option_trade_{strike_price}"
            self._subscribe_feed(feed_name, feed_value)
        self.strikes_watching += add_strikes - remove_strikes

    async def process_event(self, **kwargs):
        """
        This method should be implemented by the derived class to perform any
        processing logic that needs to be done at regular intervals during the trading day.
        Only relevant if you do not provide a list of timers as it is used in the default timer only.
        """
        # TODO: this class is not using the default timer, so this method is not needed
        # check and remove it from this class
        self.logger.info(f"Processing event for {self.name}.")
        result = None  # Placeholder for api call
        topic = "default" if "topic" not in kwargs else kwargs["topic"]
        self.notify_subscribers(topic, result)
        pass

    async def process_notification(self, topic, event):
        # This class does not subscribe to any external notifications
        # so no need to implement this method in this class
        pass

    def process_feed(self, ticks):
        # if executor not in running state, do not process the ticks
        if not self.running:
            return
        topic = None
        match ticks:
            case {"interval": "1second", "stock_code": "NIFTY"}:
                key = "nifty_1sec_ohlcv"
            case {"interval": "1minute", "stock_code": "NIFTY"}:
                key = "nifty_1min_ohlcv"
            case {"stock_name": "NIFTY 50"}:
                key = "option_trade"
            case {"orderDate": _}:
                key = "order_update"
            case _:
                print("No match found")
                return
        # send data to respective rolling data frame
        df = self.data_frames[key]
        self.event_loop.create_task(df.add_data(ticks))
        # notify subscribers
        if key in DATA_FEEDS:
            topic = DATA_FEEDS[key]["topic"]
        elif key == "order_update":
            topic = OREDR_FEED["topic"]
        self.event_loop.create_task(self.notify_subscribers(topic, ticks))

    async def _add_data_and_notify(self, data, **kwargs):
        if data and len(data) > 0:
            df_name = kwargs.get("df_name ", "default")
            asyncio.create_task(self.data_frames[df_name].add_data(data))
            topic = kwargs.get("topic", "default")
            asyncio.create_task(self.notify_subscribers(topic, data))

    # TODO: think and see where to update the iv, rv, spread
    async def fetch_nifty_chain_per_min(self, **kwargs):
        """
        Called by the timer to fetch NIFTY option chain.
        """
        data = await asyncio.to_thread(
            self.http_conn.get_option_chain(self.expiry_date)
        )
        await self._add_data_and_notify(data, **kwargs)

    async def fetch_order_updates(self, **kwargs):
        """
        Called by the timer to fetch order updates.
        """
        updates = await asyncio.to_thread(
            self.http_conn.get_order_updates()  # TODO: Implement this method in BreezeData
        )
        await self._add_data_and_notify(updates, **kwargs)

    async def fetch_margin(self, **kwargs):
        """
        Called by the timer to fetch margin data.
        """
        margin = await asyncio.to_thread(
            self.http_conn.get_margin()  # TODO: Implement this method in BreezeData
        )
        await self._add_data_and_notify(margin, **kwargs)

    async def update_strike_watch(self, **kwargs):
        """
        Called by the timer to update the strikes watching list.
        """
        new_strikes = self.trade_master.get_strike_prices()
        # TODO: Add ATM strike with neary ticks to the watch list
        self._modify_strikes_watch(new_strikes)


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

    # TODO: In main code hae this call subscribe to various topics as
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
        if self.is_valid_async_method(method_name):
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

    async def on_feed_option_trade(self, ticks):
        # TODO: implement actual logic
        print(f"Processing on_feed_option_trade:{ticks}")

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

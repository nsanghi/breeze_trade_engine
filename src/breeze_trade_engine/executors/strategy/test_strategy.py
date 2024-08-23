import asyncio
import logging
from breeze_trade_engine.executors.async_base_executor import (
    AsyncBaseExecutor,
    Singleton,
    Subscriber,
)
from breeze_trade_engine.provider.breeze import BreezeData


FEEDS = {
    "nifty_1sec_ohlcv_feed": {
        "config": {
            "exchange_code": "NSE",
            "stock_code": "NIFTY",
            "interval": "1second",
        },
        "callback": "process_nifty_1sec_ohlcv",
    },
    "nifty_1min_ohlcv_feed": {
        "config": {
            "exchange_code": "NSE",
            "stock_code": "NIFTY",
            "interval": "1minute",
        },
        "callback": "process_nifty_1min_ohlcv",
    },
    # "option_feed": {
    #     "config": {
    #         "exchange_code": "NFO",
    #         "stock_code": "NIFTY",
    #         "product_type": "options",
    #         "expiry_date": "<DYNAMIC>",
    #         "strike_price": "<DYNAMIC>",
    #         "right": "<DYNAMIC>",
    #         "get_exchange_quotes": True,
    #         "get_market_depth": False,
    #     },
    #     "callback": "process_option_feed",
    # },
    "order_update_feed": {
        "config": {
            "get_order_notification": True,
        },
        "callback": "process_order_notification",
    },
}


class TestStartegy(Singleton, AsyncBaseExecutor, Subscriber):

    def __init__(
        self,
        name="vssma",
        start_time="09:15",
        end_time="15:30",
        interval=60,
        timers=None,
    ):
        AsyncBaseExecutor.__init__(
            self, name, start_time, end_time, interval, timers
        )
        self.logger = logging.getLogger(__name__)
        self.consecutive_failures = 0
        self.file = None
        self.conn = BreezeData(ws_connect_flag=True)
        # self.do_post_init()

    def _subscribe_feed(self, feed_name):

        if feed := FEEDS[feed_name]:
            self.conn.breeze.subscribe_feeds(**feed["config"])
            self.logger.info(f"Subscribed to feed: {feed_name}")

    def _unsubscribe_feed(self, feed_name):
        if feed := FEEDS[feed_name]:
            self.conn.breeze.unsubscribe_feeds(**feed["config"])
            self.logger.info(f"Unsubscribed from feed: {feed_name}")

    # TODO: Think of subscription and notification mechanism for subscribers

    async def process_day_begin(self):
        # TODO: Add day begin initialization logic here
        # refresh bezze connection
        self.conn.refresh()
        self.conn.breeze.on_ticks = self.process_feed
        # reestablish the callback
        for feed in FEEDS:
            self._subscribe_feed(feed)
        self.logger.info("Day begin logic executed.")

    async def process_day_end(self):
        # TODO: Add day end cleanup logic here (move csv to paraquet, and delete csv)
        for feed in FEEDS:
            self._unsubscribe_feed(feed)
        self.logger.info("Day end logic executed.")

    async def process_notification(self, topic, event):
        # TODO: We could use this to trigger data arrival from publisher
        pass

    async def process_event(self):
        # TODO: Add main processing logic here for each timer callback
        # may need to thikn and redesign this based on multi timer concept now
        pass

    def process_feed(self, ticks):
        callback = None
        match ticks:
            case {"interval": "1second", "stock_code": "NIFTY"}:
                callback = FEEDS["nifty_1sec_ohlcv_feed"]["callback"]
            case {"interval": "1minute", "stock_code": "NIFTY"}:
                callback = FEEDS["nifty_1min_ohlcv_feed"]["callback"]
            case {"stock_name": "NIFTY 50"}:
                callback = FEEDS["option_feed"]["callback"]
            case {"orderDate": _}:
                callback = FEEDS["order_update_feed"]["callback"]
            case _:
                print("No match found")
        if self.is_valid_async_method(callback):
            async_func = getattr(self, callback)
            self.loop.create_task(async_func(ticks))

    async def process_nifty_1sec_ohlcv(self, ticks):
        # TODO: implement actual logic
        print(f"Processing 1 sec ticks:{ticks}")

    async def process_nifty_1min_ohlcv(self, ticks):
        # TODO: implement actual logic
        print(f"Processing 1 min ticks:{ticks}")

    async def process_option_feed(self, ticks):
        # TODO: implement actual logic
        print(f"Processing option feed ticks:{ticks}")

    async def process_order_notification(self, ticks):
        # TODO: implement actual logic
        print(f"Processing order notification ticks:{ticks}")

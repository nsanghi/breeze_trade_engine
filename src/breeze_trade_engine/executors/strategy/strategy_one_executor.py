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

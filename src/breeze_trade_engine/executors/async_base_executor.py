import asyncio
import threading
import logging
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from breeze_trade_engine.common.date_utils import is_trading_day


class Singleton:
    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__new__(cls)
        return cls._instances[cls]


class AsyncBaseExecutor(ABC):

    def __init__(self, name, start_time="09:15", end_time="15:30", interval=60):
        self.name = name
        self.start_time = datetime.strptime(start_time, "%H:%M").time()
        self.end_time = datetime.strptime(end_time, "%H:%M").time()
        self.interval = interval
        self.thread = None
        self.lock = threading.Lock()
        self.running = False
        self.subscribers = []
        self.active_today = False
        self.logger = logging.getLogger(__name__)
        self.notification_loop = None
        self.notification_queues = {}
        self.thread_pool = ThreadPoolExecutor(max_workers=5)

    def __del__(self):
        self.day_end()
        self.thread_pool.shutdown(wait=True)

    @abstractmethod
    async def process_day_begin(self):
        """
        This method should be implemented by the derived class to perform any
        initialization logic that needs to be done at the beginning of the trading day.
        This method will be called only once per day.
        """
        pass

    @abstractmethod
    async def process_day_end(self):
        """
        This method should be implemented by the derived class to perform any
        cleanup logic that needs to be done at the end of the trading day.
        This method will be called only once per day.
        """
        pass

    @abstractmethod
    async def process_event(self):
        """
        This method should be implemented by the derived class to perform any
        processing logic that needs to be done at regular intervals during the trading day.
        """
        pass

    async def process_notification(self, data) -> None:
        """
        Process a notification asynchronously.
        This method should be implemented by derived classes to handle
        notifications from publishers.
        """
        try:
            pass
        except Exception as e:
            self.logger.error(f"Error processing notification: {e}")

    def start(self):
        with self.lock:
            if self.running:
                self.logger.warning(
                    f"Process {self.name} is already running, ignoring start request."
                )
                return
            self.running = True
            self.thread = threading.Thread(target=self.run_async_loop)
            self.thread.start()
            self.notification_loop = asyncio.new_event_loop()
            self.notification_thread = threading.Thread(
                target=self.run_notification_loop, daemon=True
            )
            self.notification_thread.start()
            # Wait for the notification loop to be ready
            while not self.notification_loop.is_running():
                asyncio.sleep(0.1)
            self.logger.info(f"Started process: {self.name}")

    def stop(self):
        with self.lock:
            if not self.running:
                self.logger.warning(
                    f"Process {self.name} is not running, ignoring stop request."
                )
                return
            self.running = False
            if self.thread:
                self.thread.join(timeout=10)  # Add a timeout
            if self.notification_loop:
                self.notification_loop.call_soon_threadsafe(
                    self.notification_loop.stop
                )
            self.notification_thread.join(timeout=10)  # Add a timeout
            self.logger.info(f"Stopped process: {self.name}")

    async def day_begin(self):
        if self.active_today:
            return
        try:
            await self.process_day_begin()
            self.active_today = True
            self.logger.info(f"Day begin executed for {self.name}.")
        except Exception as e:
            self.logger.error(f"Error during day begin for {self.name}: {e}")

    async def day_end(self):
        if not self.active_today:
            return
        try:
            await self.process_day_end()
            self.active_today = False
            self.logger.info(f"Day end executed for {self.name}.")
        except Exception as e:
            self.logger.error(f"Error during day end for {self.name}: {e}")

    async def run_daily_cycle(self):
        while self.running:
            now = datetime.now()
            today = now.date()

            if not is_trading_day(today):
                self.logger.info(
                    f"{today} is not a trading day. Sleeping till tomorrow."
                )
                await asyncio.sleep(self.seconds_to_tomorrow_begin())
                continue

            if now.time() < self.start_time:
                sleep_seconds = (
                    datetime.combine(today, self.start_time) - now
                ).total_seconds()
                self.logger.info(
                    f"Sleeping for {sleep_seconds:.2f} seconds until day begin"
                )
                await asyncio.sleep(sleep_seconds)
                continue

            if self.start_time <= now.time() < self.end_time:
                if not self.active_today:
                    await self.day_begin()

                next_fetch = now
                while next_fetch.time() < self.end_time and self.running:
                    try:
                        await self.process_event()
                    except Exception as e:
                        self.logger.error(f"Error during event processing: {e}")
                    next_fetch += timedelta(seconds=self.interval)
                    sleep_time = (next_fetch - datetime.now()).total_seconds()
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)

            if self.running:
                await self.day_end()

            await asyncio.sleep(self.seconds_to_tomorrow_begin())

    def run_async_loop(self):
        asyncio.run(self.run_daily_cycle())
        self.notification_loop.run_until_complete(self._process_notifications())

    def run_notification_loop(self):
        asyncio.set_event_loop(self.notification_loop)
        self.notification_loop.run_forever()

    def seconds_to_tomorrow_begin(self):
        now = datetime.now()
        next_start_time = datetime.combine(
            now.date() + timedelta(days=1), self.start_time
        )
        return (next_start_time - now).total_seconds()

    def add_subscriber(self, subscriber):
        self.subscribers.append(subscriber)
        self.notification_queues[subscriber] = asyncio.Queue()

    def remove_subscriber(self, subscriber):
        self.subscribers.remove(subscriber)
        del self.notification_queues[subscriber]

    def notify_subscribers(self, data):
        for subscriber in self.subscribers:
            self.notification_queues[subscriber].put_nowait(data)

    async def _process_notifications(self):
        while True:
            for subscriber, queue in self.notification_queues.items():
                if not queue.empty():
                    data = await queue.get()
                    await subscriber.process_notification(data)
                    queue.task_done()
            await asyncio.sleep(0.1)

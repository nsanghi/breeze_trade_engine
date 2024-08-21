import asyncio
import logging
from datetime import datetime, timedelta
from abc import ABC

# from concurrent.futures import ThreadPoolExecutor
from breeze_trade_engine.common.date_utils import is_trading_day


class Singleton:
    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__new__(cls)
        return cls._instances[cls]


class Subscriber:
    """
    Process a notification asynchronously.
    This method should be implemented by derived classes to handle
    notifications from publishers.
    """

    async def process_notification(self, topic, event):
        try:
            # This method should be overridden by actual subscriber implementations
            self.logger.warning(
                f"Received event on topic {topic}: {event} - no implementation found."
            )
        except Exception as e:
            self.logger.error(f"Error processing notification: {e}")


class AsyncBaseExecutor(ABC):

    def __init__(
        self,
        name,
        start_time="09:15",
        end_time="15:30",
        interval=None,
        timers=None,
    ):
        assert not (
            interval and timers
        ), "Only one of interval or timers can be provided"
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(
                "This class can only be instantiated within an asyncio event loop"
            )
        self.name = name
        self.start_time = datetime.strptime(start_time, "%H:%M").time()
        self.end_time = datetime.strptime(end_time, "%H:%M").time()
        self.timers = timers
        self.tasks = []
        self.interval = interval
        self.active_today = False
        self.running = False
        self.logger = logging.getLogger(__name__)
        # self.executor = ThreadPoolExecutor(
        #     max_workers=1
        # )  # with I/O and syncrhoous api calls moving to more modern asyncio.to_task syntax, there is no need to get an executor
        self.task = None
        self.subscribers = {}

    async def process_day_begin(self):
        """
        This method should be implemented by the derived class to perform any
        initialization logic that needs to be done at the beginning of the trading day.
        This method will be called only once per day.
        """
        self.logger.info(f"Day begin executed for {self.name}.")
        pass

    async def process_day_end(self):
        """
        This method should be implemented by the derived class to perform any
        cleanup logic that needs to be done at the end of the trading day.
        This method will be called only once per day.
        """
        self.logger.info(f"Day End executed for {self.name}.")
        pass

    async def process_event(self):
        """
        This method should be implemented by the derived class to perform any
        processing logic that needs to be done at regular intervals during the trading day.
        Only relevant if you do not provide a list of timers as it is used in the default timer only.
        """
        self.logger.info(f"Processing event for {self.name}.")
        result = None  # Placeholder for api call
        self.notify_subscribers("event", result)
        pass

    async def start(self):
        if self.running:
            self.logger.warning(
                f"Process {self.name} is already running, ignoring start request."
            )
            return
        self.running = True
        self.logger.info(f"Process {self.name} set to start running")

        self.task = asyncio.create_task(self.run_daily_cycle())
        self.logger.info(f"Started async task for process: {self.name}")

    async def stop(self):
        if not self.running:
            self.logger.warning(
                f"Process {self.name} is not running, ignoring stop request."
            )
            return
        self.running = False
        self.logger.info(f"Stopping process: {self.name}")
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        await self.day_end()
        self.logger.info(f"Stopped process: {self.name}")

    async def cleanup(self):
        """
        This method should be called to cleanup the executor and stop the process.
        """
        await self.stop()
        # not required as we are using modern asyncio.to_thread syntax
        # self.executor.shutdown(wait=True)

    def add_subscriber(self, topic, subscriber):
        if not isinstance(subscriber, Subscriber):
            raise TypeError(
                "Subscriber must be an instance of Subscriber class"
            )
        if topic not in self.subscribers:
            self.subscribers[topic] = set()
        self.subscribers[topic].add(subscriber)
        self.logger.info(f"Added subscriber {subscriber} to topic {topic}")

    def remove_subscriber(self, topic, subscriber):
        if topic in self.subscribers:
            self.subscribers[topic].discard(subscriber)
        self.subscribers[topic].remove(subscriber)
        self.logger.info(f"Removed subscriber {subscriber} from topic {topic}")

    async def notify_subscribers(self, topic, data):
        if topic not in self.subscribers:
            return
        tasks = []
        for subscriber in self.subscribers[topic]:
            tasks.append(
                asyncio.create_tas(subscriber.process_notification(topic, data))
            )
        await asyncio.gather(*tasks)

    async def day_begin(self):
        if self.active_today:
            return
        try:
            await self.process_day_begin()
            self.active_today = True
            self.logger.info(f"Day begin executed for {self.name}.")
            self.start_daily_timer_scheduler()
            self.logger.info("Started all timers.")
        except Exception as e:
            self.logger.error(f"Error during day begin for {self.name}: {e}")

    async def day_end(self):
        if not self.active_today:
            return
        try:
            await self.process_day_end()
            self.active_today = False
            await self.stop_daily_timer_scheduler()
            self.logger("Stopped all timers.")
            self.logger.info(f"Day end executed for {self.name}.")
        except Exception as e:
            self.logger.error(f"Error during day end for {self.name}: {e}")

    async def run_daily_cycle(self):
        while True:
            if self.running:
                now = datetime.now()
                today = now.date()

                if not is_trading_day(today):
                    self.logger.info(
                        f"{today} is not a trading day. Sleeping till tomorrow."
                    )
                    await asyncio.sleep(self.seconds_to_tomorrow_begin())
                elif now.time() < self.start_time:
                    sleep_seconds = (
                        datetime.combine(today, self.start_time) - now
                    ).total_seconds()
                    self.logger.info(
                        f"Sleeping for {sleep_seconds:.2f} seconds until day begin"
                    )
                    await asyncio.sleep(sleep_seconds)
                elif self.start_time <= now.time() < self.end_time:
                    if not self.active_today:
                        await self.day_begin()
                        # this will also start the daily timers
                elif now.time() > self.end_time:
                    await self.day_end()
                    await asyncio.sleep(self.seconds_to_tomorrow_begin())
                else:
                    self.logger.warning(
                        "Invalid state reached. Check logic. now = {now}"
                    )
            else:
                self.logger.info(f"Process {self.name} is not running.")
                await asyncio.sleep(self.interval)

    def seconds_to_tomorrow_begin(self):
        now = datetime.now()
        next_start_time = datetime.combine(
            now.date() + timedelta(days=1), self.start_time
        )
        return (next_start_time - now).total_seconds()

    async def run_timer(self, interval_seconds, processing_function):
        """
        Runs a timer for a specified interval and executes a function when the timer expires.
        """
        # process the tick
        try:
            now = datetime.now()
            if self.running:  #
                await processing_function()
        except Exception as e:
            self.logger.error(f"Error during event processing: {e}")
        next_fetch = now + timedelta(seconds=interval_seconds)
        sleep_time = (next_fetch - datetime.now()).total_seconds()
        sleep_time = max(
            0, sleep_time
        )  # catchup if last processing took longer
        self.logger.info(f"Sleeping for {sleep_time:.2f} seconds")
        await asyncio.sleep(sleep_time)

    def start_daily_timer_scheduler(self):
        if len(self.tasks) > 0:
            self.stop_daily_timer_scheduler()  # for cleanup that did not happen previous day
        if not self.timers:
            self.tasks = [
                {
                    "interval_seconds": self.interval,
                    "function": self.process_event,
                }
            ]
            self.logger.info("Started default timer.")
            return
        for timer in self.timers:
            task = asyncio.create_task(
                self.run_timer(timer["interval_seconds"], timer["function"])
            )
            self.tasks.append(task)
            self.logger.info("Started all timers.")

    async def stop_daily_timer_scheduler(self):
        for task in self.tasks:
            try:
                await task.cancel()
            except asyncio.CancelledError:
                pass
        self.tasks = []
        self.logger.info("Stopped all timers.")

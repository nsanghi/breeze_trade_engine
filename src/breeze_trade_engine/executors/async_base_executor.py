import asyncio
import logging
from datetime import datetime, timedelta
from abc import ABC
from typing import Dict, Set, Union

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
    """
    Base class for asynchronous executors.
    Attributes:
        name (str): The name of the executor.
        start_time (str): The start time of the executor in the format "HH:MM".
        end_time (str): The end time of the executor in the format "HH:MM".
        interval (int): The interval in seconds between each execution.
        timers (list): A list of timers for scheduled executions.
            a timer should be a dictionary with the following
            keys:
                - 'interval_seconds' (int): The interval in seconds between each execution.
                - 'method_name' (str): The name of the method to be executed at the end of the interval.
                    It should be a coroutine method of the class.
                - 'df_name' (str, optional): The name of the dataframe to be created.
                - 'keep_history' (bool, optional): Flag indicating if the dataframe should be stored to disk.
                - 'topic' (str, optional): The topic to this the method_name publishes its results to
                {
                    'interval_seconds': 60,
                    'method_name': 'calculate_metrics',
                    'df_name': 'metrics_df',
                    'keep_history': True,
                    'topic': 'metrics'
                }
        tasks (list): A list of tasks for the timers. Managed internally
        active_today (bool): Flag indicating if the executor is active today.
        running (bool): Flag indicating if the executor is currently running.
        logger (Logger): The logger object for logging.
        task (Task): The main task for running the daily cycle.
        subscribers (dict): A dictionary of subscribers for different topics.
    Methods:
        __init__: Initialize the AsyncBaseExecutor object.
        process_day_begin: Perform initialization logic at the beginning of the trading day.
        process_day_end: Perform cleanup logic at the end of the trading day.
        process_event: Perform processing logic at regular intervals during the trading day.
        start: Start the executor.
        stop: Stop the executor.
        add_subscriber: Add a subscriber to a topic.
        remove_subscriber: Remove a subscriber from a topic.
        notify_subscribers: Notify subscribers of a topic with data.
    """

    def __init__(
        self,
        name,
        start_time="09:15",
        end_time="15:30",
        interval=None,
        timers=None,
    ):
        """
        Initialize the AsyncBaseExecutor object.

        Parameters:
        - name (str): The name of the executor.
        - start_time (str, optional): The start time of the executor in the format "HH:MM".
          Defaults to "09:15".
        - end_time (str, optional): The end time of the executor in the format "HH:MM".
          Defaults to "15:30".
        - interval (int, optional): The interval in seconds between each execution.
          Only one of interval or timers can be provided. Defaults to None.
        - timers (list, optional): A list of timers for scheduled executions.
          Only one of interval or timers can be provided. Defaults to None.
          Each timer should be a dictionary with the following keys:
            - 'interval_seconds' (int): The interval in seconds between each execution.
            - 'method_name' (str): The name of the method to be executed at the end of the interval.
              It should be a coroutine function.
        """

        assert not (
            interval and timers
        ), "Only one of interval or timers can be provided"
        try:
            self.event_loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(
                "This class can only be instantiated within an asyncio event loop"
            )
        self.name = name
        self.start_time = datetime.strptime(start_time, "%H:%M").time()
        self.end_time = datetime.strptime(end_time, "%H:%M").time()
        self.timers: Union[Dict[str, Dict], None] = timers
        self._tasks: Dict[str, asyncio.Task] = dict()
        self.interval = interval
        self.running = False
        self.logger = logging.getLogger(__name__)
        # self.executor = ThreadPoolExecutor(
        #     max_workers=1
        # )  # with I/O and syncrhoous api calls moving to more modern asyncio.to_task syntax, there is no need to get an executor
        self._daily_cycle_task: Union[asyncio.Task, None] = None
        self.subscribers: Dict[str, Set[Subscriber]] = dict()

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

    async def process_event(self, **kwargs):
        """
        This method should be implemented by the derived class to perform any
        processing logic that needs to be done at regular intervals during the trading day.
        Only relevant if you do not provide a list of timers as
        it is used in the default timer only that is created when `interval` is provided.
        """
        self.logger.info(f"Processing event for {self.name}.")
        result = None  # Placeholder for api call
        topic = "default" if "topic" not in kwargs else kwargs["topic"]
        self.notify_subscribers(topic, result)
        pass

    async def start(self):
        """
        This method should be called to start the executor
        """
        # This method does not start daily timers which
        # are started when _day_begin is called
        if self.running:
            self.logger.warning(
                f"Process {self.name} is already running, ignoring start request."
            )
            return
        self.running = True
        self.logger.info(f"Process {self.name} set to start running")

        self._daily_cycle_task = asyncio.create_task(self._daily_cycle())
        self.logger.info(f"Started async task for process: {self.name}")

    async def stop(self):
        """
        This method should be called to stop the executor
        """
        if not self.running:
            self.logger.warning(
                f"Process {self.name} is not running, ignoring stop request."
            )
            return
        self.running = False
        self.logger.info(f"Stopping process: {self.name}")
        await self._day_end()
        if self._daily_cycle_task:
            try:
                self._daily_cycle_task.cancel()
            except asyncio.CancelledError:
                pass
        self.logger.info(f"Stopped process: {self.name}")

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
                asyncio.create_task(
                    subscriber.process_notification(topic, data)
                )
            )
        # comneted out as we do not want to wait for all subscribers to finish
        # await asyncio.gather(*tasks)

    async def _day_begin(self):
        try:
            # instead of overriding _day_begin in sub class, we use hook pattern
            # so that derived class process_day_begin is called before starting timers
            await self.process_day_begin()
            self.logger.info(f"Day begin executed for {self.name}.")
            await self._start_timers()
            self.logger.info("Started all timers.")
        except Exception as e:
            self.logger.error(f"Error during day begin for {self.name}: {e}")

    async def _day_end(self):
        try:
            await self._stop_timers()
            # instead of overriding _day_end in sub class, we use hook pattern
            # so that derived class process_day_end is called after stopping timers
            await self.process_day_end()
            self.logger("Stopped all timers.")
            self.logger.info(f"Day end executed for {self.name}.")
        except Exception as e:
            self.logger.error(f"Error during day end for {self.name}: {e}")

    async def _daily_cycle(self):
        while True:
            now = datetime.now()
            today = now.date()

            if not is_trading_day(today):
                self.logger.info(
                    f"{today} is not a trading day. Sleeping till tomorrow."
                )
                await asyncio.sleep(self._seconds_to_tomorrow_begin())
            elif now.time() < self.start_time:
                sleep_seconds = (
                    datetime.combine(today, self.start_time) - now
                ).total_seconds()
                self.logger.info(
                    f"Sleeping for {sleep_seconds:.2f} seconds until day begin"
                )
                await asyncio.sleep(sleep_seconds)
            elif self.start_time <= now.time() < self.end_time:
                # this will also start the daily timers
                await self._day_begin()
                # sleep till end of day
                seconds_to_day_end = self.end_time - now.time()
                await asyncio.sleep(seconds_to_day_end)
            elif now.time() > self.end_time:
                # stop all daily timers
                await self._day_end()
                # sleep till next day
                await asyncio.sleep(self._seconds_to_tomorrow_begin())
            else:
                self.logger.warning(
                    "Invalid state reached. Check logic. now = {now}"
                )

    def _seconds_to_tomorrow_begin(self):
        now = datetime.now()
        next_start_time = datetime.combine(
            now.date() + timedelta(days=1), self.start_time
        )
        return (next_start_time - now).total_seconds()

    async def _run_timer_loop(self, interval_seconds, method_name, **kwargs):
        """
        Runs a timer in a loop for a specified interval and executes a function
        It is never called when self.running is Falsebecase all timers are stopped in that case
        """
        # process the tick
        try:
            now = datetime.now()
            async_func = getattr(self, method_name)
            await async_func(**kwargs)
        except Exception as e:
            self.logger.error(f"Error during event processing: {e}")
        next_fetch = now + timedelta(seconds=interval_seconds)
        sleep_time = (next_fetch - datetime.now()).total_seconds()
        sleep_time = max(
            0, sleep_time
        )  # catchup if last processing took longer
        self.logger.info(f"Sleeping for {sleep_time:.2f} seconds")
        await asyncio.sleep(sleep_time)

    def _is_valid_timer_defn(self, timer_config):
        if not isinstance(timer_config, dict):
            return False
        if (
            "interval_seconds" not in timer_config
            or "method_name" not in timer_config
        ):
            return False
        method_name = getattr(self, timer_config["method_name"], None)
        return self._is_valid_async_method(method_name)

    def _is_valid_async_method(self, method_name):
        if not hasattr(self, method_name):
            return False
        method = getattr(self, method_name)
        return asyncio.iscoroutinefunction(method)

    async def _start_timers(self):
        if len(self._tasks) > 0:
            await self._stop_timers()  # for cleanup that did not happen previous day
        self._tasks = dict()
        if not self.timers:
            self.timers = {
                "default": {
                    "interval_seconds": self.interval,
                    "method_name": "process_event",
                }
            }
        for timer_name, timer_config in self.timers.items():
            # Check if the timer definition is valid
            if not self._is_valid_timer_defn(timer_config):
                self.logger.error(
                    f"Invalid definition for timer-'{timer_name}': {timer_config}. Skipping."
                )
                continue
            # Start the timer and add it to the list of tasks
            task_name = timer_name
            task = asyncio.create_task(self._run_timer_loop(**timer_config))
            self._tasks[task_name] = task
            self.logger.info("Started all timers.")

    async def _stop_timers(self):
        for task_name, task in self._tasks.items():
            try:
                await task.cancel()
                del self._tasks[task_name]
            except asyncio.CancelledError:
                pass
        self._tasks = dict()  # not really needed as we are deleting all tasks
        self.logger.info("Stopped all timers.")

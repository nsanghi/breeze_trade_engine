import time
import logging
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from breeze_trade_engine.common.date_utils import is_trading_day


class Singleton:
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__new__(cls, *args, **kwargs)
        return cls._instances[cls]


class BaseExecutor(ABC):

    def __init__(self, name, start_time, end_time, interval):
        self.name = name
        self.start_time = datetime.strptime(start_time, "%H:%M").time()
        self.end_time = datetime.strptime(start_time, "%H:%M").time()
        self.interval = interval
        self.is_running = False
        self.subscribers = []
        self.active_today = False
        self.logger = logging.getLogger(__name__)
        self.run_trading_day()

    def __del__(self):
        self.day_end

    def start(self):
        if not self.is_running:
            self.is_running = True
            self.logger.info(f"Started process: {self.name}")
        else:
            self.logger.info(
                f"Process {self.name} is already running, cannot start again."
            )

    def stop(self):
        if self.is_running:
            self.is_running = False
            self.logger.info(f"Stopped process: {self.name}")
        else:
            self.logger.info(
                f"Process {self.name} is not running, cannot stop."
            )

    def day_begin(self):
        if self.active_today:  # skip if already active
            return
        self.process_day_begin()
        self.active_today = True
        self.logger.info("Trading day has begun. Activating {self.name}.")

    def day_end(self):
        if not self.active_today:
            return
        self.process_day_end()
        self.active_today = False
        print(
            "Trading day has ended. Performing cleanup and deactivating {self.name}."
        )

    @abstractmethod
    def process_day_begin(self):
        pass

    @abstractmethod
    def process_day_end(self):
        pass

    @abstractmethod
    def process_event(self):
        pass

    def run_trading_day(self):
        while True:
            now = datetime.now().time()
            today = datetime.now().date()
            if self.start_time <= now <= self.end_time and is_trading_day(
                today
            ):
                self.day_begin()
                while self.start_time <= now <= self.end_time:
                    if self.is_running:
                        self.process_event()
                    time.sleep(self.interval)
                    now = datetime.now().time()
                self.day_end()
            sleep_duration = self.calculate_sleep_duration()
            time.sleep(sleep_duration)

    def calculate_sleep_duration(self):
        now = datetime.now()
        next_start_time = datetime.combine(
            now.date() + timedelta(days=1), self.start_time
        )
        return (next_start_time - now).total_seconds()

    def add_subscriber(self, subscriber):
        self.subscribers.append(subscriber)

    def remove_subscriber(self, subscriber):
        self.subscribers.remove(subscriber)

    def notify_subscribers(self, data):
        for subscriber in self.subscribers:
            subscriber.process_data(data)

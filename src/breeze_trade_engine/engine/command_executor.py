import logging
import time
import json
import os
from datetime import time as dt_time
from breeze_trade_engine.executors.data import OptionChainDataFetcher, LiveFeed
from breeze_trade_engine.executors.strategy import StrategyOneExecutor
from dotenv import load_dotenv


class ProcessManager:
    def __init__(self, path):
        self.path = path
        self.processes = {}
        self.command_file = self.path + "/manager_commands.json"
        self.is_running = False

    def add_process(self, process):
        if process.name not in self.processes:
            self.processes[process.name] = process
            print(f"Added process: {process.name}")
        else:
            print(f"Process {process.name} already exists")

    def remove_process(self, process_name):
        if process_name in self.processes:
            self.processes[process_name].stop()
            del self.processes[process_name]
            print(f"Removed process: {process_name}")

    def start_process(self, process_name):
        if process_name in self.processes:
            self.processes[process_name].start()
            print(f"Started process: {process_name}")

    def stop_process(self, process_name):
        if process_name in self.processes:
            self.processes[process_name].stop()
            print(f"Stopped process: {process_name}")

    def start_all_processes(self):
        for process in self.processes.values():
            process.start()
        print("Started all processes")

    def stop_all_processes(self):
        for process in self.processes.values():
            process.stop()
        print("Stopped all processes")

    def list_processes(self):
        print("Current Processes:")
        for name, process in self.processes.items():
            status = "Running" if process.is_running else "Stopped"
            print(f"- {name}: {status}")

    def run(self):
        self.is_running = True
        while self.is_running:
            if os.path.exists(self.command_file):
                with open(self.command_file, "r") as f:
                    command = json.load(f)
                os.remove(self.command_file)
                self.execute_command(command)
            time.sleep(1)

    def execute_command(self, command):
        action = command.get("action")
        target = command.get("target")
        params = command.get("params", {})

        if action == "add":
            self.add_new_process(params)
        elif action == "remove":
            self.remove_remove(target)
        elif action == "start" and target in self.processes:
            self.start_process(target)
        elif action == "stop" and target in self.processes:
            self.stop_process(target)
        elif action == "start_all":
            self.start_all_processes()
        elif action == "stop_all":
            self.stop_all_processes()
        elif action == "list":
            self.list_processes()
        elif action == "shutdown":
            self.shutdown()
            self.is_running = False

    def add_new_process(self, params):
        process_type = params.get("type")
        start_time = dt_time.fromisoformat(params.get("start_time"))
        end_time = dt_time.fromisoformat(params.get("end_time"))
        interval = params.get("interval")

        if process_type == "option_chain" and process_type not in self.processes:
            # TODO: Think of subscription and notification mechanism for subscribers
            new_process = OptionChainDataFetcher(
                process_type, start_time, end_time, interval
            )
        elif process_type == "live_feed" and process_type not in self.processes:
            new_process = LiveFeed(process_type, start_time, end_time, interval)
        elif process_type == "startegy_one" and process_type not in self.processes:
            new_process = StrategyOneExecutor(
                process_type, start_time, end_time, interval
            )
        else:
            print(f"Unknown fetcher type: {process_type}")
            return

        if new_process:
            self.add_process(new_process)

    def shutdown(self):
        print("Shutting down the system...")
        self.stop_all_processes()


def main():
    load_dotenv()
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    logging.basicConfig(level=numeric_level)

    path = os.getenv("DIR_PATH", "data")
    manager = ProcessManager(path=path)

    # Create initial processes
    option_chain_fetcher = OptionChainDataFetcher(
        "OptionChainDataFetcher", dt_time(9, 15), dt_time(15, 30), 60
    )
    live_feed = LiveFeed("LiveFeed", dt_time(9, 15), dt_time(15, 30), 60)
    strategy_one = StrategyOneExecutor(
        "Crypto Fetcher", dt_time(9, 15), dt_time(15, 30), 60
    )
    strategy_one = StrategyOneExecutor(
        "Crypto Fetcher", dt_time(9, 15), dt_time(15, 30), 60
    )

    # Add fetchers to manager
    manager.add_data_fetcher(option_chain_fetcher)
    manager.add_data_fetcher(live_feed)

    # Add Startegy Executor to manager
    manager.set_live_feed(strategy_one)

    print("System is ready. Use the controller to start and stop components.")
    manager.run()


if __name__ == "__main__":
    main()

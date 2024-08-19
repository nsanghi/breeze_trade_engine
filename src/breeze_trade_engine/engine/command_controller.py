import json
import os
import argparse
from dotenv import load_dotenv


def send_command(action, target=None, params=None):
    command = {"action": action}
    if target:
        command["target"] = target
    if params:
        command["params"] = params

    path = os.getenv("DIR_PATH", "logs")
    with open(f"{path}/manager_commands.json", "w") as f:
        json.dump(command, f)


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Control the System")
    parser.add_argument(
        "action",
        choices=[
            "start",
            "start_all",
            "stop",
            "stop_all",
            "add",
            "remove",
            "list",
            "shutdown",
        ],
        help="Action to perform",
    )
    parser.add_argument(
        "--target",
        help="Target Process (e.g., 'option_chain', 'live_feed', 'strategy_one')",
    )
    parser.add_argument(
        "--type",
        choices=["option_chain", "live_feed", "strategy_one"],
        help="Process to add",
    )
    parser.add_argument(
        "--start-time", help="Start time for the new process (HH:MM)"
    )
    parser.add_argument(
        "--end-time", help="End time for the new process (HH:MM)"
    )
    parser.add_argument(
        "--interval", type=int, help="Interval in seconds for the new process"
    )

    args = parser.parse_args()

    if args.action in ["start", "stop", "remove"] and not args.target:
        print(f"Error: '{args.action}' action requires a --target")
        return

    if args.action == "add":
        if not all([args.type, args.start_time, args.end_time, args.interval]):
            print(
                "Error: 'add' action requires --type, --start-time, --end-time, and --interval"
            )
            return
        params = {
            "type": args.type,
            "start_time": args.start_time,
            "end_time": args.end_time,
            "interval": args.interval,
        }
        send_command(args.action, params=params)
    elif args.action in ["start", "stop", "remove"]:
        send_command(args.action, args.target)
    else:
        send_command(args.action)

    print(
        f"Sent command: {args.action} {args.target if args.target else ''} {params if args.action == 'add' else ''}"
    )


if __name__ == "__main__":
    main()

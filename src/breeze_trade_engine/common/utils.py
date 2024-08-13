import os
from datetime import datetime


def is_trading_day(day):
    # Read HOLIDAYS from .env file
    holidays = os.getenv("HOLIDAYS", "").split(",")

    # Convert day to date object if it's a string
    if isinstance(day, str):
        day = datetime.strptime(day, "%Y-%m-%d").date()

    # Check if day is not in HOLIDAYS list and not a Saturday or Sunday
    if day.strftime("%Y-%m-%d") not in holidays and day.weekday() < 5:
        return True
    else:
        return False

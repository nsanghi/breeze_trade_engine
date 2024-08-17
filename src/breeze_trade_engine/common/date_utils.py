import os
from datetime import datetime, timedelta
import calendar


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


def get_next_weekly_expiry(current_date=datetime.now()):
    """
    Returns the next Thursday expiry adjusing by -1 day for Thursday is a holiday
    Args:
        current_date: The date from which to calculate the Thursday. Defaults to
                     today's date.

    Returns:
        A Next weekly exipry of NIFTY50 options.
    """

    # Calculate the number of days to the next Thursday
    days_to_next_thursday = (calendar.THURSDAY - current_date.weekday()) % 7
    next_expiry = current_date + timedelta(days=days_to_next_thursday)
    if not is_trading_day(next_expiry):
        next_expiry = next_expiry - timedelta(days=1)
    return next_expiry


def get_all_expiry_dates(current_date=datetime.now()):
    """
    Returns a list of the dates of the next four Thursdays, including today,
    as well as the Thursdays of the next two months. If today is past the last
    Thursday of the month, it returns the last Thursdays of the next three months.

    Args:
        current_date: The date from which to calculate the Thursdays. Defaults to
                     today's date.

    Returns:
        A list of tz string format representing the next four Thursdays for
        weekly NIFTY50 option expiry and also the next 3 monthly expiry NIFTY50 options.
    """

    # Calculate the number of days to the next Thursday
    days_to_next_thursday = (calendar.THURSDAY - current_date.weekday()) % 7
    next_thursday = current_date + timedelta(days=days_to_next_thursday)

    # Get the next four Thursdays
    weekly_expiries = [next_thursday + timedelta(days=7 * i) for i in range(4)]

    # Get the last Thursday of the three months
    target_months = (
        3
        if next_thursday
        > next_thursday.replace(
            day=calendar.monthrange(current_date.year, current_date.month)[1]
        )
        else 2
    )
    monthly_expiries = []
    for i in range(1, target_months + 1):
        next_month = (current_date.month + i) % 12
        next_year = current_date.year + ((current_date.month + i) // 12)
        last_day_of_month = calendar.monthrange(next_year, next_month)[1]
        last_thursday_of_month = datetime(
            next_year, next_month, last_day_of_month
        )
        while last_thursday_of_month.weekday() != calendar.THURSDAY:
            last_thursday_of_month -= timedelta(days=1)
        monthly_expiries.append(last_thursday_of_month)

    # Combine the lists of all expiries
    all_expiries = weekly_expiries + monthly_expiries
    adjusted_expiries = []
    for day in all_expiries:
        if not is_trading_day():
            adjusted_expiries.append(day - timedelta(days=1))
        else:
            adjusted_expiries.append(day)
    adjusted_expiries_str = [
        date.strftime("%Y-%m-%dT15:30:00.000Z") for date in adjusted_expiries
    ]
    return adjusted_expiries_str

"""Time-related utility functions"""
from datetime import datetime, timedelta, date, time
from typing import Tuple


def get_datetime_range_today() -> Tuple[datetime, datetime]:
    """Get a tuple of datetimes which would be the first and last
       datetime for today.

    Returns:
        Tuple[datetime, datetime]: Start and End Datetimes for today.
    """
    date_today = date.today() - timedelta(days=1)
    range_start = datetime.combine(date_today, time(0, 0, 0))
    range_end = datetime.combine(date_today, time(23, 59, 59))

    return range_start, range_end


def get_datetime_today() -> str:
    """Get a string formatted datetime for now.

    Returns:
        str: String formatted datetime for now
    """
    return datetime.today().strftime("%Y-%m-%d %H:%M:%S")

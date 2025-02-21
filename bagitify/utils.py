"""Helpers too generic to be in other modules."""
import os

from pathlib import Path
from datetime import datetime as Datetime


DT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def are_same_fs(path1: Path, path2: Path) -> bool:
    """Check if two paths are on the same filesystem.

    Assuming that the paths exist, otherwise will raise error.
    """
    return os.stat(path1).st_dev == os.stat(path2).st_dev


def create_dir_if_not_exist(dir: Path):
    os.makedirs(dir, exist_ok=True)


def format_datetime(datetime: Datetime) -> str:
    return datetime.strftime(DT_FORMAT)


def parse_datetime(dt_str: str) -> Datetime:
    return Datetime.strptime(dt_str, DT_FORMAT)


def get_dataset_name_from_tabledap_url(tabledap_url: str) -> str:
    return tabledap_url.split("/")[-1]


def round_to_start_of_month(start_datetime: Datetime) -> Datetime:
    month_start = Datetime(
        day=1, month=start_datetime.month, year=start_datetime.year)
    return month_start


def round_to_next_month(end_datetime: Datetime) -> Datetime:
    if end_datetime.month < 12:
        next_month_start = Datetime(
            day=1, month=end_datetime.month + 1, year=end_datetime.year)
    else:
        next_month_start = Datetime(
            day=1, month=1, year=end_datetime.year + 1)
    return next_month_start

"""Helpers too generic to be in other modules."""
import os

from pathlib import Path


def are_same_fs(path1: Path, path2: Path) -> bool:
    """Check if two paths are on the same filesystem.
    
    Assuming that the paths exist, otherwise will raise error.
    """
    return os.stat(path1).st_dev == os.stat(path2).st_dev


def create_dir_if_not_exist(dir: Path):
    os.makedirs(dir, exist_ok=True)

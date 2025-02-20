"""Module for managing BagIt archives."""

from pathlib import Path


def is_bag(bag_directory: Path) -> bool:
    """Check if directory is a BagIt archive."""
    return (bag_directory / "bagit.txt").is_file()

# TODO move more stuff here

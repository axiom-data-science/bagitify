"""Module for managing BagIt archives."""

import bagit

from pathlib import Path


def is_bag(bag_directory: Path) -> bool:
    """Check if directory is a BagIt archive."""
    return (bag_directory / "bagit.txt").is_file()


def bag_it_up(bag_directory: Path, bagit_metadata: dict, create: bool = True):
    """Create or update a BagIt archive."""
    if create:
        bagit.make_bag(bag_directory, bag_info=bagit_metadata, checksums=["sha256"])
        return  # new bag created, done

    # Open the existing bag
    bag = bagit.Bag(str(bag_directory))
    # Update bag-info
    bag.info.update(bagit_metadata)
    # Any potentially new files have already been written to the `data` payload directory,
    # so just persist any metadata changes made and update manifests with checksums
    bag.save(manifests=True)

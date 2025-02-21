"""Generate NCEI bagit archives from an ERDDAP tabledap dataset.

For more information on the bagit standard, see: https://en.wikipedia.org/wiki/BagIt
"""

import requests

from pathlib import Path
from typing import Optional

from bagitify.bagit_wrapper import bag_it_up, is_bag
from bagitify.download import download_data_for_bag
from bagitify.metadata import prep_bagit_metadata
from bagitify.utils import (
    are_same_fs,
    create_dir_if_not_exist,
    Datetime,
    format_datetime,
    get_dataset_name_from_tabledap_url,
    parse_datetime,
)


def get_start_end(tabledap_url: str) -> tuple[Datetime, Datetime]:
    """Get the start and end times of the data in the tabledap dataset."""
    start_end_url = f'{tabledap_url}.csv0?time&orderByMinMax(%22time%22)'
    r = requests.get(start_end_url, allow_redirects=True)
    r.raise_for_status()
    processed = [
        parse_datetime(dt_str)
        for dt_str in r.content.decode("utf-8").strip().split("\n")
    ]
    start = processed[0]
    end = processed[1]
    return (start, end)


def run(
    tabledap_url: str,
    bag_directory: Optional[Path] = None,
    requested_start_datetime: Optional[Datetime] = None,
    requested_end_datetime: Optional[Datetime] = None,
    tmp_parent: Optional[Path] = None,
    verbose: bool = False,
    force: bool = False,
):
    """Generate a bagit archive from an ERDDAP tabledap dataset.

    Note: If `force` is `False` any existing files that are not in erddap will remain.
    """
    # use default bag directory based on dataset name if not provided
    if not bag_directory:
        bag_directory = Path.cwd() / "bagit_archives" / get_dataset_name_from_tabledap_url(tabledap_url)
    if not tmp_parent:
        tmp_parent = bag_directory.parent / ".tmp-bagitify"

    # create dirs if they don't already exist
    create_dir_if_not_exist(bag_directory)
    create_dir_if_not_exist(tmp_parent)

    # check filesystem is the same so that we can move data atomically
    if tmp_parent and not are_same_fs(tmp_parent, bag_directory):
        raise ValueError(
            f"Temporary directory '{tmp_parent}' must be on the same filesystem as the bag directory '{bag_directory}'."
        )

    # clean up tabledap url
    tabledap_url = tabledap_url.lower()
    # remove .html suffix if present
    if tabledap_url.endswith(".html"):
        tabledap_url = tabledap_url.removesuffix(".html")

    # determine actual range of data available in the target tabledap dataset
    data_start_datetime, data_end_datetime = get_start_end(tabledap_url)
    print(f'Dataset has time range {format_datetime(data_start_datetime)} - {format_datetime(data_end_datetime)}')

    # adjust start and end time if sane arguments were provided
    bag_start_datetime = max((d for d in [requested_start_datetime, data_start_datetime] if d))
    bag_end_datetime = min((d for d in [requested_end_datetime, data_end_datetime] if d))

    # make sure the bag directory exists
    bag_directory.mkdir(parents=True, exist_ok=True)

    download_data_for_bag(
        tabledap_url,
        bag_start_datetime,
        bag_end_datetime,
        bag_directory,
        tmp_parent=tmp_parent,
        verbose=verbose,
        force=force,
    )

    bagit_metadata = prep_bagit_metadata(tabledap_url)

    # update or create the bagit archive
    bag_it_up(bag_directory, bagit_metadata, create=not is_bag(bag_directory))

"""Generate NCEI bagit archives from an ERDDAP tabledap dataset.

For more information on the bagit standard, see: https://en.wikipedia.org/wiki/BagIt
"""

import bagit
import json
import os
import re
import requests
import tempfile

from datetime import datetime as Datetime
from pathlib import Path
from typing import Optional

from bagitify.bagit_wrapper import is_bag
from bagitify.utils import are_same_fs, create_dir_if_not_exist

DT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def get_start_end(tabledap_url: str) -> tuple[Datetime, Datetime]:
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


def format_datetime(datetime: Datetime) -> str:
    return datetime.strftime(DT_FORMAT)


def parse_datetime(dt_str: str) -> Datetime:
    return Datetime.strptime(dt_str, DT_FORMAT)


def should_download_netcdf(
    existing_nc: Optional[Path],
    force: bool,
    end_datetime: Datetime,
    verbose: bool
) -> bool:
    """Decide whether to download a netCDF file or not.
    
    Also print messages if verbose is True.
    """
    if not existing_nc or not existing_nc.is_file():
        # No existing file
        return True

    if force:
        if verbose:
            print(f"File '{existing_nc}' exists but downloads are forced. Will re-download and overwrite.")
        return True

    nc_mtime = Datetime.fromtimestamp(existing_nc.stat().st_mtime)
    if nc_mtime < end_datetime:
        # The file was written before the end date time for this monthly chunk's range,
        # therefore cannot contain the whole month of up to date data - unless somebody predicted the future :)
        if verbose:
            print(f"File '{existing_nc}' exists but was written before chunk ending {end_datetime}, re-downloading.")
        return True

    # File already exists and is up to date
    return False


def download_month_netcdf(
    tabledap_url: str,
    start_datetime: Datetime,
    destination_dir: Path,
    existing_files_dir: Optional[Path] = None,
    verbose: bool = False,
    force: bool = False,
):
    """Download netCDF file for the month starting with the provided datetime."""
    end_datetime = round_to_next_month(start_datetime)

    month_nc_url = f"{tabledap_url}.ncCFMA?&time>={format_datetime(start_datetime)}&time<{format_datetime(end_datetime)}"
    nc_filename = gen_nc_filename(tabledap_url, start_datetime)
    # destination can be a tmp directory so it might be different than existing files
    destination_nc_path = destination_dir / nc_filename
    existing_nc_path = existing_files_dir and existing_files_dir / nc_filename

    if not should_download_netcdf(existing_nc_path, force, end_datetime, verbose):
        if verbose:
            print(f"Skipping download. File '{existing_nc_path}' already exists.")
        return

    if verbose:
        print(f"Downloading nc for {format_datetime(start_datetime)} - {format_datetime(end_datetime)} to '{destination_nc_path}'.")

    r = requests.get(month_nc_url, allow_redirects=True)
    # dataset may contain data gaps one month or greater between start and end times
    if r.status_code == 404 and 'Your query produced no matching results' in str(r.content):
        print(f'No data found for month {format_datetime(start_datetime)}.')
        return
    r.raise_for_status()

    with open(destination_nc_path, "wb") as fp:
        fp.write(r.content)


def get_start_dates_for_date_range(start_datetime: Datetime, end_datetime: Datetime) -> list[Datetime]:
    month_start_dates = []
    current_start = round_to_start_of_month(start_datetime)
    if end_datetime != round_to_start_of_month(end_datetime):
        end_datetime = round_to_next_month(end_datetime)
    current_end = round_to_next_month(current_start)

    while current_end <= end_datetime:
        month_start_dates.append(current_start)
        current_start = current_end
        current_end = round_to_next_month(current_end)
    return month_start_dates


def download_netcdf_range(
    tabledap_url: str,
    start_datetime: Datetime,
    end_datetime: Datetime,
    destination_dir: Path,
    existing_files_dir: Optional[Path] = None,
    verbose: bool = False,
    force: bool = False,
):
    """Download netCDF files for each month in the provided time range."""
    for month_start_datetime in get_start_dates_for_date_range(start_datetime, end_datetime):
        download_month_netcdf(
            tabledap_url,
            month_start_datetime,
            destination_dir,
            existing_files_dir=existing_files_dir,
            verbose=verbose,
            force=force,
        )


def move_tmp_to_bag(
    bag_directory: Path,
    tmp_destination: Path,
    verbose: bool = False,
    force: bool = False,
):
    """Move necessary files from a tmp download directory to the bag directory."""
    bag_data_dir = bag_directory / "data"
    bag_exists = is_bag(bag_directory)

    if bag_exists and not force:
        any_files_moved = False
        # only kinda atomic - move individual netcdfs to the bag with existing netcdfs
        for nc_file in tmp_destination.iterdir():
            any_files_moved = True
            os.rename(nc_file, bag_data_dir / nc_file.name)
        if verbose and any_files_moved:
            print(f"Moved files from '{tmp_destination}' to '{bag_data_dir}'")
        return

    if bag_exists:
        # delete existing contents of `bag/data` so that tmp dir can overwrite it
        for existing_nc_file in bag_data_dir.iterdir():
            os.remove(existing_nc_file)
        if verbose:
            print(f"Cleared existing files in '{bag_data_dir}'")
    # move/rename tmp dir to the destination dir (either `bag` or `bag/data`)
    os.rename(tmp_destination, bag_data_dir if bag_exists else bag_directory)
    if verbose:
        print(f"Moved '{tmp_destination}' to '{bag_data_dir}'")


def download_data_for_bag(
    tabledap_url: str,
    start_datetime: Datetime,
    end_datetime: Datetime,
    bag_directory: Path,
    tmp_parent: Path,
    verbose: bool = False,
    force: bool = False,
):
    """Atomically-ish* download netCDF files for each month in the provided time range.

    *Download netCDF files to a temporary directory, then move it to the bag directory.
    If updating an existing bag without the force flag, files will be moved one by one.
    """
    with tempfile.TemporaryDirectory(prefix="bagitify-data-", dir=tmp_parent) as tmpdir:
        # 1. Download necessary files to a tmp dir
        tmp_destination = Path(tmpdir)
        download_netcdf_range(
            tabledap_url,
            start_datetime,
            end_datetime,
            destination_dir=tmp_destination,
            existing_files_dir=bag_directory / "data" if is_bag(bag_directory) else None,
            verbose=verbose,
            force=force,
        )
        # 2. move stuff from the tmp dir
        move_tmp_to_bag(bag_directory, tmp_destination, verbose=verbose, force=force)


def gen_nc_filename(tabledap_url: str, start_datetime: Datetime) -> str:
    name_parts = [get_dataset_name_from_tabledap_url(tabledap_url)]
    name_parts.append(start_datetime.strftime("%Y-%m") + ".nc")
    name = "_".join(name_parts)
    return name


def get_dataset_name_from_tabledap_url(tabledap_url: str) -> str:
    return tabledap_url.split("/")[-1]


def get_metadata(tabledap_url: str) -> dict:
    metadata_url = tabledap_url.replace("/tabledap/", "/info/") + "/index.json"
    r = requests.get(metadata_url, allow_redirects=True)
    r.raise_for_status()
    metadata = json.loads(r.content.decode("utf-8"))
    return metadata


def parse_tabledap_metadata(tabledap_metadata: dict) -> dict:
    rows = tabledap_metadata["table"]["rows"]
    nested = {}
    for row in rows:
        row_type = row[0]
        var_name = row[1]
        att_name = row[2]
        data_type = row[3]
        data_value = row[4]

        if row_type not in nested:
            nested[row_type] = {}

        if var_name not in nested[row_type]:
            nested[row_type][var_name] = {}

        nested[row_type][var_name][att_name] = {
            "data_type": data_type, "data_value": data_value}
    return nested


def prep_bagit_metadata(tabledap_url: str, config_metadata: dict) -> dict:
    tabledap_metadata = parse_tabledap_metadata(get_metadata(tabledap_url))
    bagit_metadata = config_metadata
    bagit_metadata["External-Description"] = (
      f'Sensor data from station {"".join(tabledap_url.split("/")[-1].split(".")[0:-1])}'
    )
    title = tabledap_metadata["attribute"]["NC_GLOBAL"]["title"]["data_value"]
    bagit_metadata["External-Identifier"] = title

    return bagit_metadata


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


def config_metadata_from_env() -> dict:
    config_items = ["Bag-Group-Identifier", "Contact-Email", "Contact-Name",
                    "Contact-Phone", "Organization-address", "Source-Organization"]

    config_metadata = {}
    env_keys = list(dict(os.environ).keys())

    for item in config_items:
        var_name = "BAGIT_" + item.upper().replace("-", "_")
        r = re.compile(f'^{var_name}')

        vars_from_env = list(filter(r.match, env_keys))
        if len(vars_from_env) < 1:
            print(f'Warning: {var_name} not set! Defaulting to empty string.')
            from_env = ""
            # If we want to exit instead, or perform more validation, change this.
        elif len(vars_from_env) == 1:
            from_env = os.environ.get(vars_from_env[0])
        else:
            from_env = [os.environ.get(v) for v in vars_from_env]

        config_metadata[item] = from_env

    return config_metadata


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

    config_metadata = config_metadata_from_env()
    bagit_metadata = prep_bagit_metadata(tabledap_url, config_metadata)

    # update or create the bagit archive
    bag_it_up(bag_directory, bagit_metadata, create=not is_bag(bag_directory))

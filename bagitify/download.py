"""Functions related to downloading data from ERDDAP."""

import os
from pathlib import Path
import tempfile
from typing import Optional

import requests
from bagitify.bagit_wrapper import is_bag
from bagitify.utils import (
    Datetime,
    format_datetime,
    get_dataset_name_from_tabledap_url,
    round_to_next_month,
    round_to_start_of_month,
)


def gen_nc_filename(tabledap_url: str, start_datetime: Datetime) -> str:
    name_parts = [get_dataset_name_from_tabledap_url(tabledap_url)]
    name_parts.append(start_datetime.strftime("%Y-%m") + ".nc")
    name = "_".join(name_parts)
    return name


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
    dest_dir = bag_data_dir if bag_exists else bag_directory
    os.rename(tmp_destination, dest_dir)
    # temp directories are only accessible by the creating user, so make accessible
    os.chmod(dest_dir, 0o755)
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
    dataset_name = get_dataset_name_from_tabledap_url(tabledap_url)
    with tempfile.TemporaryDirectory(prefix=f"{dataset_name}-", dir=tmp_parent) as tmpdir:
        if verbose:
            print(f"Using bag directory '{bag_directory}'")
            print(f"Using temp directory '{tmpdir}'")
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

#!/usr/bin/env python3

import bagit
import click
import datetime
import json
import os
import re
import requests

from pathlib import Path

dt_format = "%Y-%m-%dT%H:%M:%SZ"


def get_start_end(tabledap_url: str) -> (datetime, datetime):
    start_end_url = f'{tabledap_url}.csv0?time&orderByMinMax(%22time%22)'
    r = requests.get(start_end_url, allow_redirects=True)
    processed = [parse_datetime(dt_str) for dt_str in r.content.decode(
        "utf-8").strip().split("\n")]
    start = processed[0]
    end = processed[1]
    return (start, end)


def round_to_start_of_month(start_datetime: datetime) -> datetime:
    month_start = datetime.datetime(
        day=1, month=start_datetime.month, year=start_datetime.year)
    return month_start


def round_to_next_month(end_datetime: datetime) -> datetime:
    if end_datetime.month < 12:
        next_month_start = datetime.datetime(
            day=1, month=end_datetime.month + 1, year=end_datetime.year)
    else:
        next_month_start = datetime.datetime(
            day=1, month=1, year=end_datetime.year + 1)
    return next_month_start


def format_datetime(datetime: datetime) -> str:
    return datetime.strftime(dt_format)


def parse_datetime(dt_str: str) -> datetime:
    return datetime.datetime.strptime(dt_str, dt_format)


def get_month_netcdf(tabledap_url: str, start_datetime: datetime, bag_directory: Path, verbose: bool = True):
    end_datetime = round_to_next_month(start_datetime)

    month_nc_url = (
        f"{tabledap_url}.ncCFMA?&time>="
        + format_datetime(start_datetime)
        + "&time<"
        + format_datetime(end_datetime)
    )
    nc_filename = gen_nc_filename(tabledap_url, start_datetime)
    nc_path = os.path.join(bag_directory, nc_filename)
    if verbose:
        print(
            f'Downloading nc for {format_datetime(start_datetime)} - {format_datetime(end_datetime)} to {nc_path} ...')
    r = requests.get(month_nc_url, allow_redirects=True)
    # dataset may contain data gaps one month or greater between start and end times
    if r.status_code == 404 and 'Your query produced no matching results' in str(r.content):
        print(f'No data found for month {format_datetime(start_datetime)}.')
        return
    else:
        r.raise_for_status()

    with open(nc_path, "wb") as fp:
        fp.write(r.content)


def get_start_dates_for_date_range(start_datetime: datetime, end_datetime: datetime) -> list[datetime]:
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


def get_range_netcdf(tabledap_url: str, start_datetime: datetime, end_datetime: datetime, bag_directory: Path, verbose: bool = True):
    for month_start_datetime in get_start_dates_for_date_range(start_datetime, end_datetime):
        get_month_netcdf(tabledap_url, month_start_datetime, bag_directory, verbose=verbose)


def gen_nc_filename(tabledap_url: str, start_datetime: datetime) -> str:
    name_parts = [get_dataset_name_from_tabledap_url(tabledap_url)]
    name_parts.append(start_datetime.strftime("%Y-%m") + ".nc")
    name = "_".join(name_parts)
    return name


def get_dataset_name_from_tabledap_url(tabledap_url: str) -> str:
    return tabledap_url.split("/")[-1]


def get_metadata(tabledap_url: str) -> dict:
    metadata_url = tabledap_url.replace("/tabledap/", "/info/") + "/index.json"
    r = requests.get(metadata_url, allow_redirects=True)
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


def gen_archive(bag_directory: Path, bagit_metadata: dict):
    bag = bagit.make_bag(
        bag_directory, bag_info=bagit_metadata, checksums=["sha256"])

    bag.save(manifests=True)
    # should determine if this is needed here.


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


def run(tabledap_url: str, bag_directory: Path, requested_start_datetime: datetime = None, requested_end_datetime: datetime = None):
    # clean up tabledap url
    tabledap_url = tabledap_url.lower()
    # remove .html suffix if present
    if tabledap_url.endswith(".html"):
        tabledap_url = tabledap_url.removesuffix(".html")

    # determine actual range of data available in the target tabledap dataset
    data_start_datetime, data_end_datetime = get_start_end(tabledap_url)
    print(f'Dataset has time range {format_datetime(data_start_datetime)} - {format_datetime(data_end_datetime)}')

    # adjust start and end time if sane arguments were provided
    bag_start_datetime = max((d for d in [requested_start_datetime, data_start_datetime] if d), default=None)
    bag_end_datetime = min((d for d in [requested_end_datetime, data_end_datetime] if d), default=None)

    # use default bag directory based on dataset name if not provided
    if not bag_directory:
        bag_directory = Path.cwd() / "bagit_archives" / get_dataset_name_from_tabledap_url(tabledap_url)

    bag_directory.mkdir(parents=True, exist_ok=True)

    get_range_netcdf(tabledap_url, bag_start_datetime, bag_end_datetime, bag_directory)

    config_metadata = config_metadata_from_env()
    bagit_metadata = prep_bagit_metadata(tabledap_url, config_metadata)
    gen_archive(bag_directory, bagit_metadata)


@click.command()
@click.option('-d', '--bag-directory', type=click.Path(writable=True, file_okay=False, path_type=Path))
@click.option('-s', '--start-date', type=click.DateTime(), default=None)
@click.option('-e', '--end-date', type=click.DateTime(), default=None)
@click.argument('tabledap_url')
def cli(
  bag_directory: Path,
  start_date: datetime,
  end_date: datetime,
  tabledap_url: str,
):
    """Generate NCEI bagit archives from an ERDDAP tabledap dataset at TABLEDAP_URL."""
    run(tabledap_url, bag_directory, start_date, end_date)


if __name__ == "__main__":
    cli()

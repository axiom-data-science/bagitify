import argparse
import bagit
import json
import requests
import os
import datetime
import re


dt_format = "%Y-%m-%dT%H:%M:%SZ"


def get_start_end(erddap_url):
    start_end_url = erddap_url.replace(
        ".html", ".csv0?time&orderByMinMax(%22time%22)")
    r = requests.get(start_end_url, allow_redirects=True)
    processed = [parse_datetime(dt_str) for dt_str in r.content.decode(
        "utf-8").strip().split("\n")]
    start = processed[0]
    end = processed[1]
    return (start, end)


def round_to_last_month(start_datetime):
    month_start = datetime.datetime(
        day=1, month=start_datetime.month, year=start_datetime.year)
    return month_start


def round_to_next_month(end_datetime):
    if end_datetime.month < 12:
        next_month_start = datetime.datetime(
            day=1, month=end_datetime.month + 1, year=end_datetime.year)
    else:
        next_month_start = datetime.datetime(
            day=1, month=1, year=end_datetime.year + 1)
    return next_month_start


def format_datetime(datetime):
    return datetime.strftime(dt_format)


def parse_datetime(dt_str):
    return datetime.datetime.strptime(dt_str, dt_format)


def get_month_netcdf(erddap_url, start_datetime, bag_directory, verbose=True):
    end_datetime = round_to_next_month(start_datetime)

    month_nc_url = erddap_url.replace(".html", ".ncCFMA?&time>=") + format_datetime(
        start_datetime) + "&time<" + format_datetime(end_datetime)
    nc_filename = gen_nc_filename(erddap_url, start_datetime)
    nc_path = os.path.join(bag_directory, "data", nc_filename)
    if verbose:
        print(
            f'Downloading nc for {format_datetime(start_datetime)} - {format_datetime(end_datetime)} to {nc_path} ...')
    r = requests.get(month_nc_url, allow_redirects=True)
    with open(nc_path, "wb") as fp:
        fp.write(r.content)
    if verbose:
        print("Done.")


def get_range_netcdf(erddap_url, start_datetime, end_datetime, bag_directory, verbose=True):
    current_start = start_datetime
    current_end = round_to_next_month(current_start)
    while current_end <= end_datetime:
        get_month_netcdf(erddap_url, current_start,
                         bag_directory, verbose=verbose)
        current_start = current_end
        current_end = round_to_next_month(current_end)


def gen_nc_filename(erddap_url, start_datetime):
    name_parts = erddap_url.split("/")[-1].split("_")[0:-1]
    name_parts.append(start_datetime.strftime("%Y-%m") + ".nc")
    name = "_".join(name_parts)
    return name


def gen_bag_dirname(erddap_url, start_datetime, end_datetime):
    name_parts = erddap_url.split("/")[-1].split("_")[0:-1]
    name_parts.append(start_datetime.strftime("%Y-%m"))
    name_parts.append(end_datetime.strftime("%Y-%m"))
    name_parts.append("bagit")
    name = "_".join(name_parts)
    return name


def get_metadata(erddap_url):
    metadata_url = erddap_url.replace(
        "/tabledap/", "/info/").replace(".html", "/index.json")
    r = requests.get(metadata_url, allow_redirects=True)
    metadata = json.loads(r.content.decode("utf-8"))
    return metadata


def parse_erddap_metadata(erdapp_metadata):
    rows = erdapp_metadata["table"]["rows"]
    nested = {}
    for row in rows:
        row_type = row[0]
        var_name = row[1]
        att_name = row[2]
        data_type = row[3]
        data_value = row[4]

        if not row_type in nested:
            nested[row_type] = {}

        if not var_name in nested[row_type]:
            nested[row_type][var_name] = {}

        nested[row_type][var_name][att_name] = {
            "data_type": data_type, "data_value": data_value}
    return nested


def prep_bagit_metadata(erddap_url, config_metadata):
    erddap_metadat = parse_erddap_metadata(get_metadata(erddap_url))
    bagit_metadata = config_metadata
    bagit_metadata["External-Description"] = f'Sensor data from station {erddap_url.split("/")[-1].split(".")[0:-1]}'
    title = erddap_metadat["attribute"]["NC_GLOBAL"]["title"]["data_value"]
    bagit_metadata["External-Identifier"] = title

    return bagit_metadata


def gen_archive(bag_directory, bagit_metadata):
    bag = bagit.make_bag(
        bag_directory, bag_info=bagit_metadata, checksums=["sha256"])

    bag.save(manifests=True)
    # should determine if this is needed here.


def config_metadata_from_env():
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("ERDDAP_url", help="ERDDAP tabledap url to archive")
    parser.add_argument("-d", "--directory",
                        help="bag directory to use for archiving")
    parser.add_argument("-s", "--start", help="start timestamp")
    parser.add_argument("-e", "--end", help="end timestamp")

    args = parser.parse_args()
    # args.func(args)

    erddap_url = args.ERDDAP_url

    if args.start is None or args.end is None:
        start_datetime, end_datetime = get_start_end(erddap_url)

    if not args.start is None:
        start_datetime = parse_datetime(args.start)
    if not args.end is None:
        end_datetime = parse_datetime(args.end)

    if args.directory is None:
        bag_directory = os.path.join(os.getcwd(), "bagit_archives", gen_bag_dirname(
            erddap_url, start_datetime, end_datetime))
    else:
        bag_directory = args.directory

    try:
        os.mkdir(bag_directory)
        os.mkdir(os.path.join(bag_directory, "data"))
    except OSError as error:
        print(error)

    get_range_netcdf(erddap_url, start_datetime, end_datetime, bag_directory)

    config_metadata = config_metadata_from_env()
    bagit_metadata = prep_bagit_metadata(erddap_url, config_metadata)
    gen_archive(bag_directory, bagit_metadata)


if __name__ == "__main__":
    main()

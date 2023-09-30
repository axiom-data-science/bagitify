import bagit
import json
import requests
import os
import datetime


dt_format="%Y-%m-%dT%H:%M:%SZ"

def get_start_end(erddap_url):
    start_end_url = erddap_url.replace(".html",".csv0?time&orderByMinMax(%22time%22)")
    r = requests.get(start_end_url, allow_redirects=True)
    processed = [parse_datetime(dt_str) for dt_str in r.content.decode("utf-8").strip().split("\n")]
    start = processed[0]
    end = processed[1]
    return (start, end)

def round_to_last_month(start_datetime):
    month_start = datetime.datetime(day=1,month=start_datetime.month, year=start_datetime.year)
    return month_start

def round_to_next_month(end_datetime):
    next_month_start = datetime.datetime(day=1,month=end_datetime.month +1, year=end_datetime.year)
    return next_month_start

def format_datetime(datetime):
    return datetime.strftime(dt_format)

def parse_datetime(dt_str):
    return datetime.datetime.strptime(dt_str, dt_format)

def get_month_netcdf(erddap_url, start_datetime, bag_directory):
    end_datetime = round_to_next_month(start_datetime)
    month_nc_url = erddap_url.replace(".html", ".ncCFMA?&time>=") + format_datetime(start_datetime) + "&time<" + format_datetime(end_datetime)
    r = requests.get(month_nc_url, allow_redirects=True)
    nc_filename = gen_nc_filename(erddap_url, start_datetime)
    nc_path = os.path.join(bag_directory, "data", nc_filename)
    with open(nc_path, "wb") as fp:
        fp.write(r.content)
    
def get_range_netcdf(erddap_url, start_datetime, end_datetime, bag_directory):
    current_start = start_datetime
    current_end = round_to_next_month(current_start)
    while current_end <= end_datetime:
        get_month_netcdf(erddap_url, current_start, bag_directory)
        current_start = current_end
        current_end = round_to_next_month(current_end)

def gen_nc_filename(erddap_url, start_datetime):
    name_parts = erddap_url.split("/")[-1].split("_")[0:-1]
    name_parts.append(start_datetime.strftime("%Y-%m") +".nc")
    name = "_".join(name_parts)
    return name
    
def get_metadata(erddap_url):
    metadata_url = erddap_url.replace("/tabledap/","/info/").replace(".html","/index.json")
    r = requests.get(metadata_url, allow_redirects=True)
    metadata = json.loads(r.content.decode("utf-8"))
    return metadata


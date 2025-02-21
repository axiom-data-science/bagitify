"""Functions to prepare metadata for the BagIt archive."""

import json
import os
import re
import requests


def config_metadata_from_env() -> dict:
    """Get metadata from environment variables."""
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


def prep_bagit_metadata(tabledap_url: str) -> dict:
    tabledap_metadata = parse_tabledap_metadata(get_metadata(tabledap_url))
    bagit_metadata = config_metadata_from_env()
    bagit_metadata["External-Description"] = (
      f'Sensor data from station {"".join(tabledap_url.split("/")[-1].split(".")[0:-1])}'
    )
    title = tabledap_metadata["attribute"]["NC_GLOBAL"]["title"]["data_value"]
    bagit_metadata["External-Identifier"] = title

    return bagit_metadata

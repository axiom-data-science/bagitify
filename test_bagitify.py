from datetime import datetime
import json
import os
import shutil

import bagitify

testing_netcdfs = "test-data/netcdfs"
testing_metadata = "test-data/metadata.json"


def test_get_start_dates_for_date_range():
    assert bagitify.get_start_dates_for_date_range(
        start_datetime=datetime(2023, 3, 22),
        end_datetime=datetime(2023, 6, 19),
    ) == [
        datetime(2023, 3, 1),
        datetime(2023, 4, 1),
        datetime(2023, 5, 1),
        datetime(2023, 6, 1),
    ]

    assert bagitify.get_start_dates_for_date_range(
        start_datetime=datetime(2023, 11, 1),
        end_datetime=datetime(2024, 4, 1),
    ) == [
        datetime(2023, 11, 1),
        datetime(2023, 12, 1),
        datetime(2024, 1, 1),
        datetime(2024, 2, 1),
        datetime(2024, 3, 1),
    ]

    assert bagitify.get_start_dates_for_date_range(
        start_datetime=datetime(2023, 11, 1),
        end_datetime=datetime(2024, 4, 2),
    ) == [
        datetime(2023, 11, 1),
        datetime(2023, 12, 1),
        datetime(2024, 1, 1),
        datetime(2024, 2, 1),
        datetime(2024, 3, 1),
        datetime(2024, 4, 1),
    ]


def test_archive_creation(tmp_path):
    tmp_bag_dir = os.path.join(tmp_path, "bagdir")
    shutil.copytree(testing_netcdfs, tmp_bag_dir, symlinks=False, ignore=None, copy_function=shutil.copy2,
                    ignore_dangling_symlinks=False, dirs_exist_ok=False)
    with open(testing_metadata, "r") as fp:
        bagit_metadata = json.load(fp)

    bagitify.gen_archive(tmp_bag_dir, bagit_metadata)

    assert os.path.exists(os.path.join(tmp_bag_dir, "data", "edu_usf_marine_comps_2022-07.nc"))

    baginfo_path = os.path.join(tmp_bag_dir, "bag-info.txt")
    manifest_path = os.path.join(tmp_bag_dir, "manifest-sha256.txt")
    tagmanifest_path = os.path.join(tmp_bag_dir, "tagmanifest-sha256.txt")

    assert os.path.exists(baginfo_path)
    assert os.path.exists(manifest_path)
    assert os.path.exists(tagmanifest_path)

    with open(tagmanifest_path) as fp:
        manifest_lines = fp.readlines()

    split_lines = [line.strip().split(" ") for line in manifest_lines]

    manifest_hashes = {split_line[1]: split_line[0] for split_line in split_lines}

    assert manifest_hashes["bagit.txt"] == "e91f941be5973ff71f1dccbdd1a32d598881893a7f21be516aca743da38b1689"
    assert manifest_hashes["manifest-sha256.txt"] == "2e63f8ad244293b0da33b88c9f5c94269d22e95b05e3af869e2ffa294b89a8a3"

    with open(manifest_path) as fp:
        manifest_lines = fp.readlines()

    split_lines = [line.strip().split("  ") for line in manifest_lines]

    manifest_hashes = {split_line[1]: split_line[0] for split_line in split_lines}

    assert manifest_hashes["data/edu_usf_marine_comps_2022-05.nc"] == "932f9152b7f907ed234316af6324d51092275cbdaab1671714810921eca8f935"
    assert manifest_hashes["data/edu_usf_marine_comps_2022-06.nc"] == "1300a37604ca9347f1128281e9c1fde1481d346ac9a3c78ffa5bbf60e6d9a28d"
    assert manifest_hashes["data/edu_usf_marine_comps_2022-07.nc"] == "a517c884a683b8fb695bd4d27fa6830fcf847a3ca66551df1fd839ddeb10c24d"
    assert manifest_hashes["data/edu_usf_marine_comps_2022-08.nc"] == "7ee270db9184d261204a6ce3fbe58e43429ddcc2f2be55c8787ad0fd9ef0c225"

    with open(baginfo_path, "r") as fp:
        baginfo_lines = fp.readlines()
    split_lines = [line.strip().split(": ") for line in baginfo_lines]
    baginfo_pairs = dict()
    for split_line in split_lines:
        key = split_line[0]
        value = split_line[1]
        if key in baginfo_pairs:
            if isinstance(baginfo_pairs[key], list):
                baginfo_pairs[key].append(value)
            else:
                baginfo_pairs[key] = [baginfo_pairs[key], value]
        else:
            baginfo_pairs[key] = value

    assert baginfo_pairs["Bag-Group-Identifier"] == "bgi"
    assert set(baginfo_pairs["Contact-Phone"]) == {"cp", "cp2"}

# Bagitify

Creates bagit archives of sensor data retrieved from ERDDAP.

## Usage

### To run from the command line:

First, set environment variables as necessary for thebagit metadata. Variables are as follows:

```
BAGIT_BAG_GROUP_IDENTIFIER
BAGIT_CONTACT_EMAIL
BAGIT_CONTACT_NAME
BAGIT_CONTACT_PHONE
BAGIT_ORGANIZATION_ADDRESS
BAGIT_SOURCE_ORGANIZATION
```

Any that are not set will default to emptry string in the metadata.

Bagitify supports multiple instances of any of these, accomplished by suffixing the variable names
with a string of your choice. A number or something descriptive of the reason for the duplication
probably makes the most sense, but it is basically arbitrary.

For example, setting both `BAGIT_CONTACT_PHONE` and `BAGIT_CONTACT_PHONE_2` would result in
two phone numbers being saved in the bagit metadata.

To actually call the program, run

```bash
bagitify [-d DIRECTORY] [-s START] [-e END] [-v] [-f] <tabledap_url>`
```

`-d` allows the user to specify the directory to create the bagit archive in. If not set,
the default is a directory in `./bagit_archives` with an autogenerated name like
`edu_usf_marine_comps_2022-05_2022-09_bagit` based on the tabledap url, start, and end dates.

`-s` and `-e` specify start and end dates. Expected datetime format is `%Y-%m-%d` or `%Y-%m-%dT%H:%M:%SZ`.
For example, `2022-05-01` and `2022-05-01T00:00:00Z` are valid. If a start or end is not set, it defaults to
the start or end of the data in ERDDAP, repsectively. In any case, it will be internally
rounded to the first day of the month for the start, and the first day of the next month for the end.

`-v` activates verbose mode, where additional information on operations is printed.

`-f` activates force mode, where existing files are deleted and redownloaded.

Finally, `tabledap_url` is an ERDDAP tabledap url such as `https://erddap.secoora.org/erddap/tabledap/edu_usf_marine_comps_1407d550.html`

Putting it all together, bagitify might be run like so:

```bash
bagitify -s 2022-05-01 -e 2022-08-01 https://erddap.secoora.org/erddap/tabledap/edu_usf_marine_comps_1407d550.html
```

By default, monthly netCDF files which have already been downloaded are skipped/not redownloaded, __unless__ the
end of the month is after the modification date of the local file. This behavior ensures that the netCDF file for
the current month gets updated as new data is collected.

To force existing files to be deleted and redownloaded, set the `-f` or `--force` argument.

### Docker

The Docker version works essentially the same way, though the variables will need to be set through the docker command,
and it will be important to bind mount the place it will be writing to so that you can get the results. For example: 

```
docker build -t bagitify .

docker run \
  -e BAGIT_BAG_GROUP_IDENTIFIER="bgi" \
  -e BAGIT_CONTACT_EMAIL="fake@email.address" \
  -e BAGIT_CONTACT_NAME="John Doe" \
  -e BAGIT_CONTACT_PHONE="(123) 456-7890" \
  -e BAGIT_ORGANIZATION_ADDRESS="123 Fake Street, Some Town, AK 12345" \
  -e BAGIT_SOURCE_ORGANIZATION="Fake Org" \
  -v ./ncei-archives:/srv/bagitify/bagit_archives \
  bagitify -s 2022-05-01 -e 2022-08-01 -v https://erddap.secoora.org/erddap/tabledap/edu_usf_marine_comps_1407d550.html
```

An example Docker Compose file is also provided in this repository at `docker-compose.yml`.
The mounted data directory must be writable by the user in the bagitify container,
which is user is 57439 by default but can be set via the `BAGITIFY_USER` environment variable.

Example to run using Docker Compose using your user id:

```
mkdir -p ./ncei-archives
BAGITIFY_USER=$(id -u) docker compose run bagitify
```

Environment variables can also be managed using `--env-file` in `docker run`.

## Installation

1. Install dependencties into a new conda environment:
    ```
    conda env create -f environment.yml
    ```
2. Then activate the environment:
    ```
    conda activate bagitify
    ```
3. And install the executable if you wish:
    ```
    pip install .
    ```
    - alternatively, to run the program you can just execute the module `python -m bagitify.cli`
    - if you install with `pip` you should be able to run `bagitify`

## Testing

With the project installed and environment active, run
```
python -m pytest
```

This will collect and run all tests in the `test/` dir like `pytest` does.

## Contributing

Contributions via pull request are welcome. Please add tests for any new features and fix any
formatting issues identified by `flake8` prior to submitting.

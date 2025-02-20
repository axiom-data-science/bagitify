"""Command line interface for bagitify."""
from pathlib import Path
from typing import Optional
import click

from bagitify.bagitify import Datetime, run

CLICK_DATETIME_FORMATS = ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%SZ']


@click.command()
@click.option(
    '-d',
    '--bag-directory',
    type=click.Path(writable=True, file_okay=False, path_type=Path),
    help='Directory to create the bagit archive in',
)
@click.option('-s', '--start-date', type=click.DateTime(CLICK_DATETIME_FORMATS), default=None, help='Data start date')
@click.option('-e', '--end-date', type=click.DateTime(CLICK_DATETIME_FORMATS), default=None, help='Data end date')
@click.option('-v', '--verbose/--no-verbose', default=False, help='Print more information about the process')
@click.option('-f', '--force/--no-force', default=False, help='Delete and redownload existing files')
@click.argument('tabledap_url')
def cli(
  bag_directory: Optional[Path],
  start_date: Optional[Datetime],
  end_date: Optional[Datetime],
  verbose: bool,
  force: bool,
  tabledap_url: str,
):
    """Generate NCEI bagit archives from an ERDDAP tabledap dataset at TABLEDAP_URL."""
    run(tabledap_url, bag_directory, start_date, end_date, verbose, force)


if __name__ == "__main__":
    cli()

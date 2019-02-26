import csv
import json
import os
import sys

import click
from jinja2 import Environment
from jinja2.loaders import FileSystemLoader

root_location = os.path.abspath(os.path.dirname(__file__))

env = Environment(
    loader=FileSystemLoader(os.path.join(root_location, 'sql')),
    trim_blocks=True,
    lstrip_blocks=True,
)

@click.group()
def cli():
    """DW Generator"""
    pass

@cli.command()
@click.option('--schema', help='The schema name')
@click.option('--table', help='The table name')
@click.option('--ddls', help='The ddl directory', type=click.Path(exists=True), required=True)
def table(schema, table, ddls):
  """Read table definition"""
  with open(os.path.join(ddls, '{}.csv'.format(table))) as table_file:
    rows = list(csv.DictReader(table_file, delimiter=';'))

  print(list(rows[0].keys()))
  for row in rows:
    print(list(row.values()))

  # click.secho("{}".format('Hello World!'), file=sys.stderr, fg='cyan')
  

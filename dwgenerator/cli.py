import json
import os
import sys

import click
from jinja2 import Environment
from jinja2.loaders import FileSystemLoader

from .dbobjects import Schema, Table

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
  tbl = Table.read(os.path.join(ddls, schema, '{}.csv'.format(table)))
  print(tbl)
  tbl.print_table()

@cli.command()
@click.option('--schema', help='The schema name')
@click.option('--ddls', help='The ddl directory', type=click.Path(exists=True), required=True)
def schema(schema, ddls):
  """Read schema content"""
  sch = Schema.read(os.path.join(ddls, schema))
  print(sch)
  sch.print_schema()

  

import json
import os
import sys

import click
from jinja2 import Environment
from jinja2.loaders import FileSystemLoader

from .dbobjects import Schema, Table
from .mappings import TableMappings, ColumnMappings

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

@cli.command()
@click.option('--mappings', help='The mappings directory', type=click.Path(exists=True), required=True)
@click.option('--source', help='Mappings from schema.table')
@click.option('--target', help='Mappings to schema.table')
def table_mappings(mappings, source, target):
  """Read table mappings"""
  m = TableMappings.read(mappings)
  if source:
    m = m.from_table(*source.split('.'))
  if target:
    m = m.to_table(*target.split('.'))
  m.print_mappings()

@cli.command()
@click.option('--mappings', help='The mappings directory', type=click.Path(exists=True), required=True)
@click.option('--source', help='Mappings from schema.table[.column]')
@click.option('--target', help='Mappings to schema.table[.column]')
def column_mappings(mappings, source, target):
  """Read column mappings"""
  m = ColumnMappings.read(mappings)
  if source:
    source_parts = source.split('.')
    if len(source_parts) == 2:
      m = m.from_table(*source_parts)
    if len(source_parts) == 3:
      m = m.from_column(*source_parts)
  if target:
    target_parts = target.split('.')
    if len(target_parts) == 2:
      m = m.to_table(*target_parts)
    if len(target_parts) == 3:
      m = m.to_column(*target_parts)
  m.print_mappings()

@cli.command()
@click.option('--mappings', help='The mappings directory', type=click.Path(exists=True), required=True)
def mapping_sources(mappings):
  """Infer source tables from column mappings"""
  m = ColumnMappings.read(mappings)
  tables = m.source_tables()
  for column in tables:
    print(column)
    
@cli.command()
@click.option('--mappings', help='The mappings directory', type=click.Path(exists=True), required=True)
def mapping_targets(mappings):
  """Infer target tables from column mappings"""
  m = ColumnMappings.read(mappings)
  tables = m.target_tables()
  for column in tables:
    print(column)
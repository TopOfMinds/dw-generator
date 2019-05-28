import json
import os
import sys
from pathlib import Path

import click
from jinja2 import Environment
from jinja2.loaders import FileSystemLoader

from .dbobjects import Schema, Table, create_typed_table, Hub, Link, Satellite
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
@click.option('--schema', help='The schema name', required=True)
@click.option('--table', help='The table name', required=True)
@click.option('--ddls', help='The ddl directory', type=click.Path(exists=True), required=True)
def table(schema, table, ddls):
  """Read table definition"""
  tbl = Table.read(os.path.join(ddls, schema, '{}.csv'.format(table)))
  print(tbl)
  tbl.print_table()

@cli.command()
@click.option('--schema', help='The schema name', required=True)
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
  for table in tables:
    typed_table = create_typed_table(table)
    if typed_table:
      print(typed_table)

@cli.command()
@click.option('--mappings', help='The mappings directory', type=click.Path(exists=True), required=True)
@click.option('--target', help='Mappings to schema.table')
def generate_view(mappings, target):
  """Generate view SQl for a table"""
  mappings_path = Path(mappings)
  tm = TableMappings.read(mappings_path / 'table')
  cm = ColumnMappings.read(mappings_path / 'column')
  tables = cm.target_tables()
  if target:
    schema_name, table_name = target.split('.')
    tables = [table for table in tables if table.schema == schema_name and table.name == table_name]
  for table in tables:
    target_table = create_typed_table(table)
    print(target_table)
    source_tables = tm.to_table(target_table.schema, target_table.name).table_mappings
    source_column_mappings = cm.to_table(target_table.schema, target_table.name)
    # source_column_mappings.print_mappings()
    for source_table in source_tables:
      print('\t{source_schema}.{source_table}, filter={source_filter}'.format(**source_table))
      target_column_mappings = source_column_mappings.from_table(source_table['source_schema'], source_table['source_table'])
      source_columns = target_column_mappings.column_mappings
      if isinstance(target_table, Hub):
        target_column_mappings.to_column(column=target_table.key).print_mappings()
        for column in target_table.business_keys:
          target_column_mappings.to_column(column=column).print_mappings()
        target_column_mappings.to_column(column=target_table.load_dts).print_mappings()
        target_column_mappings.to_column(column=target_table.rec_src).print_mappings()
      else:
        for source_column in source_columns:
          print('\t\t{src_schema}.{src_table}.{src_column}, transformation={transformation}'.format(**source_column))


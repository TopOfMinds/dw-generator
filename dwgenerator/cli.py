import json, sys, csv
from pathlib import Path

import click

from .dbobjects import Schema, Table, create_typed_table, Hub, Link, Satellite, MetaDataError, MetaDataWarning
from .mappings import TableMappings, ColumnMappings, Mappings
from .templates import render

@click.group()
def cli():
    """DW Generator"""
    pass

@cli.command()
@click.option('--metadata', help='The metadata directory', type=click.Path(exists=True), default='metadata', show_default=True)
@click.option('--dbtype', help='The target database type', default='snowflake', show_default=True)
@click.option('--target', help='Mappings to schema.table')
@click.option('--out', help='Output directory')
@click.option('-v', '--verbose', help='Print extra information', count=True)
def generate_view(metadata, dbtype, target, out, verbose):
  """Generate view SQL for a table"""
  metadata_path = Path(metadata)
  mappings_path = metadata_path / 'mapping'
  tm = TableMappings.read(mappings_path / 'table')
  cm = ColumnMappings.read(mappings_path / 'column')
  target_tables_path = metadata_path / 'target_tables.csv'
  with open(target_tables_path, encoding='utf-8') as target_tables_file:
    target_tables = list(csv.DictReader(target_tables_file, dialect=csv.excel))
  table_def_path = metadata_path / 'table_def'
  target_table_paths = [
    table_def_path / target_table['schema'] / (target_table['table'] + '.csv')
    for target_table in target_tables if target_table['generate'] == 'true'
  ]
  tables = [create_typed_table(Table.read(target_table_path)) for target_table_path in target_table_paths]
  mappings = Mappings(tm, cm, tables + cm.source_tables())
  if target:
    schema_name, table_name = target.split('.')
    tables = [table for table in tables if table.schema == schema_name and table.name == table_name]
  for target_table in tables:
    try:
      if verbose: 
        click.secho(str(target_table), file=sys.stderr, fg='cyan')
      if target_table.table_type in ['hub', 'link', 'satellite']:
        target_table.check()
        mappings.check(target_table)
        sql = render(target_table, mappings, dbtype, 'view')
        if out:
          outpath = Path(out) / target_table.schema / (target_table.name + '_v.sql')
          click.secho(str(outpath), file=sys.stderr, fg='green')
          with open(outpath, 'w') as outfile:
            outfile.write(sql)
        else:
          print(sql)
      else:
        click.secho('Unknown table type: {}'.format(target_table), file=sys.stderr, fg='yellow')
    except MetaDataError as e:
        click.secho("Meta data error: {}".format(e), file=sys.stderr, fg='red')
        # break
    except MetaDataWarning as e:
        click.secho("Meta data warning: {}".format(e), file=sys.stderr, fg='yellow')

@cli.group()
def util():
  """Various utily commands"""
  pass

@util.command()
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

@util.command()
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

@util.command()
@click.option('--mappings', help='The mappings directory', type=click.Path(exists=True), required=True)
def mapping_sources(mappings):
  """Infer source tables from column mappings"""
  m = ColumnMappings.read(mappings)
  tables = m.source_tables()
  for column in tables:
    print(column)
    
@util.command()
@click.option('--mappings', help='The mappings directory', type=click.Path(exists=True), required=True)
def mapping_targets(mappings):
  """Infer target tables from column mappings"""
  m = ColumnMappings.read(mappings)
  tables = m.target_tables()
  for table in tables:
    typed_table = create_typed_table(table)
    if typed_table:
      print(typed_table)

@util.command()
@click.option('--mappings', help='The mappings directory', type=click.Path(exists=True), required=True)
@click.option('--target', help='Mappings to schema.table')
def generate_params(mappings, target):
  """Generate view metadata for a table"""
  mappings_path = Path(mappings)
  tm = TableMappings.read(mappings_path / 'table')
  cm = ColumnMappings.read(mappings_path / 'column')
  tables = cm.target_tables()
  mappings = Mappings(tm, cm, tables + cm.source_tables())
  if target:
    schema_name, table_name = target.split('.')
    tables = [table for table in tables if table.schema == schema_name and table.name == table_name]
  for target_table in tables:
    print(target_table.full_name)
    for source_table in mappings.source_tables(target_table):
      print('\t{}: filter={}'.format(
        source_table.full_name,
        mappings.filter(source_table, target_table)
      ))
      if isinstance(target_table, Hub):
        target_columns = [target_table.key] + target_table.business_keys + [target_table.load_dts, target_table.rec_src]
      if isinstance(target_table, Link):
        target_columns = [target_table.root_key] + target_table.keys + [target_table.load_dts, target_table.rec_src]
      if isinstance(target_table, Satellite):
        target_columns = [target_table.key] + target_table.attributes + [target_table.load_dts, target_table.rec_src]
      if target_columns: 
        for column in target_columns:
          if column:
            source_map = mappings.source_column(source_table, column)
            if source_map:
              print('\t\t{target} <= ({transformation}) <= {source}'.format(target=column, **source_map))
            else:
              print('\t\t{target} <= None'.format(target=column))


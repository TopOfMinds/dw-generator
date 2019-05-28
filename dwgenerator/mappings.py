import csv
from pathlib import Path
from itertools import groupby
from .dbobjects import Column, Table, Schema

class TableMappings:
  def __init__(self, table_mappings):
    self.table_mappings = table_mappings

  @classmethod
  def read(cls, path):
    paths = Path(path).glob('*.csv')
    table_mappings = []
    for path in paths:
      with open(path, encoding='utf-8') as mappings_file:
        table_mappings += list(csv.DictReader(mappings_file, delimiter=','))
    return TableMappings(table_mappings)

  def from_table(self, schema, table):
    return TableMappings([
      mapping for mapping in self.table_mappings 
      if mapping['source_schema'] == schema and mapping['source_table'] == table
    ])

  def to_table(self, schema, table):
    return TableMappings([
      mapping for mapping in self.table_mappings 
      if mapping['target_schema'] == schema and mapping['target_table'] == table
    ])

  def print_mappings(self):
    for mapping in self.table_mappings:
      print('{source_schema}.{source_table}\t{source_filter}\t{target_schema}.{target_table}'.format(**mapping))


class ColumnMappings:
  def __init__(self, column_mappings):
    self.column_mappings = column_mappings

  @classmethod
  def read(cls, path):
    paths = Path(path).glob('*.csv')
    column_mappings = []
    for path in paths:
      with open(path, encoding='utf-8') as mappings_file:
        column_mappings += [m for m in csv.DictReader(mappings_file, dialect=csv.excel) if not m['src_schema'].startswith('--')]
    return ColumnMappings(column_mappings)

  def from_table(self, schema, table):
    return ColumnMappings([
      mapping for mapping in self.column_mappings 
      if mapping['src_schema'] == schema and mapping['src_table'] == table
    ])

  def to_table(self, schema, table):
    return ColumnMappings([
      mapping for mapping in self.column_mappings 
      if mapping['tgt_schema'] == schema and mapping['tgt_table'] == table
    ])

  def from_column(self, schema, table, column):
    return ColumnMappings([
      mapping for mapping in self.column_mappings 
      if mapping['src_schema'] == schema and mapping['src_table'] == table and mapping['src_column'] == column
    ])

  def to_column(self, schema, table, column):
    return ColumnMappings([
      mapping for mapping in self.column_mappings 
      if mapping['tgt_schema'] == schema and mapping['tgt_table'] == table and mapping['tgt_column'] == column
    ])

  def source_tables(self):
    schema_table_name = lambda m: [m['src_schema'], m['src_table']]
    table_groups = groupby(sorted(self.column_mappings, key=schema_table_name), key=schema_table_name)
    tables = [
      Table(
        key[0],
        key[1],
        [Column(column, None, None) for column in set(m['src_column'] for m in group)],
        None
      ) for key, group in table_groups]
    return tables

  def target_tables(self):
    schema_table_name = lambda m: [m['tgt_schema'], m['tgt_table']]
    table_groups = groupby(sorted(self.column_mappings, key=schema_table_name), key=schema_table_name)
    tables = [
      Table(
        key[0],
        key[1],
        [Column(column, None, None) for column in set(m['tgt_column'] for m in group)],
        None
      ) for key, group in table_groups]
    return tables

  def print_mappings(self):
    for mapping in self.column_mappings:
      print('{src_schema}.{src_table}.{src_column}\t{transformation}\t{tgt_schema}.{tgt_table}.{tgt_column}'.format(**mapping))
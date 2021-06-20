import csv, re
from os import path
from pathlib import Path
from itertools import groupby
from .dbobjects import Column, Table, Schema, create_typed_table, MetaDataError, MetaDataWarning

TRANSFORM_PARAM_RE = re.compile(r"\$(\d+)")

class TableMappings(list):
  def __init__(self, table_mappings):
    super().__init__(table_mappings)

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
      mapping for mapping in self
      if mapping['source_schema'] == schema and mapping['source_table'] == table
    ])

  def to_table(self, schema, table):
    return TableMappings([
      mapping for mapping in self
      if mapping['target_schema'] == schema and mapping['target_table'] == table
    ])

  def print_mappings(self):
    for mapping in self:
      print('{source_schema}.{source_table}\t{source_filter}\t{target_schema}.{target_table}'.format(**mapping))


class ColumnMappings(list):
  def __init__(self, column_mappings):
    super().__init__(column_mappings)

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
      mapping for mapping in self
      if mapping['src_schema'] == schema and mapping['src_table'] == table
    ])

  def to_table(self, schema, table):
    return ColumnMappings([
      mapping for mapping in self 
      if mapping['tgt_schema'] == schema and mapping['tgt_table'] == table
    ])

  def from_column(self, schema, table, column):
    return ColumnMappings([
      mapping for mapping in self 
      if mapping['src_schema'] == schema and mapping['src_table'] == table and mapping['src_column'] == column
    ])

  def to_column(self, schema=None, table=None, column=None):
    if isinstance(column, Column):
      schema = column.parent.schema
      table = column.parent.name
      column = column.name
    return ColumnMappings([
      mapping for mapping in self 
      if mapping['tgt_schema'] == schema and mapping['tgt_table'] == table and mapping['tgt_column'] == column
    ])

  def to_column_list(self, column):
    column_mappings = self.to_column(column.parent.schema, column.parent.name, column.name)
    return [{
      'source_full_name': '{src_schema}.{src_table}.{src_column}'.format(**column_mapping),
      'source': '{src_column}'.format(**column_mapping),
      'transformation': column_mapping['transformation']
    } for column_mapping in column_mappings]

  def source_tables(self):
    schema_table_name = lambda m: [m['src_schema'], m['src_table']]
    table_groups = groupby(sorted(self, key=schema_table_name), key=schema_table_name)
    tables = [
      create_typed_table(Table(
        key[0],
        key[1],
        [Column(column, None, None) for column in set(m['src_column'] for m in group)],
        None
      )) for key, group in table_groups]
    return tables

  def target_tables(self):
    schema_table_name = lambda m: [m['tgt_schema'], m['tgt_table']]
    table_groups = groupby(sorted(self, key=schema_table_name), key=schema_table_name)
    tables = [
      create_typed_table(Table(
        key[0],
        key[1],
        [Column(column, None, None) for column in dict.fromkeys(m['tgt_column'] for m in group)], # order preserving dedup
        None
      )) for key, group in table_groups]
    return tables

  def print_mappings(self):
    for mapping in self:
      print('{src_schema}.{src_table}.{src_column}\t{transformation}\t{tgt_schema}.{tgt_table}.{tgt_column}'.format(**mapping))


def apply_transform(column_names, transform, prefix):
  if prefix:
    column_names = ["{}.{}".format(prefix, column_name) for column_name in column_names]
  if transform:
    # params = TRANSFORM_PARAM_RE.findall(transform)
    return TRANSFORM_PARAM_RE.sub(lambda s: column_names[int(s.group(1)) - 1], transform)
  else:
    return ';'.join(column_names)

class Mappings:
  def __init__(self, table_mappings, column_mappings, tables):
    self.table_mappings = table_mappings
    self.column_mappings = column_mappings
    self.tables = tables
    self._table_dict = dict((table.full_name, table) for table in tables)

  def source_tables(self, target_table):
    source_mappings = self.table_mappings.to_table(target_table.schema, target_table.name)
    source_table_names = ['{source_schema}.{source_table}'.format(**m) for m in source_mappings]
    source_tables = [self._table_dict.get(table_name) for table_name in source_table_names]
    return [source_table for source_table in source_tables if source_table]

  def filter(self, source_table, target_table):
    mappings = (self.table_mappings
                .from_table(source_table.schema, source_table.name)
                .to_table(target_table.schema, target_table.name)
                )
    try:
      return mappings[0]['source_filter']
    except IndexError:
      return None

  def source_columns(self, source_table, target_column, prefix=None):
    if target_column:
      target_table = target_column.parent
      source_column_mappings = self.column_mappings.to_table(target_table.schema, target_table.name)
      target_column_mappings = source_column_mappings.from_table(source_table.schema, source_table.name)
      source_maps = target_column_mappings.to_column_list(target_column)
      result = [
        apply_transform(source_map['source'].split(';'), source_map['transformation'], prefix)
        for source_map in source_maps
      ]
      return result
    else:
      return []

  def source_column(self, source_table, target_column, prefix=None):
    source_columns = self.source_columns(source_table, target_column, prefix)
    if len(source_columns) > 0:
      return source_columns[0]
    else:
      return None

  def check(self, target_table):
    source_mappings = self.table_mappings.to_table(target_table.schema, target_table.name)
    source_table_names = ['{source_schema}.{source_table}'.format(**m) for m in source_mappings]
    if len(source_mappings) == 0:
      raise MetaDataError('There is no table mappings to {target_table_name}'.format(
        target_table_name=target_table.full_name
      ))
    source_tables = self.source_tables(target_table)
    if len(source_tables) == 0:
      raise MetaDataError('There is no column mappings to {target_table_name} from {source_table_names}'.format(
        target_table_name=target_table.full_name,
        source_table_names=', '.join(source_table_names)
      ))
    for target_column in target_table.columns:
      # print(target_column)
      if target_table.table_type in ['version_pointer']:
        src_columns = self.column_mappings.to_column(target_table.schema, target_table.name, target_column.name)
        if len(src_columns) < 1:
          raise MetaDataError(f'There is no mapping to {target_column.full_name}')
      else:
        for source_table in source_tables:
          # print(source_table.full_name)
          source_column = self.source_column(source_table, target_column)
          # print(source_column)
          if source_column == None:
            raise MetaDataError('There is no mapping to {target_column_name} from {source_table_name}'.format(
              target_column_name=target_column.full_name, source_table_name=source_table.full_name
            ))

  def path(self, vp):
    source_tables = set(l.full_name for l in self.source_tables(vp))
    metrics_mapping = self.column_mappings.to_column(vp.schema, vp.name, vp.metrics_key.name)[0]
    context_mapping = self.column_mappings.to_column(vp.schema, vp.name, vp.context_key.name)[0]
    current_column = vp.parent[metrics_mapping['src_table']][metrics_mapping['src_column']]
    goal_column = vp.parent[context_mapping['src_table']][context_mapping['src_column']]
    # print('goal:', goal_column.full_name)
    path_ = [current_column]
    seen = set([current_column.full_name])
    while current_column.full_name != goal_column.full_name:
      # print('current:', current_column.full_name)
      current_table = current_column.parent
      if current_table.table_type == 'hub':
        goals = [
          sat.key for sat in current_table.related_satellites
          if sat.key.full_name not in seen and sat.key == goal_column
        ]
        if goals:
          current_column = goals[0]
        else:
          columns = [
            fk.columns[0] for l in current_table.related_links
            for fk in l.fks
            if l.full_name in source_tables
            and fk.columns[0].full_name not in seen
            and fk.foreign_table_name == current_table.name
          ]
          if columns:
            current_column = columns[0]
          else:
            raise MetaDataError(f'No link from {current_column.full_name}')
      elif current_table.table_type == 'link':
        columns = [
          (fk.foreign_columns[0], fk.columns[0]) for fk in current_table.fks
          if fk.columns[0].full_name not in seen and fk.foreign_table.full_name in source_tables
        ]
        if columns:
          (current_column, link_key) = columns[0]
          path_.append(link_key)
          seen.add(link_key.full_name)
        else:
          raise MetaDataError(f'No hub from {current_column.full_name}')
      elif current_table.table_type == 'satellite':
        current_column = current_table.related_hub[0].key
      else:
        raise MetaDataError(f'Wrong type, table={current_table} should not be in a vp path')
      path_.append(current_column)
      seen.add(current_column.full_name)
    return path_


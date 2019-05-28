import csv
from glob import glob
from os.path import join
from pathlib import PurePath

class Column:
  def __init__(self, name, type_, parent):
    self.name = name
    self.type = type_
    self.parent = parent

  @property
  def full_name(self):
    return "{}.{}".format(self.parent.full_name, self.name)

  def __str__(self):
    return "{}: {}".format(self.name, self.type)

class Columns(list):
  pass

class Table:
  def __init__(self, schema, name, columns, path):
    self.schema = schema
    self.name = name
    self.columns = columns
    self.path = path
    for column in columns:
      column.parent = self
  
  @classmethod
  def read(cls, path):
    p = PurePath(path)
    table_name = p.stem
    schema_name = p.parts[-2]
    # The table defs are saved as utf-8 BOM, which they shouldn't, and this is handled by utf-8-sig
    with open(path, encoding='utf-8-sig') as table_file:
      orig_columns = list(csv.DictReader(table_file, delimiter=','))
    columns = [Column(column['name'], column['type'], None) for column in orig_columns]
    table = Table(schema_name, table_name, columns, path)
    return table

  @property
  def full_name(self):
    return "{}.{}".format(self.schema, self.name)

  def __str__(self):
    return "{}({})".format(self.full_name, ", ".join(str(c) for c in self.columns))

  def print_table(self):
    for column in self.columns:
      print(column.full_name)

class Schema:
  def __init__(self, name, tables, path):
    self.name = name
    self.tables = tables
    self.path = path

  @classmethod
  def read(cls, path):
    p = PurePath(path)
    schema = p.stem
    tables_glob = join(path, '*.csv')
    table_paths = glob(tables_glob)
    tables = [Table.read(table_path) for table_path in table_paths]
    return Schema(schema, tables, path)
    for table in tables:
      print(table)

  def __str__(self):
    return "{}: {}".format(self.name, self.path)

  def print_schema(self):
    for table in self.tables:
      print(table)

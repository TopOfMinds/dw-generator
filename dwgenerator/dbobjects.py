import csv
from glob import glob
from os.path import join
from pathlib import PurePath

class Table:
  def __init__(self, schema, name, columns, path):
    self.schema = schema
    self.name = name
    self.columns = columns
    self.path = path
  
  @classmethod
  def read(cls, path):
    p = PurePath(path)
    table = p.stem
    schema = p.parts[-2]
    with open(path) as table_file:
      columns = list(csv.DictReader(table_file, delimiter=';'))
    return Table(schema, table, columns, path)

  def __str__(self):
    return "{}.{}: {}".format(self.schema, self.name, self.path)

  def print_table(self):
    print(list(self.columns[0].keys()))
    for column in self.columns:
      print(list(column.values()))

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

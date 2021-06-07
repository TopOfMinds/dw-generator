import csv, re
from glob import glob
from os.path import join
from pathlib import PurePath

class MetaDataError(Exception):
    pass

class MetaDataWarning(Warning):
    pass

class Column:
  def __init__(self, name, type_, parent=None):
    self.name = name
    self.type = type_
    self.parent = parent

  @property
  def full_name(self):
    return "{}.{}".format(self.parent.full_name, self.name)

  def __eq__(self, other):
    return isinstance(other, Column) and vars(self) == vars(other)

  def __str__(self):
    return "{}: {}".format(self.name, self.type)

class Columns(list):
  pass

class Table:
  def __init__(self, schema, name, columns, path=None, **properties):
    self.schema = schema
    self.name = name
    self.columns = columns
    self.path = path
    self.properties = properties
    for column in columns:
      column.parent = self
  
  @classmethod
  def from_columns(cls, schema_name, table_name, columns, path=None):
    is_property = lambda column: len(column['name']) >= 1 and column['name'][0] == '#'
    properties = {
      column['name'][1:].lower(): column['type']
      for column in columns if is_property(column)
    }
    new_columns = [
      Column(column['name'].lower(), column['type'], None)
      for column in columns if not is_property(column)
    ]
    table = Table(schema_name, table_name, new_columns, path, **properties)
    return table

  @classmethod
  def read(cls, path):
    p = PurePath(path)
    table_name = p.stem
    schema_name = p.parts[-2]
    # The table defs are saved as utf-8 BOM, which they shouldn't, and this is handled by utf-8-sig
    with open(path, encoding='utf-8-sig') as table_file:
      orig_columns = list(csv.DictReader(table_file, delimiter=','))
    return cls.from_columns(schema_name, table_name, orig_columns, path)

  @property
  def full_name(self):
    return "{}.{}".format(self.schema, self.name)

  def __str__(self):
    return "{}({})".format(self.full_name, ", ".join(str(c) for c in self.columns))

  def __getitem__(self, column_name):
    try:
      return [column for column in self.columns if column.name == column_name][0]
    except IndexError:
      return None

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

  def __str__(self):
    return "{}: {}".format(self.name, self.path)

  def print_schema(self):
    for table in self.tables:
      print(table)

class DataVaultObject(Table):
  table_type='dv'
  load_dts_name = 'load_dts'
  rec_src_name = 'rec_src'
  column_role_names = [load_dts_name, rec_src_name]
  def __init__(self, table):
    super().__init__(table.schema, table.name, table.columns, table.path)

  @property
  def load_dts(self):
    return self[self.load_dts_name]

  @property
  def rec_src(self):
    return self[self.rec_src_name]

  def check(self):
    for column_name in self.column_role_names:
      column = getattr(self, column_name)
      if column == None:
        raise MetaDataError('{table} is a {type} and must have a {column_name} column'.format(
          table=self.full_name, type=self.table_type, column_name=column_name
        ))
      if isinstance(column, list) and len(column) == 0:
        raise MetaDataError('{table} is a {type} and must have at lest one {column_name} column'.format(
          table=self.full_name, type=self.table_type, column_name=column_name
        ))

class Hub(DataVaultObject):
  table_type = 'hub'
  column_role_names = DataVaultObject.column_role_names + ['key', 'business_keys']
  def __init__(self, table):
    super().__init__(table)
    self.key_name = re.sub(r'_h$', '_key', self.name)

  @property
  def key(self):
    return self[self.key_name]

  @property
  def business_keys(self):
    return [
      c for c in self.columns 
      if c.name not in set([self.key_name, self.load_dts_name, self.rec_src_name])
    ]

  def __str__(self):
    return  "{full_name}(key={key}, business_keys=[{business_keys}], load_dts={load_dts}, rec_src={rec_src})".format(
      full_name=self.full_name,
      key=self.key,
      business_keys=', '.join(str(c) for c in self.business_keys),
      load_dts=self.load_dts,
      rec_src=self.rec_src,
    )

class Link(DataVaultObject):
  table_type='link'
  column_role_names = DataVaultObject.column_role_names + ['root_key', 'keys']
  def __init__(self, table):
    super().__init__(table)
    self.root_key_name = self.name + '_key'

  @property
  def root_key(self):
    return self[self.root_key_name]

  @property
  def keys(self):
    return [c for c in self.columns if c.name.endswith('_key') and c.name != self.root_key_name]

  def __str__(self):
    return  "{full_name}(root_key={root_key}, keys=[{keys}], load_dts={load_dts}, rec_src={rec_src})".format(
      full_name=self.full_name,
      root_key=self.root_key,
      keys=', '.join(str(c) for c in self.keys),
      load_dts=self.load_dts,
      rec_src=self.rec_src,
    )

class Satellite(DataVaultObject):
  table_type='satellite'
  column_role_names = DataVaultObject.column_role_names + ['key', 'attributes']
  def __init__(self, table):
    super().__init__(table)

  @property
  def key(self):
    try:
      return [c for c in self.columns if c.name.endswith('_key')][0]
    except IndexError:
      return None

  @property
  def attributes(self):
    return [
      c for c in self.columns 
      if c.name not in set([self.key.name if self.key else None, self.load_dts_name, self.rec_src_name])
    ]

  def __str__(self):
    return  "{full_name}(key={key}, attributes=[{attributes}], load_dts={load_dts}, rec_src={rec_src})".format(
      full_name=self.full_name,
      key=self.key,
      attributes=', '.join(str(c) for c in self.attributes),
      load_dts=self.load_dts,
      rec_src=self.rec_src,
    )


def create_typed_table(table):
  if table.name.endswith('_h'):
    return Hub(table)
  elif table.name.endswith('_l'):
    return Link(table)
  elif table.name.endswith('_s'):
    return Satellite(table)
  else:
    return table
import unittest

from dwgenerator.dbobjects import Schema, Table, Column, create_typed_table, Hub, Link, Satellite, MetaDataError, MetaDataWarning

class TestDBObjects(unittest.TestCase):
  def test_table_from_columns(self):
    orig_columns = [
      dict(name='key', type='numeric'),
      dict(name='field1', type='text'),
      dict(name='field2', type='numeric'),
      dict(name='#generate_type', type='table'),
      dict(name='#other_meta', type='foo'),
    ]
    table = Table.from_columns('db', 'test_table', orig_columns)
    self.assertEqual(table.properties, {'generate_type': 'table', 'other_meta': 'foo'})
    expected = [
      Column('key', 'numeric', table),
      Column('field1', 'text', table),
      Column('field2', 'numeric', table),
    ]
    self.assertEqual(table.columns, expected)

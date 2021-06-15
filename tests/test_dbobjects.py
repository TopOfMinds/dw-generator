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

  def create_example_hub(self, **properties):
    hub = create_typed_table(
      Table('dv', 'example_h', [
        Column('example_key', 'text'),
        Column('example_id1', 'text'),
        Column('example_id2', 'numeric'),
        Column('load_dts', 'numeric'),
        Column('rec_src', 'text'),
      ], **properties)
    )
    hub.check()
    return hub

  def test_hub_fields(self):
    hub = self.create_example_hub()
    self.assertEqual(hub.key.name, 'example_key')
    self.assertEqual([c.name for c in hub.business_keys], ['example_id1', 'example_id2'])
    self.assertEqual([c.name for c in hub.pk], ['example_key'])
    self.assertEqual([c.name for c in hub.uk], ['example_id1', 'example_id2'])
    self.assertEqual(hub.fks, [])

  def create_example_link(self, **properties):
    link = create_typed_table(
      Table('dv', 'example_l', [
        Column('example_l_key', 'text'),
        Column('example1_key', 'text'),
        Column('example2_key', 'text'),
        Column('load_dts', 'numeric'),
        Column('rec_src', 'text'),
      ], **properties)
    )
    link.check()
    return link

  def test_link_fields(self):
    link = self.create_example_link()
    self.assertEqual(link.root_key.name, 'example_l_key')
    self.assertEqual([c.name for c in link.keys], ['example1_key', 'example2_key'])
    self.assertEqual([c.name for c in link.pk], ['example_l_key'])
    self.assertEqual(link.uk, [])
    self.assertEqual(
      [fk.names() for fk in link.fks],
      [
        (('example_l', ['example1_key']), ('example1_h', ['example1_key'])),
        (('example_l', ['example2_key']), ('example2_h', ['example2_key']))
      ]
    )

  def create_example_satellite(self, **properties):
    satellite = create_typed_table(
      Table('dv', 'example_s', [
        Column('example_key', 'text'),
        Column('load_dts', 'numeric'),
        Column('attribute1', 'text'),
        Column('attribute2', 'numeric'),
        Column('rec_src', 'text'),
      ], **properties)
    )
    satellite.check()
    return satellite

  def test_satellite_fields(self):
    satellite = self.create_example_satellite()
    self.assertEqual(satellite.key.name, 'example_key')
    self.assertEqual([a.name for a in satellite.attributes], ['attribute1', 'attribute2'])
    self.assertEqual([c.name for c in satellite.pk], ['example_key', 'load_dts'])
    self.assertEqual(satellite.uk, [])
    self.assertEqual(
      [fk.names() for fk in satellite.fks],
      [(('example_s', ['example_key']), ('example_h', ['example_key']))]
    )

  def create_example_link_satellite(self, **properties):
    link_satellite = create_typed_table(
      Table('dv', 'example_l_s', [
        Column('example_l_key', 'text'),
        Column('load_dts', 'numeric'),
        Column('effective_ts', 'numeric'),
        Column('rec_src', 'text'),
      ], **properties)
    )
    link_satellite.check()
    return link_satellite

  def test_link_satellite_fields(self):
    link_satellite = self.create_example_link_satellite()
    self.assertEqual(link_satellite.key.name, 'example_l_key')
    self.assertEqual([a.name for a in link_satellite.attributes], ['effective_ts'])
    self.assertEqual([c.name for c in link_satellite.pk], ['example_l_key', 'load_dts'])
    self.assertEqual(
      [fk.names() for fk in link_satellite.fks],
      [(('example_l_s', ['example_l_key']), ('example_l', ['example_l_key']))]
    )

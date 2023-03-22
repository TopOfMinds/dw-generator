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

  def create_example_hub(self, table_number="", **properties):
    hub = create_typed_table(
      Table('dv', f'example{table_number}_h', [
        Column(f'example{table_number}_key', 'text'),
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

  def create_example_satellite(self, table_number="", **properties):
    satellite = create_typed_table(
      Table('dv', f'example{table_number}_s', [
        Column(f'example{table_number}_key', 'text'),
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

  def create_example_version_pointer(self, table1_number, table2_number, **properties):
    version_pointer = create_typed_table(
      Table('dv', f'example_{table1_number}_{table2_number}_vp', [
        Column(f'example{table1_number}_key', 'text'),
        Column(f'example{table2_number}_key', 'text'),
        Column(f'example{table2_number}_load_dts', 'numeric'),
        Column('load_dts', 'numeric'),
      ], **properties)
    )
    version_pointer.check()
    return version_pointer

  def test_version_pointer_fields(self):
    vp = self.create_example_version_pointer("1", "2")
    self.assertEqual(vp.name, 'example_1_2_vp')
    self.assertEqual(vp.metrics_key.name, 'example1_key')
    self.assertEqual(vp.context_key.name, 'example2_key')
    self.assertEqual(vp.context_load_dts.name, 'example2_load_dts')
    self.assertEqual(vp.load_dts.name, 'load_dts')

  def test_link_fk_lookup(self):
    hub1 = self.create_example_hub("1")
    hub2 = self.create_example_hub("2")
    link = self.create_example_link()
    _ = Schema('dv', [hub1, hub2, link])
    [fk1, fk2] = link.fks
    self.assertEqual(fk1.foreign_table, hub1)
    self.assertEqual(fk1.foreign_columns, [hub1.key])
    self.assertEqual(fk2.foreign_table, hub2)
    self.assertEqual(fk2.foreign_columns, [hub2.key])

  def test_satellite_fk_lookup(self):
    hub = self.create_example_hub()
    satellite = self.create_example_satellite()
    _ = Schema('dv', [hub, satellite])
    [fk] = satellite.fks
    self.assertEqual(fk.foreign_table, hub)
    self.assertEqual(fk.foreign_columns, [hub.key])

  def test_link_satellite_fk_lookup(self):
    link = self.create_example_link()
    link_satellite = self.create_example_link_satellite()
    _ = Schema('dv', [link, link_satellite])
    [fk] = link_satellite.fks
    self.assertEqual(fk.foreign_table, link)
    self.assertEqual(fk.foreign_columns, [link.root_key])

  def test_hub_references(self):
    hub1 = self.create_example_hub("1")
    hub2 = self.create_example_hub("2")
    satellite1 = self.create_example_satellite("1")
    link = self.create_example_link()
    _ = Schema('dv', [hub1, hub2, satellite1, link])
    self.assertEqual(hub1.referred_tables(), [])
    result = hub1.referring_tables()
    result.sort(key=lambda t: t.name)
    self.assertEqual(result, [satellite1, link])
    self.assertEqual(hub1.related_links, [link])
    self.assertEqual(hub1.related_satellites, [satellite1])

  def test_link_references(self):
    hub1 = self.create_example_hub("1")
    hub2 = self.create_example_hub("2")
    link = self.create_example_link()
    link_satellite = self.create_example_link_satellite()
    _ = Schema('dv', [hub1, hub2, link, link_satellite])
    self.assertEqual(link.referred_tables(), [hub1, hub2])
    self.assertEqual(link.referring_tables(), [link_satellite])
    self.assertEqual(link.related_hubs, [hub1, hub2])
    self.assertEqual(link.related_link_satellites, [link_satellite])

  def test_satellite_references(self):
    hub = self.create_example_hub()
    satellite = self.create_example_satellite()
    _ = Schema('dv', [hub, satellite])
    self.assertEqual(satellite.referred_tables(), [hub])
    self.assertEqual(satellite.referring_tables(), [])
    self.assertEqual(satellite.related_hub, [hub])

  def test_link_satellite_references(self):
    hub1 = self.create_example_hub("1")
    hub2 = self.create_example_hub("2")
    link = self.create_example_link()
    link_satellite = self.create_example_link_satellite()
    _ = Schema('dv', [hub1, hub2, link, link_satellite])
    self.assertEqual(link_satellite.referred_tables(), [link])
    self.assertEqual(link_satellite.referring_tables(), [])
    self.assertEqual(link_satellite.related_link, [link])

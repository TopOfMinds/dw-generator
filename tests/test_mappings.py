import unittest
from collections import namedtuple

from dwgenerator.dbobjects import Schema, Table, Column, create_typed_table, Hub, Link, Satellite, MetaDataError, MetaDataWarning
from dwgenerator.mappings import Mappings, TableMappings, ColumnMappings

TableMapping = namedtuple('TableMapping',
  'source_schema source_table source_filter target_schema target_table')
ColumnMapping = namedtuple('ColumnMapping',
  'src_schema src_table src_column transformation tgt_schema tgt_table tgt_column')

class TestMappings(unittest.TestCase):

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

  def create_example_link(self, table1_number, table2_number, **properties):
    link = create_typed_table(
      Table('dv', f'example_{table1_number}_{table2_number}_l', [
        Column(f'example_{table1_number}_{table2_number}_l_key', 'text'),
        Column(f'example{table1_number}_key', 'text'),
        Column(f'example{table2_number}_key', 'text'),
        Column('load_dts', 'numeric'),
        Column('rec_src', 'text'),
      ], **properties)
    )
    link.check()
    return link

  def create_example_satellite(self, table_number="", variation="", **properties):
    satellite = create_typed_table(
      Table('dv', f'example{table_number}{variation}_s', [
        Column(f'example{table_number}_key', 'text'),
        Column('load_dts', 'numeric'),
        Column('attribute1', 'text'),
        Column('attribute2', 'numeric'),
        Column('rec_src', 'text'),
      ], **properties)
    )
    satellite.check()
    return satellite

  def create_example_link_satellite(self, table1_number, table2_number, **properties):
    link_satellite = create_typed_table(
      Table('dv', f'example_{table1_number}_{table2_number}_l_s', [
        Column(f'example_{table1_number}_{table2_number}_l_key', 'text'),
        Column('load_dts', 'numeric'),
        Column('effective_ts', 'numeric'),
        Column('rec_src', 'text'),
      ], **properties)
    )
    link_satellite.check()
    return link_satellite

  def create_example_version_pointer(self, table1_number, table2_number, **properties):
    version_pointer = create_typed_table(
      Table('dv', f'example_{table1_number}_{table2_number}_vp', [
        Column(f'example{table1_number}_m_key', 'text'),
        Column(f'example{table2_number}_c_key', 'text'),
        Column(f'example{table2_number}_c_load_dts', 'numeric'),
        Column('load_dts', 'numeric'),
      ], **properties)
    )
    version_pointer.check()
    return version_pointer

  def test_mappings_vp_0(self):
    sat1 = self.create_example_satellite("1")
    hub1 = self.create_example_hub("1")
    sat2 = self.create_example_satellite("1", "1")
    vp_1_1 = self.create_example_version_pointer("1", "1")
    _ = Schema('dv', [sat1, hub1, sat2, vp_1_1])
    # print(schema.referred_tables)
    table_mappings = TableMappings([t._asdict() for t in [
      TableMapping("dv", "example1_s", "", "dv", "example_1_1_vp"),
      TableMapping("dv", "example11_s", "", "dv", "example_1_1_vp")
    ]])
    column_mappings = ColumnMappings([c._asdict() for c in [
      ColumnMapping("dv", "example1_s", "example1_key", "", "dv", "example_1_1_vp", "example1_m_key"),
      ColumnMapping("dv", "example11_s", "example1_key", "", "dv", "example_1_1_vp", "example1_c_key"),
      ColumnMapping("dv", "example11_s", "load_dts", "", "dv", "example_1_1_vp", "example1_c_load_dts"),
      ColumnMapping("dv", "example1_s", "load_dts", "", "dv", "example_1_1_vp", "load_dts"),
    ]])
    mappings = Mappings(table_mappings, column_mappings, [vp_1_1] + column_mappings.source_tables())
    mappings.check(vp_1_1)
    path = mappings.path(vp_1_1)
    self.assertEqual(
      [c.full_name for c in path],
      ['dv.example1_s.example1_key', 'dv.example1_h.example1_key', 'dv.example11_s.example1_key']
    )

  def test_mappings_vp_1(self):
    sat1 = self.create_example_satellite("1")
    hub1 = self.create_example_hub("1")
    link_1_2 = self.create_example_link("1", "2")
    hub2 = self.create_example_hub("2")
    sat2 = self.create_example_satellite("2")
    vp_1_2 = self.create_example_version_pointer("1", "2")
    schema = Schema('dv', [sat1, hub1, link_1_2, hub2, sat2, vp_1_2])
    # print(schema.referred_tables)
    table_mappings = TableMappings([t._asdict() for t in [
      TableMapping("dv", "example1_s", "", "dv", "example_1_2_vp"),
      TableMapping("dv", "example1_h", "", "dv", "example_1_2_vp"),
      TableMapping("dv", "example_1_2_l", "", "dv", "example_1_2_vp"),
      TableMapping("dv", "example2_h", "", "dv", "example_1_2_vp"),
      TableMapping("dv", "example2_s", "", "dv", "example_1_2_vp")
    ]])
    column_mappings = ColumnMappings([c._asdict() for c in [
      ColumnMapping("dv", "example1_s", "example1_key", "", "dv", "example_1_2_vp", "example1_m_key"),
      ColumnMapping("dv", "example2_s", "example2_key", "", "dv", "example_1_2_vp", "example2_c_key"),
      ColumnMapping("dv", "example2_s", "load_dts", "", "dv", "example_1_2_vp", "example2_c_load_dts"),
      ColumnMapping("dv", "example1_s", "load_dts", "", "dv", "example_1_2_vp", "load_dts"),
    ]])
    mappings = Mappings(table_mappings, column_mappings, list(schema.tables.values()) + column_mappings.source_tables())
    mappings.check(vp_1_2)
    path = mappings.path(vp_1_2)
    self.assertEqual(
      [c.full_name for c in path],
      [
        'dv.example1_s.example1_key', 'dv.example1_h.example1_key', 'dv.example_1_2_l.example1_key',
        'dv.example_1_2_l.example2_key', 'dv.example2_h.example2_key', 'dv.example2_s.example2_key'
      ]
    )

  def test_mappings_vp_2(self):
    sat1 = self.create_example_satellite("1")
    hub1 = self.create_example_hub("1")
    link_1_2 = self.create_example_link("1", "2")
    hub2 = self.create_example_hub("2")
    link_2_3 = self.create_example_link("2", "3")
    lsat_2_3 = self.create_example_link_satellite("2", "3")
    hub3 = self.create_example_hub("3")
    sat3 = self.create_example_satellite("3")
    vp_1_3 = self.create_example_version_pointer("1", "3")
    schema = Schema('dv', [sat1, hub1, link_1_2, hub2, link_2_3, lsat_2_3, hub3, sat3, vp_1_3])
    # print(schema.referred_tables)
    table_mappings = TableMappings([t._asdict() for t in [
      TableMapping("dv", "example1_s", "", "dv", "example_1_3_vp"),
      TableMapping("dv", "example1_h", "", "dv", "example_1_3_vp"),
      TableMapping("dv", "example_1_2_l", "", "dv", "example_1_3_vp"),
      TableMapping("dv", "example2_h", "", "dv", "example_1_3_vp"),
      TableMapping("dv", "example_2_3_l", "", "dv", "example_1_3_vp"),
      TableMapping("dv", "example3_h", "", "dv", "example_1_3_vp"),
      TableMapping("dv", "example3_s", "", "dv", "example_1_3_vp")
    ]])
    column_mappings = ColumnMappings([c._asdict() for c in [
      ColumnMapping("dv", "example1_s", "example1_key", "", "dv", "example_1_3_vp", "example1_m_key"),
      ColumnMapping("dv", "example3_s", "example3_key", "", "dv", "example_1_3_vp", "example3_c_key"),
      ColumnMapping("dv", "example3_s", "load_dts", "", "dv", "example_1_3_vp", "example3_c_load_dts"),
      ColumnMapping("dv", "example1_s", "load_dts", "", "dv", "example_1_3_vp", "load_dts"),
    ]])
    mappings = Mappings(table_mappings, column_mappings, list(schema.tables.values()) + column_mappings.source_tables())
    mappings.check(vp_1_3)
    path = mappings.path(vp_1_3)
    self.assertEqual(
      [c.full_name for c in path],
      [
        'dv.example1_s.example1_key', 'dv.example1_h.example1_key', 'dv.example_1_2_l.example1_key',
        'dv.example_1_2_l.example2_key', 'dv.example2_h.example2_key', 'dv.example_2_3_l.example2_key',
        'dv.example_2_3_l.example3_key', 'dv.example3_h.example3_key', 'dv.example3_s.example3_key'
      ]
    )


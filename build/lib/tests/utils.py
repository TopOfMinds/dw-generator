from collections import namedtuple

from dwgenerator.dbobjects import Schema, Table, Column, create_typed_table, Hub, Link, Satellite, MetaDataError, MetaDataWarning

TableMapping = namedtuple('TableMapping',
  'source_schema source_table source_filter target_schema target_table')
ColumnMapping = namedtuple('ColumnMapping',
  'src_schema src_table src_column transformation tgt_schema tgt_table tgt_column')

def create_example_hub(table_number="", **properties):
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

def create_example_link(table1_number, table2_number, **properties):
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

def create_example_satellite(table_number="", variation="", **properties):
  satellite = create_typed_table(
    Table('dv', f'example{table_number}{variation}_s', [
      Column(f'example{table_number}_key', 'text'),
      Column('load_dts', 'numeric'),
      Column('attribute1', 'text'),
      Column('attribute2', 'numeric'),
      Column('effective_ts', 'numeric'),
      Column('rec_src', 'text'),
    ], **properties)
  )
  satellite.check()
  return satellite

def create_example_link_satellite(table1_number, table2_number, **properties):
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

def create_example_version_pointer(table1_number, table2_number, **properties):
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


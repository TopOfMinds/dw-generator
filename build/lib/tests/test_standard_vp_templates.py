import sqlite3
import unittest
from collections import namedtuple
from datetime import datetime

from dwgenerator.dbobjects import Schema, Table, Column, create_typed_table, Hub, Link, Satellite, MetaDataError, MetaDataWarning
from dwgenerator.mappings import TableMappings, ColumnMappings, Mappings
from dwgenerator.templates import Templates
from .utils import TableMapping, ColumnMapping, create_example_hub, create_example_link, create_example_satellite, create_example_link_satellite, create_example_version_pointer


class TestStandardVPTemplates(unittest.TestCase):

  # Prepare the DB
  def setUp(self):
    self.dbtype = 'standard'
    self.start_ts = datetime.fromisoformat('2021-06-01T12:10:00+00:00').timestamp()
    self.templates = Templates(self.dbtype)
    self.connection = sqlite3.connect(':memory:')
    self.cur = self.connection.cursor()
    self.cur.execute("ATTACH DATABASE ':memory:' AS dv")

  def tearDown(self):
    self.connection.close()

  # Utils
  def render_view(self, target_table, mappings):
    [(_, sql), *rest] = self.templates.render(target_table, mappings)
    self.assertTrue(len(rest) == 0)
    return sql

  def executescript(self, sql, args):
    for (key, value) in args.items():
      sql = sql.replace(f':{key}', str(value))
    self.cur.executescript(sql)

  def create_table(self, target_table):
    ddl = self.templates.render_template("create_table.sql", target_table=target_table)
    # print(ddl)
    self.cur.executescript(ddl)

  def create_tables(self, *target_tables):
    for target_table in target_tables:
      self.create_table(target_table)

  # Test version pointers
  def test_vp_0(self):
    sat1 = create_example_satellite("1", effective_ts=True, generate_type='table')
    hub1 = create_example_hub("1", generate_type='table')
    sat2 = create_example_satellite("1", "1", effective_ts=True, generate_type='table')
    vp_1_1 = create_example_version_pointer("1", "1")
    schema = Schema('dv', [sat1, hub1, sat2, vp_1_1])
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
    mappings = Mappings(table_mappings, column_mappings, list(schema.tables.values()))
    mappings.check(vp_1_1)

    self.create_tables(sat1, hub1, sat2)
    ts = self.start_ts
    self.cur.executemany('INSERT INTO dv.example1_s VALUES(?, ?, ?, ?, ?, ?)', [
      ('1', ts+10, 'a', 'b', ts+6, ''),
    ])
    self.cur.executemany('INSERT INTO dv.example11_s VALUES(?, ?, ?, ?, ?, ?)', [
      ('1', ts+11, 'c', 'd', ts, ''),
      ('1', ts+12, 'e', 'f', ts+5, ''),
    ])

    sql = self.render_view(vp_1_1, mappings)
    self.cur.executescript(sql)
    result = self.cur.execute('SELECT * FROM dv.example_1_1_vp ORDER BY load_dts').fetchall()
    expected = [
      ('1', '1', ts+12, ts+10)
    ]
    self.assertEqual(result, expected)


  def test_vp_1(self):
    sat1 = create_example_satellite("1")
    hub1 = create_example_hub("1")
    link_1_2 = create_example_link("1", "2")
    hub2 = create_example_hub("2")
    sat2 = create_example_satellite("2")
    vp_1_2 = create_example_version_pointer("1", "2")
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
    mappings = Mappings(table_mappings, column_mappings, list(schema.tables.values()))
    mappings.check(vp_1_2)
    # path = mappings.path(vp_1_2)
    # print([c.full_name for c in path])

    self.create_tables(sat1, hub1, link_1_2, hub2, sat2)
    ts = self.start_ts
    self.cur.executemany('INSERT INTO dv.example1_s VALUES(?, ?, ?, ?, ?, ?)', [
      ('1', ts+10, 'a', 'b', ts+3, ''),
    ])
    self.cur.executemany('INSERT INTO dv.example_1_2_l VALUES(?, ?, ?, ?, ?)', [
      ('19', '1', '9', ts+10, ''),
    ])
    self.cur.executemany('INSERT INTO dv.example2_s VALUES(?, ?, ?, ?, ?, ?)', [
      ('9', ts+11, 'c', 'd', ts, ''),
      ('9', ts+12, 'e', 'f', ts+5, ''),
    ])

    sql = self.render_view(vp_1_2, mappings)
    self.cur.executescript(sql)
    result = self.cur.execute('SELECT * FROM dv.example_1_2_vp ORDER BY load_dts').fetchall()
    expected = [
      ('1', '9', ts+11, ts+10)
    ]
    self.assertEqual(result, expected)

  def test_vp_1_es(self):
    sat1 = create_example_satellite("1")
    hub1 = create_example_hub("1")
    link_1_2 = create_example_link("1", "2")
    lsat_1_2 = create_example_link_satellite("1", "2")
    hub2 = create_example_hub("2")
    sat2 = create_example_satellite("2")
    vp_1_2 = create_example_version_pointer("1", "2")
    schema = Schema('dv', [sat1, hub1, link_1_2, lsat_1_2, hub2, sat2, vp_1_2])
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
    mappings = Mappings(table_mappings, column_mappings, list(schema.tables.values()))
    mappings.check(vp_1_2)
    # path = mappings.path(vp_1_2)
    # print([c.full_name for c in path])

    self.create_tables(sat1, hub1, link_1_2, lsat_1_2, hub2, sat2)
    ts = self.start_ts
    self.cur.executemany('INSERT INTO dv.example1_s VALUES(?, ?, ?, ?, ?, ?)', [
      ('1', ts+10, 'a', 'b', ts+3, ''),
    ])
    self.cur.executemany('INSERT INTO dv.example_1_2_l VALUES(?, ?, ?, ?, ?)', [
      ('18', '1', '8', ts+8, ''),
      ('19', '1', '9', ts+10, ''),
    ])
    self.cur.executemany('INSERT INTO dv.example_1_2_l_s VALUES(?, ?, ?, ?)', [
      ('18', ts+8, ts, ''),
      ('19', ts+10, ts, ''),
      ('19', ts+15, ts+5, ''),
    ])
    self.cur.executemany('INSERT INTO dv.example2_s VALUES(?, ?, ?, ?, ?, ?)', [
      ('9', ts+11, 'c', 'd', ts, ''),
      ('9', ts+12, 'e', 'f', ts+5, ''),
    ])

    sql = self.render_view(vp_1_2, mappings)
    self.cur.executescript(sql)
    result = self.cur.execute('SELECT * FROM dv.example_1_2_vp ORDER BY load_dts').fetchall()
    expected = [
      ('1', '9', ts+11, ts+10)
    ]
    self.assertEqual(result, expected)


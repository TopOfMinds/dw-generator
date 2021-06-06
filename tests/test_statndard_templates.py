import sqlite3
import unittest
from collections import namedtuple
from datetime import datetime

from dwgenerator.dbobjects import Schema, Table, Column, create_typed_table, Hub, Link, Satellite, MetaDataError, MetaDataWarning
from dwgenerator.mappings import TableMappings, ColumnMappings, Mappings
from dwgenerator.templates import render

TableMapping = namedtuple('TableMapping',
  'source_schema source_table source_filter target_schema target_table')
ColumnMapping = namedtuple('ColumnMapping',
  'src_schema src_table src_column transformation tgt_schema tgt_table tgt_column')

class TestIsoViews(unittest.TestCase):

  def setUp(self):
    self.dbtype = 'standard'
    self.connection = sqlite3.connect(':memory:')
    self.cur = self.connection.cursor()
    self.cur.execute("ATTACH DATABASE ':memory:' AS db")

  def tearDown(self):
    self.connection.close()

  def create_customers(self):
    self.cur.execute('CREATE TABLE db.customers (ssn, name, load_dts)')
    ts = datetime.fromisoformat('2021-06-01T12:10:00+00:00').timestamp()
    self.cur.executemany('INSERT INTO db.customers VALUES(?, ?, ?)', [
      ('198001010101', 'Michael', ts),
      ('199001010101', 'Jessica', ts + 1),
      ('199201010101', 'Ashley', ts + 2),
    ])

  def create_sales_lines(self):
    self.cur.execute('CREATE TABLE db.sales_lines (txn_id, ssn, load_dts)')
    ts = datetime.fromisoformat('2021-06-01T12:10:00+00:00').timestamp()
    self.cur.executemany('INSERT INTO db.sales_lines VALUES(?, ?, ?)', [
      ('1234', '198001010101', ts + 20),
      ('2345', '199001010101', ts + 21),
    ])

  def test_hub(self):
    target_table = create_typed_table(
      Table('db', 'customer_h', [
        Column('customer_key', 'text'),
        Column('ssn', 'text'),
        Column('load_dts', 'numeric'),
        Column('rec_src', 'text'),
    ]))
    # I use the same source and target database as sqlite cannot create views that use other dbs
    table_mappings = TableMappings([t._asdict() for t in [
      TableMapping("db", "customers", "", "db", "customer_h"),
      TableMapping("db", "sales_lines", "", "db", "customer_h")
    ]])
    column_mappings = ColumnMappings([c._asdict() for c in [
      ColumnMapping("db", "customers", "ssn", "", "db", "customer_h", "customer_key"),
      ColumnMapping("db", "customers", "ssn", "", "db", "customer_h", "ssn"),
      ColumnMapping("db", "customers", "load_dts", "", "db", "customer_h", "load_dts"),
      ColumnMapping("db", "customers", "", "'db'", "db", "customer_h", "rec_src"),
      ColumnMapping("db", "sales_lines", "ssn", "", "db", "customer_h", "customer_key"),
      ColumnMapping("db", "sales_lines", "ssn", "", "db", "customer_h", "ssn"),
      ColumnMapping("db", "sales_lines", "load_dts", "", "db", "customer_h", "load_dts"),
      ColumnMapping("db", "sales_lines", "", "'db'", "db", "customer_h", "rec_src"),
    ]])
    mappings = Mappings(table_mappings, column_mappings, [target_table] + column_mappings.source_tables())
    mappings.check(target_table)

    sql = render(target_table, mappings, self.dbtype, 'view')

    self.create_customers()
    self.create_sales_lines()
    self.cur.executescript(sql)
    result = list(self.cur.execute('SELECT * FROM db.customer_h ORDER BY load_dts'))

    expected = [
      ('198001010101', '198001010101', 1622549400.0, 'db'),
      ('199001010101', '199001010101', 1622549401.0, 'db'),
      ('199201010101', '199201010101', 1622549402.0, 'db')
    ]

    self.assertEqual(result, expected)

  def test_link(self):
    target_table = create_typed_table(
      Table('db', 'sales_line_customer_l', [
        Column('sales_line_customer_l_key', 'text'),
        Column('sales_line_key', 'text'),
        Column('customer_key', 'text'),
        Column('load_dts', 'numeric'),
        Column('rec_src', 'text'),
    ]))
    # I use the same source and target database as sqlite cannot create views that use other dbs
    table_mappings = TableMappings([
      TableMapping("db", "sales_lines", "", "db", "sales_line_customer_l")._asdict()
    ])
    column_mappings = ColumnMappings([c._asdict() for c in [
      ColumnMapping("db", "sales_lines", "txn_id,ssn", "", "db", "sales_line_customer_l", "sales_line_customer_l_key"),
      ColumnMapping("db", "sales_lines", "txn_id", "", "db", "sales_line_customer_l", "sales_line_key"),
      ColumnMapping("db", "sales_lines", "ssn", "", "db", "sales_line_customer_l", "customer_key"),
      ColumnMapping("db", "sales_lines", "load_dts", "", "db", "sales_line_customer_l", "load_dts"),
      ColumnMapping("db", "sales_lines", "", "'db'", "db", "sales_line_customer_l", "rec_src"),
    ]])
    mappings = Mappings(table_mappings, column_mappings, [target_table] + column_mappings.source_tables())
    mappings.check(target_table)

    sql = render(target_table, mappings, self.dbtype, 'view')

    self.create_sales_lines()
    self.cur.executescript(sql)
    result = list(self.cur.execute('SELECT * FROM db.sales_line_customer_l ORDER BY load_dts'))

    expected = [
      ('1234|198001010101', '1234', '198001010101', 1622549420.0, 'db'),
      ('2345|199001010101', '2345', '199001010101', 1622549421.0, 'db')
    ]

    self.assertEqual(result, expected)

  def test_satellite(self):
    target_table = create_typed_table(
      Table('db', 'customer_s', [
        Column('customer_key', 'text'),
        Column('load_dts', 'numeric'),
        Column('ssn', 'text'),
        Column('name', 'text'),
        Column('rec_src', 'text'),
    ]))
    # I use the same source and target database as sqlite cannot create views that use other dbs
    table_mappings = TableMappings([
      TableMapping("db", "customers", "", "db", "customer_s")._asdict()
    ])
    column_mappings = ColumnMappings([c._asdict() for c in [
      ColumnMapping("db", "customers", "ssn", "", "db", "customer_s", "customer_key"),
      ColumnMapping("db", "customers", "load_dts", "", "db", "customer_s", "load_dts"),
      ColumnMapping("db", "customers", "ssn", "", "db", "customer_s", "ssn"),
      ColumnMapping("db", "customers", "name", "", "db", "customer_s", "name"),
      ColumnMapping("db", "customers", "", "'db'", "db", "customer_s", "rec_src"),
    ]])
    mappings = Mappings(table_mappings, column_mappings, [target_table] + column_mappings.source_tables())
    mappings.check(target_table)

    sql = render(target_table, mappings, self.dbtype, 'view')

    self.create_customers()
    self.cur.executescript(sql)
    result = list(self.cur.execute('SELECT * FROM db.customer_s ORDER BY load_dts'))

    expected = [
      ('198001010101', 1622549400.0, '198001010101', 'Michael', 'db'),
      ('199001010101', 1622549401.0, '199001010101', 'Jessica', 'db'),
      ('199201010101', 1622549402.0, '199201010101', 'Ashley', 'db'),
    ]

    self.assertEqual(result, expected)


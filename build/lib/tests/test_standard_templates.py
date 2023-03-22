import sqlite3
import unittest
from collections import namedtuple
from datetime import datetime

from dwgenerator.dbobjects import Schema, Table, Column, create_typed_table, Hub, Link, Satellite, MetaDataError, MetaDataWarning
from dwgenerator.mappings import TableMappings, ColumnMappings, Mappings
from dwgenerator.templates import Templates

TableMapping = namedtuple('TableMapping',
  'source_schema source_table source_filter target_schema target_table')
ColumnMapping = namedtuple('ColumnMapping',
  'src_schema src_table src_column transformation tgt_schema tgt_table tgt_column')

class TestStandardTemplates(unittest.TestCase):

  # Prepare the DB
  def setUp(self):
    self.dbtype = 'standard'
    self.start_ts = datetime.fromisoformat('2021-06-01T12:10:00+00:00').timestamp()
    self.templates = Templates(self.dbtype)
    self.connection = sqlite3.connect(':memory:')
    self.cur = self.connection.cursor()
    self.cur.execute("ATTACH DATABASE ':memory:' AS db")

  def tearDown(self):
    self.connection.close()

  # Create table definitions
  def create_customer_h(self, **properties):
    target_table = create_typed_table(
      Table('db', 'customer_h', [
        Column('customer_key', 'text'),
        Column('ssn', 'text'),
        Column('load_dts', 'numeric'),
        Column('rec_src', 'text'),
      ], **properties))
    target_table.check()
    return target_table

  def create_sales_line_customer_l(self, **properties):
    target_table = create_typed_table(
      Table('db', 'sales_line_customer_l', [
        Column('sales_line_customer_l_key', 'text'),
        Column('sales_line_key', 'text'),
        Column('customer_key', 'text'),
        Column('load_dts', 'numeric'),
        Column('rec_src', 'text'),
    ], **properties))
    target_table.check()
    return target_table

  def create_customer_s(self, **properties):
    target_table = create_typed_table(
      Table('db', 'customer_s', [
        Column('customer_key', 'text'),
        Column('load_dts', 'numeric'),
        Column('ssn', 'text'),
        Column('name', 'text'),
        Column('rec_src', 'text'),
    ], **properties))
    target_table.check()
    return target_table

  # Create mappings
  def create_customer_h_mappings(self, target_table):
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
    return mappings
  
  def create_sales_line_customer_l_mappings(self, target_table):
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
    return mappings

  def create_customer_s_mappings(self, target_table):
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
    return mappings

  # Create and put test data in source tables
  def create_customers(self):
    self.cur.execute('CREATE TABLE db.customers (ssn, name, load_dts)')
    ts = self.start_ts
    self.cur.executemany('INSERT INTO db.customers VALUES(?, ?, ?)', [
      ('198001010101', 'Michael', ts),
      ('199001010101', 'Jessica', ts + 1),
      ('199201010101', 'Ashley', ts + 2),
    ])

  def create_sales_lines(self):
    self.cur.execute('CREATE TABLE db.sales_lines (txn_id, ssn, load_dts)')
    ts = self.start_ts
    self.cur.executemany('INSERT INTO db.sales_lines VALUES(?, ?, ?)', [
      ('1234', '198001010101', ts + 20),
      ('2345', '199001010101', ts + 21),
      ('2345', '199001010101', ts + 3600),
      ('3456', '199201010101', ts + 3601),
    ])

  # Utils
  def render_view(self, target_table, mappings):
    [(_, sql), *rest] = self.templates.render(target_table, mappings)
    self.assertTrue(len(rest) == 0)
    return sql

  def executescript(self, sql, args):
    for (key, value) in args.items():
      sql = sql.replace(f':{key}', str(value))
    self.cur.executescript(sql)

  # Test the data vault objects
  ## Test views
  def test_hub_view(self):
    target_table = self.create_customer_h()
    mappings = self.create_customer_h_mappings(target_table)
    sql = self.render_view(target_table, mappings)

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

  def test_link_view(self):
    target_table = self.create_sales_line_customer_l()
    mappings = self.create_sales_line_customer_l_mappings(target_table)
    sql = self.render_view(target_table, mappings)

    self.create_sales_lines()
    self.cur.executescript(sql)

    result = list(self.cur.execute('SELECT * FROM db.sales_line_customer_l ORDER BY load_dts'))
    expected = [
      ('1234|198001010101', '1234', '198001010101', 1622549420.0, 'db'),
      ('2345|199001010101', '2345', '199001010101', 1622549421.0, 'db'),
      ('3456|199201010101', '3456', '199201010101', 1622553001.0, 'db'),
    ]
    self.assertEqual(result, expected)

  def test_satellite_view(self):
    target_table = self.create_customer_s()
    mappings = self.create_customer_s_mappings(target_table)
    sql = self.render_view(target_table, mappings)

    self.create_customers()
    self.cur.executescript(sql)

    result = list(self.cur.execute('SELECT * FROM db.customer_s ORDER BY load_dts'))
    expected = [
      ('198001010101', 1622549400.0, '198001010101', 'Michael', 'db'),
      ('199001010101', 1622549401.0, '199001010101', 'Jessica', 'db'),
      ('199201010101', 1622549402.0, '199201010101', 'Ashley', 'db'),
    ]
    self.assertEqual(result, expected)

  ## Test persited tables
  def test_hub_persisted(self):
    target_table = self.create_customer_h(generate_type='table')
    mappings = self.create_customer_h_mappings(target_table)
    [(ddl_path, ddl), (etl_path, etl)] = self.templates.render(target_table, mappings)

    self.assertEqual(ddl_path.as_posix(), 'db/customer_h_t.sql')
    self.assertEqual(etl_path.as_posix(), 'db/customer_h_etl.sql')

    self.cur.executescript(ddl)
    result = list(self.cur.execute("PRAGMA db.table_info('customer_h')"))
    expected = [
      (0, 'customer_key', 'text', 0, None, 1),
      (1, 'ssn', 'text', 0, None, 0),
      (2, 'load_dts', 'numeric', 0, None, 0),
      (3, 'rec_src', 'text', 0, None, 0)
    ]
    self.assertEqual(result, expected)

    self.create_customers()
    self.create_sales_lines()

    ts = self.start_ts
    # print(etl)
    self.executescript(etl, {'start_ts': ts, 'end_ts': ts + 2})
    result1 = list(self.cur.execute('SELECT * FROM db.customer_h ORDER BY load_dts'))
    expected1 = [
      ('198001010101', '198001010101', 1622549400.0, 'db'),
      ('199001010101', '199001010101', 1622549401.0, 'db'),
    ]
    self.assertEqual(result1, expected1)

    self.executescript(etl, {'start_ts': ts + 2, 'end_ts': ts + 4000})
    result2 = list(self.cur.execute('SELECT * FROM db.customer_h ORDER BY load_dts'))
    expected2 = expected1 + [
      ('199201010101', '199201010101', 1622549402.0, 'db')
    ]
    self.assertEqual(result2, expected2)

  def test_link_persisted(self):
    target_table = self.create_sales_line_customer_l(generate_type='table')
    mappings = self.create_sales_line_customer_l_mappings(target_table)
    [(ddl_path, ddl), (etl_path, etl)] = self.templates.render(target_table, mappings)

    self.assertEqual(ddl_path.as_posix(), 'db/sales_line_customer_l_t.sql')
    self.assertEqual(etl_path.as_posix(), 'db/sales_line_customer_l_etl.sql')

    self.cur.executescript(ddl)
    result = list(self.cur.execute("PRAGMA db.table_info('sales_line_customer_l')"))
    expected = [
      (0, 'sales_line_customer_l_key', 'text', 0, None, 1),
      (1, 'sales_line_key', 'text', 0, None, 0),
      (2, 'customer_key', 'text', 0, None, 0),
      (3, 'load_dts', 'numeric', 0, None, 0),
      (4, 'rec_src', 'text', 0, None, 0)
    ]
    self.assertEqual(result, expected)

    self.create_sales_lines()

    ts = self.start_ts
    # print(etl)
    self.executescript(etl, {'start_ts': ts + 0, 'end_ts': ts + 3600})
    result1 = list(self.cur.execute('SELECT * FROM db.sales_line_customer_l ORDER BY load_dts'))
    expected1 = [
      ('1234|198001010101', '1234', '198001010101', 1622549420.0, 'db'),
      ('2345|199001010101', '2345', '199001010101', 1622549421.0, 'db'),
    ]
    self.assertEqual(result1, expected1)

    self.executescript(etl, {'start_ts': ts + 3600, 'end_ts': ts + 7200})
    result2 = list(self.cur.execute('SELECT * FROM db.sales_line_customer_l ORDER BY load_dts'))
    expected2 = expected1 + [
      ('3456|199201010101', '3456', '199201010101', 1622553001.0, 'db'),
    ]
    self.assertEqual(result2, expected2)

  def test_satellite_persisted(self):
    target_table = self.create_customer_s(generate_type='table')
    mappings = self.create_customer_s_mappings(target_table)
    [(ddl_path, ddl), (etl_path, etl)] = self.templates.render(target_table, mappings)

    self.assertEqual(ddl_path.as_posix(), 'db/customer_s_t.sql')
    self.assertEqual(etl_path.as_posix(), 'db/customer_s_etl.sql')

    self.cur.executescript(ddl)
    result = list(self.cur.execute("PRAGMA db.table_info('customer_s')"))
    expected = [
      (0, 'customer_key', 'text', 0, None, 1),
      (1, 'load_dts', 'numeric', 0, None, 2),
      (2, 'ssn', 'text', 0, None, 0),
      (3, 'name', 'text', 0, None, 0),
      (4, 'rec_src', 'text', 0, None, 0)
    ]
    self.assertEqual(result, expected)

    self.create_customers()

    ts = self.start_ts
    # print(etl)
    self.executescript(etl, {'start_ts': ts, 'end_ts': ts + 2})
    result1 = list(self.cur.execute('SELECT * FROM db.customer_s ORDER BY load_dts'))
    expected1 = [
      ('198001010101', 1622549400.0, '198001010101', 'Michael', 'db'),
      ('199001010101', 1622549401.0, '199001010101', 'Jessica', 'db'),
    ]
    self.assertEqual(result1, expected1)

    self.executescript(etl, {'start_ts': ts + 2, 'end_ts': ts + 4})
    result2 = list(self.cur.execute('SELECT * FROM db.customer_s ORDER BY load_dts'))
    expected2 = expected1 + [
      ('199201010101', 1622549402.0, '199201010101', 'Ashley', 'db'),
    ]
    self.assertEqual(result2, expected2)

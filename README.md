# Create data warehouse DDLs and queries based on patterns

## Install

- Install [Python 3](https://www.python.org/downloads/) if you don't have it. Other Python 3 distributions might also work.
- Install the dwgenerator `$ python -m pip install .`
- If you are developing the generator it might be more convenient to install the package in develop mode: `$ python -m pip install -e .`

### Run the CLI

`$ dwgenerator --help`

### Example: generate views

`$ dwgenerator generate-view --dbtype snowflake --out sql/dw/`

## Overview

The `dwgenerator` generates SQL code based on metadata. The metadata describes the schemas on the target tables and optionally the source tables and the mapping between source tables/columns and target table/column. Metadata is also needed list all tables that should be generated.

### Suggested target project structure

The generator assumes the following directory structure on a project where you
generate code (even if some locations are configurable):

```
<project name>/
    metadata/
        target_tables.csv
        table_def/
            <schema name 1>/
                <table name 1>.csv
                ...
                <table name m>.csv
            ...
            <schema name n>/
        mapping/
            table/
                <table mappings 1>.csv
                ...
            column/
                <column mappings 1>.csv
                ...
    sql/
        <db name>/
            <schema name 1>/
                <table ddl name 1>.sql
                ...
                <table ddl name m>.sql
            ...
            <schema name n>/
```

The `metadata` directory contains the needed metadata and `sql` the generated code.

## Metadata files

`target_tables.csv` lists all target tables. It states if code should be generated or not and if the generated code should be a view DDL (`view`) or a combination of a table creation DDL and a load script (`table`).

Example:

| schema | table      | generate | table_type |
| ------ | -----      | -------- |----------- |
| dv     | customer_h | true     | table      |

### Table defs

Generation of `create table` DDLs, requires knowledge of table name and column names with data types. To achieve this there should be one table definition CSV file for each target table. The file name should be on the format: `metadata/table_def/<schema_name>/<table_name>.csv`. The content in the table definition CSV is based on the output from `describe table` (in Snowflake). Currently the generator only uses the content in name and type fields, so the rest of the fields can be empty. Meta data properties kan be added by having names that start with '#'. '#generate_type' is used to decide if a table or a view should be generated, view is default.

Example:

| name           | type         |
|----------------|--------------|
| id             | NUMBER(38,0) |
| fname          | VARCHAR(50)  |
| #generate_type | table        |

### Mapping files

To generate a view or a load script the input tables together with their respective `where` constraints (if applicable) is needed for each target table. This is specified in table mapping files.

Also the input column, possibly with a transformation, is needed for each target column. This is specified in column mapping files.

### Table mappings

Table mapping CSV files should be put into the `metadata/mapping/table` directory. The generator will just read and merge all mapping files in that directory. So the mappings could be put in one or several files in any way that is convenient.

For a small project it could be enough with one global table mapping file. For a larger project there could e.g. be one table mapping file per target schema or ensemble. Or maybe one table per input source. The mapping file needs specification of source schema and table, optional filter and target schema and table.

Example:

| source_schema | source_table | source_filter | target_schema | target_table |
| ------------- | ------------ | ------------- | ------------- | ------------ |
| src_cm        | person_detail| id != 0       | dv            | customer_h   |
| src_cm        | person_detail| id != 0       | dv            | customer_s   |

### Column mappings

Column mapping CSV files should be put into the `metadata/mapping/column` directory. The generator will just read and merge all mapping files in that directory. So the mappings could be put in one or several files in any way that is convenient. Se the discussion in the table mapping section for ideas about how to organize the mappings.

In the column mapping you need to specify; source schema, table and column, an optional transformation and target schema, table and column.

Example:

| src_schema | src_table | src_column | transformation | tgt_schema | tgt_table | tgt_column |
| ---------- | --------- | ---------- | -------------- | ---------- | --------- | ---------- |
| src_cm | person_detail | id | | dv | customer_h | customer_key |
| src_cm | person_detail | id | | dv | customer_h | customer_id |
| src_cm | person_detail | now() | | dv | customer_h | load_dts |
| src_cm | person_detail | | 'cm' | dv | customer_h | rec_src |

## Development

### Testing

[Sqlite3](https://docs.python.org/3/library/sqlite3.html), that is embedded in Python is used for template testing. As window functions was implemented recently in `Sqlite3` only the latest versions of Python supports it. Python 3.9.5 has a new enough `Sqlite3` version, earlier versions of Python might work as well, but I have not tested.

Run all tests:
```
$ python -m unittest -v
```

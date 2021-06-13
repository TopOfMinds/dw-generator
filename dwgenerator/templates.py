from pathlib import Path
from re import template

from jinja2 import Environment
from jinja2.loaders import PackageLoader

from .dbobjects import MetaDataError

class Templates:
  def __init__(self, dbtype):
    self.env = Environment(
      loader=PackageLoader('dwgenerator', f'sql/{dbtype}'),
      trim_blocks=True,
      lstrip_blocks=True,
    )

  def render(self, target_table, mappings):
    table_type = target_table.table_type
    generate_type = target_table.properties['generate_type']
    if generate_type == 'view':
      template_paths = [f"{table_type}_view.sql"]
      suffixes = ['v']
    elif generate_type == 'table':
      template_paths = ["create_table.sql", f"{table_type}_etl.sql"]
      suffixes = ['t', 'etl']
    else:
      raise MetaDataError(f"Unknown generate_type={generate_type} for {target_table.name}.")

    templates = [
      self.env.get_template(template_path)
      for template_path in template_paths
    ]
    sqls = [
      template.render(target_table=target_table, mappings=mappings)
      for template in templates
    ]
    out_paths = [
      Path(target_table.schema) / (target_table.name + f'_{suffix}.sql')
      for suffix in suffixes
    ]
    return zip(out_paths, sqls)

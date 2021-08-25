from pathlib import Path
from re import template

from jinja2 import Environment
from jinja2.loaders import ChoiceLoader, PackageLoader

from .dbobjects import MetaDataError

class Templates:
  def __init__(self, dbtype):
    loaders = [
      PackageLoader('dwgenerator', f'sql/{dbtype}')
    ]
    # Use standard SQL as fallback if not the specific template is implemented.
    if dbtype != 'standard':
      loaders.append(PackageLoader('dwgenerator', 'sql/standard'))
    self.env = Environment(
      loader=ChoiceLoader(loaders),
      trim_blocks=True,
      lstrip_blocks=True,
    )

  def render_template(self, template_path, **objects):
    template = self.env.get_template(template_path)
    return template.render(**objects)

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

    sqls = [
      self.render_template(template_path, target_table=target_table, mappings=mappings)
      for template_path in template_paths
    ]
    out_paths = [
      Path(target_table.schema) / (target_table.name + f'_{suffix}.sql')
      for suffix in suffixes
    ]
    return zip(out_paths, sqls)

from pathlib import Path

from jinja2 import Environment
from jinja2.loaders import FileSystemLoader

from .dbobjects import Hub, Link, Satellite

root_location = Path(__file__).parent.resolve()
env = Environment(
    loader=FileSystemLoader(str(root_location / 'sql')),
    trim_blocks=True,
    lstrip_blocks=True,
)

def render(target_table, mappings, dbtype, target_type):
    table_type = target_table.table_type
    template_path = f"{table_type}_{target_type}.sql"
    template = env.get_template(dbtype + '/' + template_path)
    sql = template.render(target_table=target_table, mappings=mappings)
    return sql

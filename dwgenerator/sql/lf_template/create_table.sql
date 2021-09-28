{% include 'header.sql' %}

CREATE TABLE {{ target_table.full_name }} (
  {% for column in target_table.columns %}
  {{ "%-20s" | format(column.name) }} {{ column.type }}{{"," if not loop.last or target_table.pk }}
  {% endfor %}
  {% if target_table.pk %}
  CONSTRAINT pk_{{ target_table.name }} PRIMARY KEY ({% for pk in target_table.pk %}{{ pk.name }}{{", " if not loop.last }}{% endfor %})
  {% endif %}
) 
{% if target_table.properties.compression == 'true' -%}
ROW STORE COMPRESS ADVANCED
{% endif %}
;
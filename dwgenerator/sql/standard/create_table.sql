{% include 'header.sql' %}

DROP TABLE IF EXISTS {{ target_table.full_name }};

CREATE TABLE {{ target_table.full_name }} (
  {% if target_table.pk %}
  {% for column in target_table.columns %}
  {{ "%-20s" | format(column.name) }} {{ column.type }}{{","}}
  {% endfor %}
  CONSTRAINT pk_{{ target_table.name }} PRIMARY KEY ({% for pk in target_table.pk %}{{ pk.name }}{{", " if not loop.last }}{% endfor %})
  {% else %}
  {% for column in target_table.columns %}
  {{ "%-20s" | format(column.name) }} {{ column.type }}{{"," if not loop.last }}
  {% endfor %}
  {% endif %}
);
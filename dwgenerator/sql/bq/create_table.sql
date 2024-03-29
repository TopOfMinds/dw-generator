{% include 'header.sql' %}

DROP TABLE IF EXISTS {{ target_table.full_name }};

CREATE TABLE {{ target_table.full_name }} (
  {% for column in target_table.columns %}
  {{ "%-20s" | format(column.name) }} {{ column.type }}{{"," if not loop.last or target_table.pk }}
  {% endfor %}
);
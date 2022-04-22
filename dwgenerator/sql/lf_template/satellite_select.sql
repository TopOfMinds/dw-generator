{% from 'external_param.sql' import external_param %}
SELECT
  {{ target_table.key.name }}
  {% if no_deduplication_ %}
  ,{{ target_table.load_dts.name }}
  {% else %}
  ,max({{ target_table.load_dts.name }}) AS {{ target_table.load_dts.name }}
  {% endif %}
  {% for attribute in target_table.attributes %}
  ,{{ attribute.name }}
  {% endfor %}
  ,{{ target_table.rec_src.name }}
FROM (
  {%- set union_all = joiner("UNION ALL") %}
  {% for source_table in mappings.source_tables(target_table) %}
  {% set source_filter = mappings.filter(source_table, target_table) %}
  {{ union_all() }}
  SELECT
    {{ mappings.source_column(source_table, target_table.key) }} AS {{ target_table.key.name }}
    ,{{ mappings.source_column(source_table, target_table.load_dts) }} AS {{ target_table.load_dts.name }}
    {% for attribute in target_table.attributes %}
    ,{{ mappings.source_column(source_table, attribute) }} AS {{ attribute.name }}
    {% endfor %}
    ,{{ mappings.source_column(source_table, target_table.rec_src) }} AS {{ target_table.rec_src.name }}
  FROM
    {{ source_table.full_name }}
  {% if source_filter or insert_ %}
  WHERE
    {% set and_ = joiner("AND ") %}
    {% if source_filter %}
    {{ and_() }}{{ source_filter }}
    {% endif %}
    {% if insert_ %}
    {{ and_() }}{{ external_param('start_ts') }} <= {{ mappings.source_column(source_table, target_table.load_dts) }}
    AND {{ mappings.source_column(source_table, target_table.load_dts) }} < {{ external_param('end_ts') }}
    {%- endif %}
  {% endif %}
  {% endfor %}
)
{% if insert_ %}
q
WHERE
  NOT EXISTS (
    SELECT 1
    FROM {{ target_table.full_name }} t
    WHERE t.{{ target_table.key.name }} = q.{{ target_table.key.name }}
    {% if no_deduplication_ %}
    AND t.{{target_table.load_dts.name }} = q.{{target_table.load_dts.name }}
    {% else %}
      {% for attribute in target_table.attributes %}
      AND COALESCE(TO_CHAR(t.{{ attribute.name }}), '#') = COALESCE(TO_CHAR(q.{{ attribute.name }}), '#')
      {% endfor %}
    {% endif %}
  )
{% endif %}
{% if no_deduplication_ %}
{% else  -%}
GROUP BY
  {{ target_table.key.name }}
  {% for attribute in target_table.attributes %}
  ,{{ attribute.name }}
  {% endfor %}
  ,{{ target_table.rec_src.name }}
{% endif %}

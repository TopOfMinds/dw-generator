SELECT
  {{ target_table.key.name }}
  ,max({{ target_table.load_dts.name }}) AS {{ target_table.load_dts.name }}
  {% for attribute in target_table.attributes %}
  ,{{ attribute.name }}
  {% endfor %}
  ,{{ target_table.rec_src.name }}
FROM (
  {% set union_all = joiner("UNION ALL") %}
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
    {%+ if source_filter %}{{ and_() }}{{ source_filter }}{% endif +%}
    {% if insert_ %}
    {{ and_() }}:start_ts <= {{ mappings.source_column(source_table, target_table.load_dts) }}
    AND {{ mappings.source_column(source_table, target_table.load_dts) }} < :end_ts
    {% endif %}
  {% endif %}
  {% endfor %}
)
{% if insert_ %}
AS q
WHERE
  NOT EXISTS (
    SELECT 1
    FROM {{ target_table.full_name }} t
    WHERE t.{{ target_table.key.name }} = q.{{ target_table.key.name }}
      {% for attribute in target_table.attributes %}
      AND t.{{ attribute.name }} = q.{{ attribute.name }}
      {% endfor %}
  )
{% endif %}
GROUP BY
  {{ target_table.key.name }}
  {% for attribute in target_table.attributes %}
  ,{{ attribute.name }}
  {% endfor %}
  ,{{ target_table.rec_src.name }}

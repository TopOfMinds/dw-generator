{% from 'external_param.sql' import external_param %}
SELECT
  {{ target_table.root_key.name }}
  {% for key in target_table.keys %}
  ,{{ key.name }}
  {% endfor %}
  ,{{ target_table.load_dts.name }}
  ,{{ target_table.rec_src.name }}
FROM (
  SELECT
    {{ target_table.root_key.name }}
    {% for key in target_table.keys %}
    ,{{ key.name }}
    {% endfor %}
    ,{{ target_table.load_dts.name }}
    ,{{ target_table.rec_src.name }}
    ,row_number() over(PARTITION BY {{ target_table.root_key.name }} ORDER BY {{ target_table.load_dts.name }} asc) rn
  FROM (
    {% set union_all = joiner("UNION ALL") %}
    {% for source_table in mappings.source_tables(target_table) %}
    {% set source_filter = mappings.filter(source_table, target_table) %}
    {% set concat = joiner(" || '|' || ") %}
    {{ union_all() }}
    SELECT
      {%+ for key in target_table.keys %}{{ concat() }}{{ mappings.source_column(source_table, key) }}{% endfor %} AS {{ target_table.root_key.name }}
      {% for key in target_table.keys %}
      ,CAST({{ mappings.source_column(source_table, key) }} AS string) AS {{ key.name }}
      {% endfor %}
      ,{{ mappings.source_column(source_table, target_table.load_dts) }} AS {{ target_table.load_dts.name }}
      ,{{ mappings.source_column(source_table, target_table.rec_src) }} AS {{ target_table.rec_src.name }}
    FROM {{ source_table.full_name }}
    {% if source_filter or insert_ %}
    {% set and_ = joiner("AND ") %}
    WHERE {% if source_filter %}{{ and_() }}{{ source_filter }}{% endif %}
      {% if insert_ %}{# FIXME: dt should not be hard coded but taken from ingestion time #}
      {{ and_() }}{{ external_param('start_ts') }} <= dt
      AND dt < {{ external_param('end_ts') }}
      {% endif %}
    {% endif %}
    {% endfor %}
  )
  {% if insert_ %}
  q
  WHERE
    NOT EXISTS (
      SELECT 1
      FROM {{ target_table.full_name }} t
      WHERE t.{{ target_table.root_key.name }} = q.{{ target_table.root_key.name }}
    )
  {% endif %}
)
WHERE rn = 1

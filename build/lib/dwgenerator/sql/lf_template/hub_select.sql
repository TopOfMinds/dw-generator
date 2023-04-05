{% from 'external_param.sql' import external_param %}
SELECT
  {{ target_table.key.name }}
  {% for target_business_key in target_table.business_keys %}
  ,{{ target_business_key.name }}
  {% endfor %}
  ,{{ target_table.load_dts.name }}
  ,{{ target_table.rec_src.name }}
  ,{{ target_table.batch_id.name }}
FROM (
  SELECT
    {{ target_table.key.name }}
    {% for target_business_key in target_table.business_keys %}
    ,{{ target_business_key.name }}
    {% endfor %}
    ,{{ target_table.load_dts.name }}
    ,{{ target_table.rec_src.name }}
	,{{ target_table.batch_id.name }}
    ,row_number() over(PARTITION BY {{ target_table.key.name }} ORDER BY {{ target_table.load_dts.name }} asc) rn
  FROM (
    {% set union_all = joiner("UNION ALL") %}
    {% for source_table in mappings.source_tables(target_table) %}
    {% set source_filter = mappings.filter(source_table, target_table) %}
    {{ union_all() }}
    SELECT
      {{ mappings.source_column(source_table, target_table.key) }} AS {{ target_table.key.name }}
      {% for target_business_key in target_table.business_keys %}
      ,{{ mappings.source_column(source_table, target_business_key) }} AS {{ target_business_key.name }}
      {% endfor %}
      ,{{ mappings.source_column(source_table, target_table.load_dts) }} AS {{ target_table.load_dts.name }}
      ,{{ mappings.source_column(source_table, target_table.rec_src) }} AS {{ target_table.rec_src.name }}
	  ,{{ mappings.source_column(source_table, target_table.batch_id) }} AS {{ target_table.batch_id.name }}
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
      WHERE t.{{ target_table.key.name }} = q.{{ target_table.key.name }}
    )
  {% endif %}
)
WHERE rn = 1

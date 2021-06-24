{% set mtr_key_col = mappings.source_column_object(target_table.metrics_key) %}
{% set mtr_sat = mtr_key_col.parent %}
{% set ctx_key_col = mappings.source_column_object(target_table.context_key) %}
{% set ctx_sat = ctx_key_col.parent %}
SELECT
  mtr.{{ mtr_key_col.name }} AS {{ target_table.metrics_key.name }}
  ,ctx.{{ ctx_key_col.name }} AS {{ target_table.context_key.name }}
  ,ctx.{{ mappings.source_column_object(target_table.context_load_dts).name }} AS {{ target_table.context_load_dts.name }}
  ,mtr.{{ mappings.source_column_object(target_table.load_dts).name }} AS {{ target_table.load_dts.name }}
FROM
  {{ mtr_key_col.parent.name }} AS mtr,
  {% set previous_column = mtr_key_col %}
  {% set previous_alias = 'mtr' %}
  {% set link_columns = mappings.link_path(target_table) %}
  {% for column1, column2 in link_columns %}
  {% set link_alias = 'l' ~ loop.index %}
  {{ column1.parent.name }} AS {{ link_alias }} ON {{ previous_alias }}.{{ previous_column.name }} = {{ link_alias }}.{{ column1.name }},
  {% set previous_column = column2 %}
  {% set previous_alias = 'l' %}
  {% endfor %}
  {% if link_columns %}
  {% set previous_column = link_columns[-1][1] %}
  {% set previous_alias = 'l' ~ (link_columns | length) %}  
  {% endif %}
  (
    SELECT
      {{ ctx_key_col.name }}
      , {{ ctx_sat.effective_ts.name }} AS effective_ts
      , coalesce(lead({{ ctx_sat.effective_ts.name }}) OVER(PARTITION BY {{ ctx_key_col.name }} ORDER BY {{ ctx_sat.effective_ts.name }}), to_date('9999-12-31', 'yyyy-mm-dd')) AS effective_ts_end
    FROM {{ ctx_sat.name }}
  ) AS ctx
  ON ctx.{{ ctx_key_col.name }} = {{ previous_alias }}.{{ previous_column.name }}
    AND ctx.effective_ts <= mtr.{{ mtr_sat.effective_ts.name }}
    AND mtr.{{ mtr_sat.effective_ts.name }} < ctx.effective_ts_end

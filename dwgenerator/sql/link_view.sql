--   ======================================================================================
--    AUTOGENERATED!!!! DO NOT EDIT!!!!
--   ======================================================================================

{% set source_table = mappings.source_tables(target_table)[0] %}
{% set source_filter = mappings.filter(source_table, target_table) %}
{% set concat = joiner(" || '|' || ") %}
{% set comma = joiner(", ") %}
CREATE OR REPLACE VIEW {{ target_table.name }} 
AS 
SELECT {% for key in target_table.keys %}{{ concat() }}{{ mappings.source_column(source_table, key) }}{% endfor %} AS {{ target_table.root_key.name }}
  {% for key in target_table.keys %}
	,{{ mappings.source_column(source_table, key) }} AS {{ key.name }}
  {% endfor %}
	, min({{ mappings.source_column(source_table, target_table.load_dts) }}) AS {{ target_table.load_dts.name }}
	,{{ mappings.source_column(source_table, target_table.rec_src) }} AS {{ target_table.rec_src.name }}
FROM {{ source_table.full_name }}
{% if source_filter %}
WHERE {{ source_filter }}
{% endif %}
GROUP BY {% for key in target_table.keys %}{{ comma() }}{{ mappings.source_column(source_table, key) }}{% endfor %}
;
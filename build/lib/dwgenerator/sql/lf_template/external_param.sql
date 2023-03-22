{% macro external_param(name) -%}
{% if name == 'start_ts' %} v_load_start_ts {% elif name == 'end_ts' %} v_load_end_ts
{% endif %}
{%- endmacro %}
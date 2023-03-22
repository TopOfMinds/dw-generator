{% macro external_param(name) -%}
{% if name == 'start_ts' %} v_batch_started {% elif name == 'end_ts' %} v_batch_ended
{% endif %}
{%- endmacro %}
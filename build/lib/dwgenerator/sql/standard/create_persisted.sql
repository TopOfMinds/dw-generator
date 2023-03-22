{% include 'header.sql' %}

{% set insert_ = target_table.properties.generate_type == 'table' %}
INSERT INTO {{ target_table.full_name }}
{% block select %}
SELECT *
FROM {{ source_table.full_name }}
{% endblock %}
;

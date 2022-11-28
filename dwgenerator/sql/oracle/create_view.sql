{% include 'header.sql' %}

CREATE OR REPLACE VIEW {{ target_table.full_name }}
AS
{% block select %}
SELECT *
FROM {{ source_table.full_name }}
{% endblock %}
;

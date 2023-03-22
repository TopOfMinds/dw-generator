{% include 'header.sql' %}

DROP VIEW IF EXISTS {{ target_table.full_name }};

CREATE VIEW {{ target_table.full_name }}
AS
{% block select %}
SELECT *
FROM {{ source_table.full_name }}
{% endblock %}
;

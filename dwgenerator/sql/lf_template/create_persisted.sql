{% include 'header.sql' %}

CREATE or REPLACE PROCEDURE {{ target_table.full_name }}_etl (p_days INT DEFAULT 2) 
AS
v_batch_started TIMESTAMP(9);
v_load_start_ts NUMBER(19);
v_load_end_ts NUMBER(19);
v_batch_id INT;
v_table_name VARCHAR(40) := '{{  target_table.name }}';
v_24h_ms_epoch NUMBER(19) := 86400000;

BEGIN

SELECT SYS_EXTRACT_UTC(SYSTIMESTAMP) INTO v_batch_started FROM DUAL;
SELECT COALESCE(MAX(load_end_ts) - p_days * v_24h_ms_epoch, TS_TO_EPOCH_MS(TO_DATE('1970-01-01', 'yyyy-mm-dd'))) INTO v_load_start_ts FROM dv.a_dv_batch_log WHERE table_name = v_table_name;

INSERT INTO dv.a_dv_batch_log (table_name, load_start_ts, load_end_ts, batch_started_ts)
VALUES (v_table_name, v_load_start_ts, TS_TO_EPOCH_MS(SYSTIMESTAMP),v_batch_started);

SELECT MAX(batch_id) INTO v_batch_id FROM dv.a_dv_batch_log WHERE table_name = v_table_name;
SELECT load_end_ts INTO v_load_end_ts FROM dv.a_dv_batch_log WHERE batch_id = v_batch_id;

{% set insert_ = target_table.properties.generate_type == 'table' %}
INSERT INTO {{ target_table.full_name }}
{% block select %}
SELECT *
FROM {{ source_table.full_name }}
{% endblock %}
;

UPDATE dv.a_dv_batch_log
SET batch_ended_ts = SYS_EXTRACT_UTC(SYSTIMESTAMP)
WHERE table_name = v_table_name
	AND batch_id = v_batch_id;

COMMIT;
END;
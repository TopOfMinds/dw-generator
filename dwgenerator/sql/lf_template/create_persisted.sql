{% include 'header.sql' %}
{% from 'external_param.sql' import external_param %}

CREATE or REPLACE PROCEDURE {{ target_table.full_name }}_etl (p_sec INT DEFAULT 600, p_flg_delete INT DEFAULT 0)
AS
v_batch_started TIMESTAMP(9);
v_load_start_ts TIMESTAMP(9);
v_load_end_ts TIMESTAMP(9);
v_batch_id INT;
v_table_name VARCHAR(40) := '{{  target_table.name }}';

BEGIN

SELECT SYS_EXTRACT_UTC(SYSTIMESTAMP) INTO v_batch_started FROM DUAL;
SELECT COALESCE(MAX(load_end_ts) - NUMTODSINTERVAL(p_sec, 'SECOND'), TO_DATE('1970-01-01', 'yyyy-mm-dd')) INTO v_load_start_ts FROM dv.xxx_dv_batch_log WHERE table_name = v_table_name;

INSERT INTO dv.xxx_dv_batch_log (table_name, load_start_ts, load_end_ts, batch_started_ts)
VALUES (v_table_name, v_load_start_ts, SYSTIMESTAMP,v_batch_started);

SELECT MAX(batch_id) INTO v_batch_id FROM dv.xxx_dv_batch_log WHERE table_name = v_table_name;
SELECT load_end_ts INTO v_load_end_ts FROM dv.xxx_dv_batch_log WHERE batch_id = v_batch_id;

DELETE FROM {{ target_table.full_name }} 
WHERE {{ external_param('start_ts') }} <= {{ target_table.load_dts.name }} AND {{ target_table.load_dts.name }} < {{ external_param('end_ts') -}}
AND p_flg_delete = 1
;

{% set insert_ = target_table.properties.generate_type == 'table' %}
{% set no_deduplication_ = target_table.properties.no_deduplication == 'true' %}
INSERT INTO {{ target_table.full_name }}
{% block select %}
SELECT *
FROM {{ source_table.full_name }}
{% endblock %}
;

UPDATE dv.xxx_dv_batch_log
SET batch_ended_ts = SYS_EXTRACT_UTC(SYSTIMESTAMP)
WHERE table_name = v_table_name
	AND batch_id = v_batch_id;

COMMIT;
END;
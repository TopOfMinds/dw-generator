{% include 'header.sql' %}
{% from 'external_param.sql' import external_param %}
{% set source_table_v = mappings.source_table(target_table) %}

CREATE or REPLACE PROCEDURE {{ target_table.full_name }}_etl (p_sec INT DEFAULT 604800, p_flg_delete INT DEFAULT 0)
AS
v_batch_started TIMESTAMP(9);
v_batch_ended TIMESTAMP(9);
v_load_start_ts TIMESTAMP(9);
v_load_end_ts TIMESTAMP(9);
v_batch_id INT;
v_no_of_rows NUMBER(9);
v_table_name VARCHAR(50) := '{{  target_table.name }}';
v_src_table_name VARCHAR(100) := '{{ source_table_v.full_name }}';

BEGIN

SELECT CAST( SYSTIMESTAMP AT TIME ZONE 'Europe/Stockholm' AS timestamp ) INTO v_load_start_ts FROM DUAL;
SELECT COALESCE(MAX(batch_ended_ts) - NUMTODSINTERVAL(p_sec, 'SECOND'), TO_DATE('1970-01-01', 'yyyy-mm-dd')) INTO v_batch_started FROM dv.batch_log WHERE table_name = v_table_name;

INSERT INTO dv.batch_log (table_name, src_table_name, load_start_ts, batch_started_ts, batch_ended_ts)
VALUES (v_table_name, v_src_table_name, CAST( SYSTIMESTAMP AT TIME ZONE 'Europe/Stockholm' AS timestamp ), v_batch_started, CAST( SYSTIMESTAMP AT TIME ZONE 'Europe/Stockholm' AS timestamp ));
COMMIT;

SELECT MAX(batch_id) INTO v_batch_id FROM dv.batch_log WHERE table_name = v_table_name;
SELECT batch_ended_ts INTO v_batch_ended FROM dv.batch_log WHERE batch_id = v_batch_id;

DELETE FROM {{ target_table.full_name }} 
WHERE v_batch_started <= {{ target_table.load_dts.name }} AND {{ target_table.load_dts.name }} < v_batch_ended
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

select count(*)  INTO v_no_of_rows FROM {{  target_table.name }} WHERE BATCH_ID =  v_batch_id;

UPDATE dv.batch_log
SET load_end_ts = CAST( SYSTIMESTAMP AT TIME ZONE 'Europe/Stockholm' AS timestamp),
NO_OF_ROWS = v_no_of_rows
WHERE table_name = v_table_name
  AND batch_id = v_batch_id;

COMMIT;
END;
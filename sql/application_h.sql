--Hub that uses GROUP BY ON bk
CREATE OR REPLACE VIEW DV.application_h AS
SELECT ID AS application_key ,
       ID ,
       'edge' AS REC_SRC ,
       min(_SDC_RECEIVED_AT) AS LOAD_DTS
FROM SRC_EDGE.customer
WHERE IS_CO <> FALSE
GROUP BY ID;

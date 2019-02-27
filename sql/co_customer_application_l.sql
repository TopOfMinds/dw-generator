/* 
 View contains logic for calculating the correct relationship between application and co_customer.
 View only contain relationships where the application is active status is true and co_in is true.
 */
CREATE OR REPLACE VIEW DV.CO_CUSTOMER_APPLICATION_L AS 
SELECT 
	customer_key||'|'||application_key AS co_customer_application_key
	,load_dts
	,customer_key
	,application_key
	,rec_src
	FROM (	
SELECT
	social_security_number AS customer_key
	,id - 1 AS application_key
	,_SDC_RECEIVED_AT AS load_dts
	,'edge' AS rec_src
FROM src_edge.CUSTOMER 
WHERE is_co = true
AND active <> false);
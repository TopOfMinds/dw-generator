/* 
 View only contain relationships where the application is active status is true and co_in is null.
 */
CREATE OR REPLACE VIEW DV.CUSTOMER_APPLICATION_L 
AS 

SELECT 
	customer_application_key
	,customer_key
	,application_key
	,load_dts
	,rec_src
FROM (
	  SELECT 
			customer_key||'|'||application_key AS customer_application_key
			,load_dts
			,customer_key
			,application_key
			,rec_src
			,row_number() over(
				PARTITION BY customer_key, application_key ORDER BY eff_ts DESC 
				) rnk
		   FROM (		
				  SELECT
						social_security_number AS customer_key
						,id AS application_key
						,_SDC_RECEIVED_AT AS load_dts
						,'edge' AS rec_src
						,created AS eff_ts
					FROM 
						src_edge.CUSTOMER 
					WHERE is_co IS null
					AND active <> FALSE
			)q
)res
WHERE res.rnk = 1
;
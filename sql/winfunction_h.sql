#Hub created using window functions, simular functionality as in a sattelite 
CREATE OR REPLACE VIEW winfunction_h AS
SELECT res1.SOCIAL_SECURITY_NUMBER AS customer_key ,
       res1.SOCIAL_SECURITY_NUMBER ,
       'edge' AS REC_SRC ,
       res1._SDC_RECEIVED_AT
FROM
  (SELECT SOCIAL_SECURITY_NUMBER ,
          _SDC_RECEIVED_AT ,
          row_number() OVER (PARTITION BY SOCIAL_SECURITY_NUMBER ORDER BY updated DESC, _SDC_RECEIVED_AT DESC) AS rnk
   FROM customer
) res1
WHERE res1.rnk = 1;

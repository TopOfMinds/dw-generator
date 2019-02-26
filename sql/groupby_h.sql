#Hub that uses group by on bk
create view groupby_h as
select
    SOCIAL_SECURITY_NUMBER as customer_key
    ,SOCIAL_SECURITY_NUMBER
    ,'edge' as REC_SRC
    ,max(_SDC_RECEIVED_AT) as LOAD_DTS
from customer
group by SOCIAL_SECURITY_NUMBER;


  
    



create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    
    
    
    as (

SELECT DISTINCT
    EVENT_DATE,
    DAY_NAME,
    DAY_OF_WEEK,
    IS_WEEKEND,
    MONTH(EVENT_DATE)                                        AS MONTH,
    QUARTER(EVENT_DATE)                                      AS QUARTER,
    YEAR(EVENT_DATE)                                         AS YEAR
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
ORDER BY EVENT_DATE
    )
;




  
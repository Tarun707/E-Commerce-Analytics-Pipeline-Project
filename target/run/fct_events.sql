
  
    



create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    
    
    
    as (

SELECT
    EVENT_TIME,
    EVENT_DATE,
    EVENT_HOUR,
    PRODUCT_ID,
    USER_ID,
    USER_SESSION,
    PRICE,
    IS_VIEW,
    IS_CART,
    IS_PURCHASE
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )
;




  
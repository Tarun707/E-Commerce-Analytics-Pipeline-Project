
  
    



create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_products
    
    
    
    as (

SELECT DISTINCT
    PRODUCT_ID,
    CATEGORY_ID,
    CATEGORY_CODE,
    CATEGORY_L1,
    CATEGORY_LEAF,
    BRAND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )
;




  
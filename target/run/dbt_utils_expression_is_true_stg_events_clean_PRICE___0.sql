
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
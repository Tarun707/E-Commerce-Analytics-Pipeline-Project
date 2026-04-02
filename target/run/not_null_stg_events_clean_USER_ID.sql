
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_ID is null



  
  
      
    ) dbt_internal_test
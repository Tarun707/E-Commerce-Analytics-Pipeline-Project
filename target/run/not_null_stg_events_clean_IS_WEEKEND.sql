
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_HOUR
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_HOUR is null



  
  
      
    ) dbt_internal_test
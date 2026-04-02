
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TYPE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TYPE is null



  
  
      
    ) dbt_internal_test
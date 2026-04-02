
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
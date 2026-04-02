
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
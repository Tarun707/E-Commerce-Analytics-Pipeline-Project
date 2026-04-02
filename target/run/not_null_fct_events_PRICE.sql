
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where PRICE is null



  
  
      
    ) dbt_internal_test
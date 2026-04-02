
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_CART is null



  
  
      
    ) dbt_internal_test
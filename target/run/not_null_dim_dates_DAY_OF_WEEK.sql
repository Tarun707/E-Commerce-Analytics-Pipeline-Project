
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_OF_WEEK
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where DAY_OF_WEEK is null



  
  
      
    ) dbt_internal_test
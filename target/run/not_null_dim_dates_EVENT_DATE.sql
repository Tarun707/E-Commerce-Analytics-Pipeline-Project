
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
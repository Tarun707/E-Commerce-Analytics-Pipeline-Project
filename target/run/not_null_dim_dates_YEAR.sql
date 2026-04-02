
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where YEAR is null



  
  
      
    ) dbt_internal_test

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
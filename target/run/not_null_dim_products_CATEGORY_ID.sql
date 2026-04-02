
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_ID
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_ID is null



  
  
      
    ) dbt_internal_test

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
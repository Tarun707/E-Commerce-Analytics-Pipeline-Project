
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where BRAND is null



  
  
      
    ) dbt_internal_test
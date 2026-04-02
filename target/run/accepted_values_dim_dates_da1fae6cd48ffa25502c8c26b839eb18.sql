
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        MONTH as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by MONTH

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6','7','8','9','10','11','12'
)



  
  
      
    ) dbt_internal_test
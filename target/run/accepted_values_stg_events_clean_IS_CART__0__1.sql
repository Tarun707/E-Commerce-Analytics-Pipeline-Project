
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_CART as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_CART

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
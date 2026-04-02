
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_NAME as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by DAY_NAME

)

select *
from all_values
where value_field not in (
    'Mon','Tue','Wed','Thu','Fri','Sat','Sun'
)



  
  
      
    ) dbt_internal_test

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_TYPE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by EVENT_TYPE

)

select *
from all_values
where value_field not in (
    'view','cart','purchase'
)



  
  
      
    ) dbt_internal_test
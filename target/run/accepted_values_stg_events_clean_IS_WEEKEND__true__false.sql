
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_WEEKEND as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_WEEKEND

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test
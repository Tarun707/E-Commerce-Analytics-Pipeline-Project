
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_QUARTER as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by EVENT_QUARTER

)

select *
from all_values
where value_field not in (
    '1','2','3','4'
)



  
  
      
    ) dbt_internal_test
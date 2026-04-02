
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select EVENT_DATE as from_field
    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    where EVENT_DATE is not null
),

parent as (
    select EVENT_DATE as to_field
    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    USER_SESSION as unique_field,
    count(*) as n_records

from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_SESSION is not null
group by USER_SESSION
having count(*) > 1



  
  
      
    ) dbt_internal_test
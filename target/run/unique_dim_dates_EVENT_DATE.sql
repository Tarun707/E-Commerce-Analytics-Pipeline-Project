
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    EVENT_DATE as unique_field,
    count(*) as n_records

from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where EVENT_DATE is not null
group by EVENT_DATE
having count(*) > 1



  
  
      
    ) dbt_internal_test
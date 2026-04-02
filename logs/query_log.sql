============================== 0b6d21bd-ed1d-4dc6-b692-4ae5370f8df6 ==============================
-- created_at: 2026-03-09T14:42:04.657205048+00:00
-- finished_at: 2026-03-09T14:42:04.933424639+00:00
-- elapsed: 276ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2ea12-3202-69f6-0014-581600011a3e
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."RAW"."EVENTS_RAW";
-- created_at: 2026-03-09T14:42:07.179338600+00:00
-- finished_at: 2026-03-09T14:42:07.470583616+00:00
-- elapsed: 291ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2ea12-3202-69f6-0014-581600011a42
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-09T14:42:08.712918271+00:00
-- finished_at: 2026-03-09T14:42:09.012838626+00:00
-- elapsed: 299ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2ea12-3202-69f6-0014-581600011a46
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-09T14:42:09.015186137+00:00
-- finished_at: 2026-03-09T14:42:33.479375821+00:00
-- elapsed: 24.5s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2ea12-3202-69f6-0014-581600011a4a
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    
    
    
    as (

WITH source AS (
    SELECT * FROM ECOMMERCE_ANALYTICS.RAW.EVENTS_RAW
),
cleaned AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        COALESCE(BRAND, 'unknown')                                      AS BRAND,
        COALESCE(CATEGORY_CODE, 'unknown')                              AS CATEGORY_CODE,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.', 1), ''),
            'unknown')                                                   AS CATEGORY_L1,
        COALESCE(
            NULLIF(
                SPLIT_PART(CATEGORY_CODE, '.',
                    ARRAY_SIZE(SPLIT(CATEGORY_CODE, '.'))),
            ''),
            'unknown')                                                   AS CATEGORY_LEAF,
        DATE(EVENT_TIME)                                                 AS EVENT_DATE,
        HOUR(EVENT_TIME)                                                 AS EVENT_HOUR,
        DAYNAME(EVENT_TIME)                                              AS DAY_NAME,
        DAYOFWEEK(EVENT_TIME)                                            AS DAY_OF_WEEK,
        CASE WHEN DAYOFWEEK(EVENT_TIME)
             IN (0, 6) THEN TRUE
             ELSE FALSE END                                              AS IS_WEEKEND,
        CASE WHEN EVENT_TYPE = 'view'     THEN 1 ELSE 0 END             AS IS_VIEW,
        CASE WHEN EVENT_TYPE = 'cart'     THEN 1 ELSE 0 END             AS IS_CART,
        CASE WHEN EVENT_TYPE = 'purchase' THEN 1 ELSE 0 END             AS IS_PURCHASE
    FROM source
    WHERE USER_SESSION IS NOT NULL
)
SELECT * FROM cleaned
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.stg_events_clean", "profile_name": "user", "target_name": "dev"} */;

============================== eafb4e5a-4c67-49e5-b397-e88419ee2415 ==============================
-- created_at: 2026-03-10T03:43:32.181051433+00:00
-- finished_at: 2026-03-10T03:43:32.445883267+00:00
-- elapsed: 264ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2ed1f-3202-6cbe-0014-58160001a0f2
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."RAW"."EVENTS_RAW";
-- created_at: 2026-03-10T03:43:34.256985088+00:00
-- finished_at: 2026-03-10T03:43:34.538242690+00:00
-- elapsed: 281ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2ed1f-3202-6cbe-0014-58160001a0f6
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:43:35.647883904+00:00
-- finished_at: 2026-03-10T03:43:35.910603047+00:00
-- elapsed: 262ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2ed1f-3202-6cbe-0014-58160001a0fa
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T03:43:35.913390079+00:00
-- finished_at: 2026-03-10T03:44:00.300940471+00:00
-- elapsed: 24.4s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2ed1f-3202-6cbe-0014-58160001a0fe
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    
    
    
    as (

WITH source AS (
    SELECT * FROM ECOMMERCE_ANALYTICS.RAW.EVENTS_RAW
),
cleaned AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        COALESCE(BRAND, 'unknown')                                      AS BRAND,
        COALESCE(CATEGORY_CODE, 'unknown')                              AS CATEGORY_CODE,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.', 1), ''),
            'unknown')                                                   AS CATEGORY_L1,
        COALESCE(
            NULLIF(
                SPLIT_PART(CATEGORY_CODE, '.',
                    ARRAY_SIZE(SPLIT(CATEGORY_CODE, '.'))),
            ''),
            'unknown')                                                   AS CATEGORY_LEAF,
        DATE(EVENT_TIME)                                                 AS EVENT_DATE,
        HOUR(EVENT_TIME)                                                 AS EVENT_HOUR,
        DAYNAME(EVENT_TIME)                                              AS DAY_NAME,
        DAYOFWEEK(EVENT_TIME)                                            AS DAY_OF_WEEK,
        CASE WHEN DAYOFWEEK(EVENT_TIME)
             IN (0, 6) THEN TRUE
             ELSE FALSE END                                              AS IS_WEEKEND,
        CASE WHEN EVENT_TYPE = 'view'     THEN 1 ELSE 0 END             AS IS_VIEW,
        CASE WHEN EVENT_TYPE = 'cart'     THEN 1 ELSE 0 END             AS IS_CART,
        CASE WHEN EVENT_TYPE = 'purchase' THEN 1 ELSE 0 END             AS IS_PURCHASE
    FROM source
    WHERE USER_SESSION IS NOT NULL
)
SELECT * FROM cleaned
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.stg_events_clean", "profile_name": "user", "target_name": "dev"} */;

============================== a60d92bc-12d7-4fe9-994e-da05bc7c006a ==============================
-- created_at: 2026-03-10T03:48:42.091195799+00:00
-- finished_at: 2026-03-10T03:48:42.333740552+00:00
-- elapsed: 242ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a212
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-10T03:48:44.195016611+00:00
-- finished_at: 2026-03-10T03:48:44.551989422+00:00
-- elapsed: 356ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a21a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.217909131+00:00
-- finished_at: 2026-03-10T03:48:44.564759733+00:00
-- elapsed: 346ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a226
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.209925408+00:00
-- finished_at: 2026-03-10T03:48:44.572617311+00:00
-- elapsed: 362ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a222
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.257816476+00:00
-- finished_at: 2026-03-10T03:48:44.578243361+00:00
-- elapsed: 320ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a256
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.235638663+00:00
-- finished_at: 2026-03-10T03:48:44.580231066+00:00
-- elapsed: 344ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a242
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.225070049+00:00
-- finished_at: 2026-03-10T03:48:44.621242797+00:00
-- elapsed: 396ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a22a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.257467899+00:00
-- finished_at: 2026-03-10T03:48:44.623201915+00:00
-- elapsed: 365ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a262
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TYPE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TYPE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.235907791+00:00
-- finished_at: 2026-03-10T03:48:44.624987285+00:00
-- elapsed: 389ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a232
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.245265646+00:00
-- finished_at: 2026-03-10T03:48:44.626459609+00:00
-- elapsed: 381ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a25a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.232433009+00:00
-- finished_at: 2026-03-10T03:48:44.636961254+00:00
-- elapsed: 404ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a23a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.204378870+00:00
-- finished_at: 2026-03-10T03:48:44.643172964+00:00
-- elapsed: 438ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a21e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.229302528+00:00
-- finished_at: 2026-03-10T03:48:44.670069454+00:00
-- elapsed: 440ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a22e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.259700862+00:00
-- finished_at: 2026-03-10T03:48:44.670744499+00:00
-- elapsed: 411ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a25e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.253739853+00:00
-- finished_at: 2026-03-10T03:48:44.698728215+00:00
-- elapsed: 444ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a24e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.250895412+00:00
-- finished_at: 2026-03-10T03:48:45.108876472+00:00
-- elapsed: 857ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a24a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.249875682+00:00
-- finished_at: 2026-03-10T03:48:45.112698052+00:00
-- elapsed: 862ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a246
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.196875023+00:00
-- finished_at: 2026-03-10T03:48:45.134118703+00:00
-- elapsed: 937ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a216
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.230666430+00:00
-- finished_at: 2026-03-10T03:48:45.177090462+00:00
-- elapsed: 946ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a236
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.252244046+00:00
-- finished_at: 2026-03-10T03:48:45.233936047+00:00
-- elapsed: 981ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a252
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:48:44.246773761+00:00
-- finished_at: 2026-03-10T03:48:47.229975574+00:00
-- elapsed: 3.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.unique_stg_events_clean_USER_SESSION.3b890bd156
-- query_id: 01c2ed24-3202-6cbe-0014-58160001a23e
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.unique_stg_events_clean_USER_SESSION.3b890bd156", "profile_name": "user", "target_name": "dev"} */;

============================== 817f657f-a7f1-42aa-bd0a-c80c8fae15b0 ==============================
-- created_at: 2026-03-10T03:54:03.531843489+00:00
-- finished_at: 2026-03-10T03:54:03.796214426+00:00
-- elapsed: 264ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2aa
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-10T03:54:05.641590953+00:00
-- finished_at: 2026-03-10T03:54:05.898488131+00:00
-- elapsed: 256ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2b2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.663513045+00:00
-- finished_at: 2026-03-10T03:54:05.922488682+00:00
-- elapsed: 258ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2b6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.669114022+00:00
-- finished_at: 2026-03-10T03:54:05.923286639+00:00
-- elapsed: 254ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2ba
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.669963446+00:00
-- finished_at: 2026-03-10T03:54:05.924389903+00:00
-- elapsed: 254ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2c6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.670704911+00:00
-- finished_at: 2026-03-10T03:54:05.924394825+00:00
-- elapsed: 253ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2c2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.671386018+00:00
-- finished_at: 2026-03-10T03:54:05.926127686+00:00
-- elapsed: 254ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2be
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.678590700+00:00
-- finished_at: 2026-03-10T03:54:05.927865886+00:00
-- elapsed: 249ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2ca
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.681237769+00:00
-- finished_at: 2026-03-10T03:54:05.930672396+00:00
-- elapsed: 249ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2ce
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.688940547+00:00
-- finished_at: 2026-03-10T03:54:05.953514312+00:00
-- elapsed: 264ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2d2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.689112828+00:00
-- finished_at: 2026-03-10T03:54:05.954266270+00:00
-- elapsed: 265ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2da
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TYPE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TYPE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.689682922+00:00
-- finished_at: 2026-03-10T03:54:05.954274069+00:00
-- elapsed: 264ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2d6
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.691233589+00:00
-- finished_at: 2026-03-10T03:54:05.955857427+00:00
-- elapsed: 264ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2de
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.695901877+00:00
-- finished_at: 2026-03-10T03:54:05.957858415+00:00
-- elapsed: 261ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2e6
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.698221526+00:00
-- finished_at: 2026-03-10T03:54:05.959429343+00:00
-- elapsed: 261ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2ee
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.708707289+00:00
-- finished_at: 2026-03-10T03:54:05.966812074+00:00
-- elapsed: 258ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2f2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.707410922+00:00
-- finished_at: 2026-03-10T03:54:05.968809142+00:00
-- elapsed: 261ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2e2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.709194169+00:00
-- finished_at: 2026-03-10T03:54:05.972289973+00:00
-- elapsed: 263ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2ea
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.733684136+00:00
-- finished_at: 2026-03-10T03:54:06.001802724+00:00
-- elapsed: 268ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2f6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T03:54:05.737476667+00:00
-- finished_at: 2026-03-10T03:54:06.007881599+00:00
-- elapsed: 270ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70
-- query_id: 01c2ed2a-3202-6cbe-0014-58160001a2fa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70", "profile_name": "user", "target_name": "dev"} */;

============================== bdaa4d3c-8d02-459c-a150-32a08acf77e0 ==============================

============================== 8b0111e5-3823-4a9f-945b-ad08140bf0c3 ==============================

============================== 352f8375-47fa-48a7-8d05-85cbffd8e234 ==============================

============================== 0af35c0e-acfd-4341-b070-0678fc6c1dd7 ==============================
-- created_at: 2026-03-10T04:07:07.873533048+00:00
-- finished_at: 2026-03-10T04:07:08.154677053+00:00
-- elapsed: 281ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a302
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-10T04:07:10.065685443+00:00
-- finished_at: 2026-03-10T04:07:10.312885272+00:00
-- elapsed: 247ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a30a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.061732023+00:00
-- finished_at: 2026-03-10T04:07:10.313874021+00:00
-- elapsed: 252ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a306
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TYPE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TYPE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.067409637+00:00
-- finished_at: 2026-03-10T04:07:10.319410692+00:00
-- elapsed: 252ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a312
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.064255240+00:00
-- finished_at: 2026-03-10T04:07:10.320998514+00:00
-- elapsed: 256ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a30e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.069679457+00:00
-- finished_at: 2026-03-10T04:07:10.326081464+00:00
-- elapsed: 256ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a316
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.092412536+00:00
-- finished_at: 2026-03-10T04:07:10.356877981+00:00
-- elapsed: 264ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a31a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.100701191+00:00
-- finished_at: 2026-03-10T04:07:10.360030144+00:00
-- elapsed: 259ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a326
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.098142996+00:00
-- finished_at: 2026-03-10T04:07:10.361820208+00:00
-- elapsed: 263ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a322
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.107352451+00:00
-- finished_at: 2026-03-10T04:07:10.365501911+00:00
-- elapsed: 258ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a332
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.107724565+00:00
-- finished_at: 2026-03-10T04:07:10.372212803+00:00
-- elapsed: 264ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a33e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.111335267+00:00
-- finished_at: 2026-03-10T04:07:10.373046051+00:00
-- elapsed: 261ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a32a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.114001387+00:00
-- finished_at: 2026-03-10T04:07:10.378806273+00:00
-- elapsed: 264ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a32e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.120686128+00:00
-- finished_at: 2026-03-10T04:07:10.379558585+00:00
-- elapsed: 258ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a342
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.117831416+00:00
-- finished_at: 2026-03-10T04:07:10.380361331+00:00
-- elapsed: 262ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a33a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.105833258+00:00
-- finished_at: 2026-03-10T04:07:10.385981458+00:00
-- elapsed: 280ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a336
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.093822002+00:00
-- finished_at: 2026-03-10T04:07:10.397798445+00:00
-- elapsed: 303ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a31e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.139693616+00:00
-- finished_at: 2026-03-10T04:07:10.403557580+00:00
-- elapsed: 263ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a346
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.157859213+00:00
-- finished_at: 2026-03-10T04:07:10.427247018+00:00
-- elapsed: 269ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a34a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.159061357+00:00
-- finished_at: 2026-03-10T04:07:10.444483754+00:00
-- elapsed: 285ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a352
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T04:07:10.157623702+00:00
-- finished_at: 2026-03-10T04:07:10.453945498+00:00
-- elapsed: 296ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5
-- query_id: 01c2ed37-3202-6cbe-0014-58160001a34e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5", "profile_name": "user", "target_name": "dev"} */;

============================== b22669c5-9679-45cd-8307-dcbe2f46a92a ==============================
-- created_at: 2026-03-10T06:54:27.770151368+00:00
-- finished_at: 2026-03-10T06:54:28.107489879+00:00
-- elapsed: 337ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2edde-3202-69f6-0014-58160001721e
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."RAW"."EVENTS_RAW";
-- created_at: 2026-03-10T06:54:29.903310079+00:00
-- finished_at: 2026-03-10T06:54:30.169575982+00:00
-- elapsed: 266ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2edde-3202-69f6-0014-581600017222
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:54:31.310760178+00:00
-- finished_at: 2026-03-10T06:54:31.603068677+00:00
-- elapsed: 292ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2edde-3202-69f6-0014-581600017226
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T06:54:31.605951215+00:00
-- finished_at: 2026-03-10T06:54:56.518797715+00:00
-- elapsed: 24.9s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2edde-3202-69f6-0014-58160001722a
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    
    
    
    as (

WITH source AS (
    SELECT * FROM ECOMMERCE_ANALYTICS.RAW.EVENTS_RAW
),
cleaned AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        COALESCE(BRAND, 'unknown')                                      AS BRAND,
        COALESCE(CATEGORY_CODE, 'unknown')                              AS CATEGORY_CODE,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.', 1), ''),
            'unknown')                                                   AS CATEGORY_L1,
        COALESCE(
            NULLIF(
                SPLIT_PART(CATEGORY_CODE, '.',
                    ARRAY_SIZE(SPLIT(CATEGORY_CODE, '.'))),
            ''),
            'unknown')                                                   AS CATEGORY_LEAF,
        DATE(EVENT_TIME)                                                 AS EVENT_DATE,
        HOUR(EVENT_TIME)                                                 AS EVENT_HOUR,
        DAYNAME(EVENT_TIME)                                              AS DAY_NAME,
        DAYOFWEEK(EVENT_TIME)                                            AS DAY_OF_WEEK,
        CASE WHEN DAYOFWEEK(EVENT_TIME)
             IN (0, 6) THEN TRUE
             ELSE FALSE END                                              AS IS_WEEKEND,
        MONTH(EVENT_TIME)                                                AS EVENT_MONTH,
        QUARTER(EVENT_TIME)                                             AS EVENT_QUARTER,
        YEAR(EVENT_TIME)                                                AS EVENT_YEAR,    
        CASE WHEN EVENT_TYPE = 'view'     THEN 1 ELSE 0 END             AS IS_VIEW,
        CASE WHEN EVENT_TYPE = 'cart'     THEN 1 ELSE 0 END             AS IS_CART,
        CASE WHEN EVENT_TYPE = 'purchase' THEN 1 ELSE 0 END             AS IS_PURCHASE
    FROM source
    WHERE USER_SESSION IS NOT NULL
)
SELECT * FROM cleaned
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.stg_events_clean", "profile_name": "user", "target_name": "dev"} */;

============================== 2c0a9549-e2d3-4558-8d00-3a2abd32356a ==============================
-- created_at: 2026-03-10T06:56:25.168237263+00:00
-- finished_at: 2026-03-10T06:56:25.472280759+00:00
-- elapsed: 304ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2ede0-3202-69f6-0014-58160001722e
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-10T06:56:27.412060760+00:00
-- finished_at: 2026-03-10T06:56:27.779301141+00:00
-- elapsed: 367ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02
-- query_id: 01c2ede0-3202-69f6-0014-581600017266
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_MONTH
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_MONTH is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.379548863+00:00
-- finished_at: 2026-03-10T06:56:27.783765280+00:00
-- elapsed: 404ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43
-- query_id: 01c2ede0-3202-69f6-0014-581600017246
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.452296034+00:00
-- finished_at: 2026-03-10T06:56:27.812936746+00:00
-- elapsed: 360ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7
-- query_id: 01c2ede0-3202-69f6-0014-581600017286
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_YEAR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.424001529+00:00
-- finished_at: 2026-03-10T06:56:27.817540652+00:00
-- elapsed: 393ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296
-- query_id: 01c2ede0-3202-69f6-0014-58160001727a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.436107190+00:00
-- finished_at: 2026-03-10T06:56:27.823173836+00:00
-- elapsed: 387ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177
-- query_id: 01c2ede0-3202-69f6-0014-581600017282
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.420099660+00:00
-- finished_at: 2026-03-10T06:56:27.824681956+00:00
-- elapsed: 404ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380
-- query_id: 01c2ede0-3202-69f6-0014-58160001726a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.392049568+00:00
-- finished_at: 2026-03-10T06:56:27.837851122+00:00
-- elapsed: 445ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da
-- query_id: 01c2ede0-3202-69f6-0014-58160001724e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.385319941+00:00
-- finished_at: 2026-03-10T06:56:27.843014481+00:00
-- elapsed: 457ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd
-- query_id: 01c2ede0-3202-69f6-0014-58160001724a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.365590330+00:00
-- finished_at: 2026-03-10T06:56:27.853009951+00:00
-- elapsed: 487ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b
-- query_id: 01c2ede0-3202-69f6-0014-581600017232
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TYPE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TYPE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.424783849+00:00
-- finished_at: 2026-03-10T06:56:27.874268110+00:00
-- elapsed: 449ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4
-- query_id: 01c2ede0-3202-69f6-0014-581600017272
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_QUARTER
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_QUARTER is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.424089062+00:00
-- finished_at: 2026-03-10T06:56:27.883272956+00:00
-- elapsed: 459ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4
-- query_id: 01c2ede0-3202-69f6-0014-581600017276
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.364784184+00:00
-- finished_at: 2026-03-10T06:56:27.885178498+00:00
-- elapsed: 520ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e
-- query_id: 01c2ede0-3202-69f6-0014-581600017236
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.403598818+00:00
-- finished_at: 2026-03-10T06:56:27.897554053+00:00
-- elapsed: 493ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a
-- query_id: 01c2ede0-3202-69f6-0014-581600017256
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.367902046+00:00
-- finished_at: 2026-03-10T06:56:27.900671259+00:00
-- elapsed: 532ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561
-- query_id: 01c2ede0-3202-69f6-0014-58160001723a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.378728232+00:00
-- finished_at: 2026-03-10T06:56:27.913567651+00:00
-- elapsed: 534ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70
-- query_id: 01c2ede0-3202-69f6-0014-58160001723e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.436976501+00:00
-- finished_at: 2026-03-10T06:56:27.933020025+00:00
-- elapsed: 496ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526
-- query_id: 01c2ede0-3202-69f6-0014-58160001727e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.406673402+00:00
-- finished_at: 2026-03-10T06:56:27.933720212+00:00
-- elapsed: 527ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5
-- query_id: 01c2ede0-3202-69f6-0014-58160001725e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.454406061+00:00
-- finished_at: 2026-03-10T06:56:27.948397340+00:00
-- elapsed: 493ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a
-- query_id: 01c2ede0-3202-69f6-0014-58160001728a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.410085434+00:00
-- finished_at: 2026-03-10T06:56:28.208516420+00:00
-- elapsed: 798ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57
-- query_id: 01c2ede0-3202-69f6-0014-58160001726e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.405015669+00:00
-- finished_at: 2026-03-10T06:56:28.242898493+00:00
-- elapsed: 837ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73
-- query_id: 01c2ede0-3202-69f6-0014-58160001725a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.405658698+00:00
-- finished_at: 2026-03-10T06:56:28.332198986+00:00
-- elapsed: 926ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62
-- query_id: 01c2ede0-3202-69f6-0014-581600017262
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.382711741+00:00
-- finished_at: 2026-03-10T06:56:28.334591779+00:00
-- elapsed: 951ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c
-- query_id: 01c2ede0-3202-69f6-0014-581600017242
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T06:56:27.393016585+00:00
-- finished_at: 2026-03-10T06:56:28.356960029+00:00
-- elapsed: 963ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187
-- query_id: 01c2ede0-3202-69f6-0014-581600017252
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187", "profile_name": "user", "target_name": "dev"} */;

============================== 49909364-75d6-46b1-b3f6-76e763d46756 ==============================

============================== c1e80351-3b91-411a-92fd-ccd922a3859c ==============================
-- created_at: 2026-03-10T11:10:07.321057146+00:00
-- finished_at: 2026-03-10T11:10:07.619430397+00:00
-- elapsed: 298ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2eede-3202-6d4b-0014-58160001c042
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-10T11:10:09.411595892+00:00
-- finished_at: 2026-03-10T11:10:09.678849587+00:00
-- elapsed: 267ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2eede-3202-6d4b-0014-58160001c046
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:10.680061595+00:00
-- finished_at: 2026-03-10T11:10:10.939632713+00:00
-- elapsed: 259ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2eede-3202-6d4b-0014-58160001c04a
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T11:10:10.695537389+00:00
-- finished_at: 2026-03-10T11:10:10.956545726+00:00
-- elapsed: 261ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2eede-3202-6d4b-0014-58160001c04e
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T11:10:10.724342661+00:00
-- finished_at: 2026-03-10T11:10:11.008818329+00:00
-- elapsed: 284ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2eede-3202-6d4b-0014-58160001c052
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T11:10:10.942227278+00:00
-- finished_at: 2026-03-10T11:10:14.862284046+00:00
-- elapsed: 3.9s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2eede-3202-6d4b-0014-58160001c056
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    
    
    
    as (

SELECT DISTINCT
    EVENT_DATE,
    DAY_NAME,
    DAY_OF_WEEK,
    IS_WEEKEND,
    MONTH(EVENT_DATE)                                        AS MONTH,
    QUARTER(EVENT_DATE)                                      AS QUARTER,
    YEAR(EVENT_DATE)                                         AS YEAR
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
ORDER BY EVENT_DATE
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_dates", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:11.010983294+00:00
-- finished_at: 2026-03-10T11:10:17.426885509+00:00
-- elapsed: 6.4s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2eede-3202-6d4b-0014-58160001c05e
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_products
    
    
    
    as (

SELECT DISTINCT
    PRODUCT_ID,
    CATEGORY_ID,
    CATEGORY_CODE,
    CATEGORY_L1,
    CATEGORY_LEAF,
    BRAND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_products", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:10.958949061+00:00
-- finished_at: 2026-03-10T11:10:31.007690876+00:00
-- elapsed: 20.0s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2eede-3202-6d4b-0014-58160001c05a
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    
    
    
    as (

SELECT
    EVENT_TIME,
    EVENT_DATE,
    EVENT_HOUR,
    PRODUCT_ID,
    USER_ID,
    USER_SESSION,
    PRICE,
    IS_VIEW,
    IS_CART,
    IS_PURCHASE,
    IS_WEEKEND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.fct_events", "profile_name": "user", "target_name": "dev"} */;

============================== 92847275-42d7-4133-aadc-075919361f6c ==============================
-- created_at: 2026-03-10T11:10:47.078031315+00:00
-- finished_at: 2026-03-10T11:10:47.340633028+00:00
-- elapsed: 262ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2eede-3202-6d4b-0014-58160001c062
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."DIM_DATES";
-- created_at: 2026-03-10T11:10:47.107770590+00:00
-- finished_at: 2026-03-10T11:10:47.365970467+00:00
-- elapsed: 258ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2eede-3202-6d4b-0014-58160001c066
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."DIM_PRODUCTS";
-- created_at: 2026-03-10T11:10:47.348075290+00:00
-- finished_at: 2026-03-10T11:10:47.592229722+00:00
-- elapsed: 244ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2eede-3202-6d4b-0014-58160001c06a
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."FCT_EVENTS";
-- created_at: 2026-03-10T11:10:49.265703879+00:00
-- finished_at: 2026-03-10T11:10:49.620428051+00:00
-- elapsed: 354ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_PURCHASE.b9b980b060
-- query_id: 01c2eede-3202-6d4b-0014-58160001c07a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_PURCHASE.b9b980b060", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.298081524+00:00
-- finished_at: 2026-03-10T11:10:49.635807618+00:00
-- elapsed: 337ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_QUARTER__1__2__3__4.4182f7a3b9
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0a2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        QUARTER as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by QUARTER

)

select *
from all_values
where value_field not in (
    '1','2','3','4'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_QUARTER__1__2__3__4.4182f7a3b9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.271903175+00:00
-- finished_at: 2026-03-10T11:10:49.638456128+00:00
-- elapsed: 366ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_IS_WEEKEND.c97a7435a9
-- query_id: 01c2eede-3202-6d4b-0014-58160001c08a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_IS_WEEKEND.c97a7435a9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.312763612+00:00
-- finished_at: 2026-03-10T11:10:49.641875568+00:00
-- elapsed: 329ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_QUARTER.6c5112ec42
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0ca
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select QUARTER
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where QUARTER is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_QUARTER.6c5112ec42", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.262107878+00:00
-- finished_at: 2026-03-10T11:10:49.655305697+00:00
-- elapsed: 393ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_PRODUCT_ID.de5c71a3af
-- query_id: 01c2eede-3202-6d4b-0014-58160001c086
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_PRODUCT_ID.de5c71a3af", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.301990381+00:00
-- finished_at: 2026-03-10T11:10:49.656167919+00:00
-- elapsed: 354ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_WEEKEND.929546128f
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0aa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_WEEKEND.929546128f", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.296770898+00:00
-- finished_at: 2026-03-10T11:10:49.660151715+00:00
-- elapsed: 363ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_TIME.6fb849a6a6
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0a6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_TIME.6fb849a6a6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.268718831+00:00
-- finished_at: 2026-03-10T11:10:49.668695078+00:00
-- elapsed: 399ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_fct_events_PRICE___0.437bc2a8d4
-- query_id: 01c2eede-3202-6d4b-0014-58160001c082
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_fct_events_PRICE___0.437bc2a8d4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.257980542+00:00
-- finished_at: 2026-03-10T11:10:49.673562842+00:00
-- elapsed: 415ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_VIEW.5f15874792
-- query_id: 01c2eede-3202-6d4b-0014-58160001c06e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_VIEW.5f15874792", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.257980629+00:00
-- finished_at: 2026-03-10T11:10:49.679167362+00:00
-- elapsed: 421ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_CART.c05a32eb8a
-- query_id: 01c2eede-3202-6d4b-0014-58160001c072
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_CART.c05a32eb8a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.258725204+00:00
-- finished_at: 2026-03-10T11:10:49.683360081+00:00
-- elapsed: 424ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_EVENT_DATE.4e052aed85
-- query_id: 01c2eede-3202-6d4b-0014-58160001c076
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_EVENT_DATE.4e052aed85", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.290083902+00:00
-- finished_at: 2026-03-10T11:10:49.700096744+00:00
-- elapsed: 410ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_HOUR.6ef38f0fd6
-- query_id: 01c2eede-3202-6d4b-0014-58160001c09a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_HOUR
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_HOUR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_HOUR.6ef38f0fd6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.304871207+00:00
-- finished_at: 2026-03-10T11:10:49.706108964+00:00
-- elapsed: 401ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_PRICE.feae84d4e9
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0ba
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_PRICE.feae84d4e9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.322916441+00:00
-- finished_at: 2026-03-10T11:10:49.723986377+00:00
-- elapsed: 401ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_YEAR.6998c47062
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0da
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where YEAR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_YEAR.6998c47062", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.298063968+00:00
-- finished_at: 2026-03-10T11:10:49.727997612+00:00
-- elapsed: 429ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_DAY_OF_WEEK.11e0aa6c4d
-- query_id: 01c2eede-3202-6d4b-0014-58160001c09e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_OF_WEEK
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where DAY_OF_WEEK is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_DAY_OF_WEEK.11e0aa6c4d", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.304875483+00:00
-- finished_at: 2026-03-10T11:10:49.732244847+00:00
-- elapsed: 427ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_MONTH.371af8f104
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0be
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select MONTH
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where MONTH is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_MONTH.371af8f104", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.338013189+00:00
-- finished_at: 2026-03-10T11:10:49.751909336+00:00
-- elapsed: 413ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_DAY_NAME.437c012c02
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0de
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_NAME
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where DAY_NAME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_DAY_NAME.437c012c02", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.311374311+00:00
-- finished_at: 2026-03-10T11:10:49.769946967+00:00
-- elapsed: 458ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_CODE.0d8dad3555
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0c2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_CODE.0d8dad3555", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.267978823+00:00
-- finished_at: 2026-03-10T11:10:49.785474456+00:00
-- elapsed: 517ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_L1.5043e574fa
-- query_id: 01c2eede-3202-6d4b-0014-58160001c07e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_L1.5043e574fa", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.319291894+00:00
-- finished_at: 2026-03-10T11:10:49.793479696+00:00
-- elapsed: 474ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_PRODUCT_ID.1f28c486d7
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0d2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_PRODUCT_ID.1f28c486d7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.302185073+00:00
-- finished_at: 2026-03-10T11:10:49.795814323+00:00
-- elapsed: 493ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_LEAF.060e9e5228
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0ae
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_LEAF.060e9e5228", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.347467101+00:00
-- finished_at: 2026-03-10T11:10:49.823430591+00:00
-- elapsed: 475ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_da1fae6cd48ffa25502c8c26b839eb18.b4192735d6
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0e2
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_da1fae6cd48ffa25502c8c26b839eb18.b4192735d6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.357330711+00:00
-- finished_at: 2026-03-10T11:10:49.880461052+00:00
-- elapsed: 523ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_BRAND.a5ec5821ff
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0ea
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_BRAND.a5ec5821ff", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.317401816+00:00
-- finished_at: 2026-03-10T11:10:49.902823617+00:00
-- elapsed: 585ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_IS_WEEKEND__true__false.8dd2703a3a
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0ce
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_WEEKEND as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by IS_WEEKEND

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_IS_WEEKEND__true__false.8dd2703a3a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.303622017+00:00
-- finished_at: 2026-03-10T11:10:49.914152453+00:00
-- elapsed: 610ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_251de65f5724bcf7d23364b11dc72fed.66c9ba7fe7
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0b6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_NAME as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by DAY_NAME

)

select *
from all_values
where value_field not in (
    'Mon','Tue','Wed','Thu','Fri','Sat','Sun'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_251de65f5724bcf7d23364b11dc72fed.66c9ba7fe7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.310666153+00:00
-- finished_at: 2026-03-10T11:10:49.922242186+00:00
-- elapsed: 611ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_DAY_OF_WEEK__0__1__2__3__4__5__6.7efae6f4dd
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0c6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_OF_WEEK as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by DAY_OF_WEEK

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_DAY_OF_WEEK__0__1__2__3__4__5__6.7efae6f4dd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.351769702+00:00
-- finished_at: 2026-03-10T11:10:49.951063856+00:00
-- elapsed: 599ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_USER_ID.60ee434024
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0e6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_USER_ID.60ee434024", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.361631198+00:00
-- finished_at: 2026-03-10T11:10:49.985165306+00:00
-- elapsed: 623ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_ID.579c7768dd
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0fa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_ID
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_ID.579c7768dd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.361325486+00:00
-- finished_at: 2026-03-10T11:10:49.985929970+00:00
-- elapsed: 624ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_USER_SESSION.19d896670e
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0f6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_USER_SESSION.19d896670e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.302548877+00:00
-- finished_at: 2026-03-10T11:10:49.988770189+00:00
-- elapsed: 686ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_VIEW__0__1.5fe37935f3
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0b2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_VIEW__0__1.5fe37935f3", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.373806119+00:00
-- finished_at: 2026-03-10T11:10:49.989495888+00:00
-- elapsed: 615ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_DATE.db2e1e2234
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0fe
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_DATE.db2e1e2234", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.285567547+00:00
-- finished_at: 2026-03-10T11:10:50.020209583+00:00
-- elapsed: 734ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.unique_dim_dates_EVENT_DATE.46b102f080
-- query_id: 01c2eede-3202-6d4b-0014-58160001c096
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.unique_dim_dates_EVENT_DATE.46b102f080", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.277177837+00:00
-- finished_at: 2026-03-10T11:10:50.168869345+00:00
-- elapsed: 891ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.unique_dim_products_PRODUCT_ID.5b3502e45d
-- query_id: 01c2eede-3202-6d4b-0014-58160001c08e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    PRODUCT_ID as unique_field,
    count(*) as n_records

from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where PRODUCT_ID is not null
group by PRODUCT_ID
having count(*) > 1



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.unique_dim_products_PRODUCT_ID.5b3502e45d", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.318818628+00:00
-- finished_at: 2026-03-10T11:10:50.221196855+00:00
-- elapsed: 902ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_CART__0__1.5c40c41c0c
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0d6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_CART as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_CART

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_CART__0__1.5c40c41c0c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.392397128+00:00
-- finished_at: 2026-03-10T11:10:50.302968616+00:00
-- elapsed: 910ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_PURCHASE__0__1.692b30d4c4
-- query_id: 01c2eede-3202-6d4b-0014-58160001c106
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_PURCHASE__0__1.692b30d4c4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.282818877+00:00
-- finished_at: 2026-03-10T11:10:50.328202223+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.relationships_fct_events_EVENT_DATE__EVENT_DATE__ref_dim_dates_.3f7b055ece
-- query_id: 01c2eede-3202-6d4b-0014-58160001c092
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.relationships_fct_events_EVENT_DATE__EVENT_DATE__ref_dim_dates_.3f7b055ece", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.353632735+00:00
-- finished_at: 2026-03-10T11:10:50.352479757+00:00
-- elapsed: 998ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_789f24d42f36ec2f925c6d85d0999705.663b6a250f
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0ee
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_HOUR as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by EVENT_HOUR

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_789f24d42f36ec2f925c6d85d0999705.663b6a250f", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.376244938+00:00
-- finished_at: 2026-03-10T11:10:50.514844295+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_WEEKEND__true__false.b010ff5ce4
-- query_id: 01c2eede-3202-6d4b-0014-58160001c102
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_WEEKEND as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_WEEKEND

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_WEEKEND__true__false.b010ff5ce4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T11:10:49.359190339+00:00
-- finished_at: 2026-03-10T11:10:50.581060396+00:00
-- elapsed: 1.2s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.relationships_fct_events_14461f4a74831fe72206b0fc8a0392ab.bb7991c1fa
-- query_id: 01c2eede-3202-6d4b-0014-58160001c0f2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select PRODUCT_ID as from_field
    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    where PRODUCT_ID is not null
),

parent as (
    select PRODUCT_ID as to_field
    from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.relationships_fct_events_14461f4a74831fe72206b0fc8a0392ab.bb7991c1fa", "profile_name": "user", "target_name": "dev"} */;

============================== aab3c4cf-a9c2-43ff-b68c-4078ea4c1e8e ==============================
-- created_at: 2026-03-10T15:41:32.302485187+00:00
-- finished_at: 2026-03-10T15:41:32.590558489+00:00
-- elapsed: 288ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2efed-3202-6dbd-0014-5816000210f6
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."RAW"."EVENTS_RAW";
-- created_at: 2026-03-10T15:41:34.702346860+00:00
-- finished_at: 2026-03-10T15:41:34.998747911+00:00
-- elapsed: 296ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2efed-3202-6c48-0014-58160002210a
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:41:36.162406809+00:00
-- finished_at: 2026-03-10T15:41:36.475641162+00:00
-- elapsed: 313ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2efed-3202-6c48-0014-58160002210e
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T15:41:36.480233271+00:00
-- finished_at: 2026-03-10T15:42:09.108868456+00:00
-- elapsed: 32.6s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2efed-3202-6db9-0014-58160001e17a
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    
    
    
    as (

WITH source AS (
    SELECT * FROM ECOMMERCE_ANALYTICS.RAW.EVENTS_RAW
),
brand_fixed AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        COALESCE(
            BRAND,
            FIRST_VALUE(BRAND) IGNORE NULLS OVER (
                PARTITION BY PRODUCT_ID
                ORDER BY EVENT_TIME
            ),
            'unknown'
        )                        AS BRAND,
        COALESCE(
            CATEGORY_CODE, 
            'unknown'
        )                        AS CATEGORY_CODE
    FROM source
    WHERE USER_SESSION IS NOT NULL
),
cleaned AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        BRAND,
        CATEGORY_CODE,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.', 1), ''),
            'unknown')           AS CATEGORY_L1,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.',
                ARRAY_SIZE(SPLIT(CATEGORY_CODE, '.'))), ''),
            'unknown')           AS CATEGORY_LEAF,
        DATE(EVENT_TIME)         AS EVENT_DATE,
        HOUR(EVENT_TIME)         AS EVENT_HOUR,
        DAYNAME(EVENT_TIME)      AS DAY_NAME,
        DAYOFWEEK(EVENT_TIME)    AS DAY_OF_WEEK,
        CASE WHEN DAYOFWEEK(EVENT_TIME)
             IN (0, 6) THEN TRUE
             ELSE FALSE END      AS IS_WEEKEND,
        MONTH(EVENT_TIME)        AS EVENT_MONTH,
        QUARTER(EVENT_TIME)      AS EVENT_QUARTER,
        YEAR(EVENT_TIME)         AS EVENT_YEAR,
        CASE WHEN EVENT_TYPE = 'view'
             THEN 1 ELSE 0 END   AS IS_VIEW,
        CASE WHEN EVENT_TYPE = 'cart'
             THEN 1 ELSE 0 END   AS IS_CART,
        CASE WHEN EVENT_TYPE = 'purchase'
             THEN 1 ELSE 0 END   AS IS_PURCHASE
    FROM brand_fixed
)
SELECT * FROM cleaned
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.stg_events_clean", "profile_name": "user", "target_name": "dev"} */;

============================== f79ff79a-f50c-43b7-8b85-d6c4cb0fc2fc ==============================
-- created_at: 2026-03-10T15:44:19.386195929+00:00
-- finished_at: 2026-03-10T15:44:19.676740763+00:00
-- elapsed: 290ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2eff0-3202-6dbd-0014-5816000210fa
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-10T15:44:21.804660696+00:00
-- finished_at: 2026-03-10T15:44:22.210887295+00:00
-- elapsed: 406ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4
-- query_id: 01c2eff0-3202-6db9-0014-58160001e182
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.931600646+00:00
-- finished_at: 2026-03-10T15:44:22.236545816+00:00
-- elapsed: 304ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296
-- query_id: 01c2eff0-3202-6db9-0014-58160001e186
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.936662506+00:00
-- finished_at: 2026-03-10T15:44:22.281095816+00:00
-- elapsed: 344ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd
-- query_id: 01c2eff0-3202-6db9-0014-58160001e18e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.649649579+00:00
-- finished_at: 2026-03-10T15:44:22.325719686+00:00
-- elapsed: 676ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561
-- query_id: 01c2eff0-3202-6dc3-0014-58160002400e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.938938641+00:00
-- finished_at: 2026-03-10T15:44:22.418688252+00:00
-- elapsed: 479ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177
-- query_id: 01c2eff0-3202-6c48-0014-581600022112
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.777533944+00:00
-- finished_at: 2026-03-10T15:44:22.432601224+00:00
-- elapsed: 655ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73
-- query_id: 01c2eff0-3202-6db9-0014-58160001e17e
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.932562356+00:00
-- finished_at: 2026-03-10T15:44:22.441773030+00:00
-- elapsed: 509ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57
-- query_id: 01c2eff0-3202-6db9-0014-58160001e18a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.918899455+00:00
-- finished_at: 2026-03-10T15:44:22.452014842+00:00
-- elapsed: 533ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b
-- query_id: 01c2eff0-3202-6d9c-0014-581600023162
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TYPE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TYPE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.943151175+00:00
-- finished_at: 2026-03-10T15:44:22.462619599+00:00
-- elapsed: 519ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02
-- query_id: 01c2eff0-3202-6d9c-0014-581600023166
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_MONTH
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_MONTH is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.936427561+00:00
-- finished_at: 2026-03-10T15:44:22.523302548+00:00
-- elapsed: 586ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da
-- query_id: 01c2eff0-3202-6db6-0014-58160002010a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.939906030+00:00
-- finished_at: 2026-03-10T15:44:22.543848228+00:00
-- elapsed: 603ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a
-- query_id: 01c2eff0-3202-6db6-0014-581600020112
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.908541677+00:00
-- finished_at: 2026-03-10T15:44:22.648979690+00:00
-- elapsed: 740ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7
-- query_id: 01c2eff0-3202-6d97-0014-58160001f10a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_YEAR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.924031895+00:00
-- finished_at: 2026-03-10T15:44:22.651728266+00:00
-- elapsed: 727ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526
-- query_id: 01c2eff0-3202-6dbd-0014-58160002110a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.947942500+00:00
-- finished_at: 2026-03-10T15:44:22.697431280+00:00
-- elapsed: 749ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e
-- query_id: 01c2eff0-3202-6dbd-0014-58160002110e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.948298429+00:00
-- finished_at: 2026-03-10T15:44:22.704569700+00:00
-- elapsed: 756ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62
-- query_id: 01c2eff0-3202-6db6-0014-58160002010e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.903273380+00:00
-- finished_at: 2026-03-10T15:44:22.717454737+00:00
-- elapsed: 814ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4
-- query_id: 01c2eff0-3202-6dbd-0014-5816000210fe
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_QUARTER
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_QUARTER is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.934723320+00:00
-- finished_at: 2026-03-10T15:44:22.728534259+00:00
-- elapsed: 793ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70
-- query_id: 01c2eff0-3202-6dbd-0014-581600021106
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.798068068+00:00
-- finished_at: 2026-03-10T15:44:22.733513891+00:00
-- elapsed: 935ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380
-- query_id: 01c2eff0-3202-6dbd-0014-581600021112
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.913946028+00:00
-- finished_at: 2026-03-10T15:44:22.752255604+00:00
-- elapsed: 838ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a
-- query_id: 01c2eff0-3202-6dbd-0014-581600021116
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:22.001260017+00:00
-- finished_at: 2026-03-10T15:44:22.807624050+00:00
-- elapsed: 806ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5
-- query_id: 01c2eff0-3202-6dbd-0014-581600021102
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:22.472737951+00:00
-- finished_at: 2026-03-10T15:44:22.823644442+00:00
-- elapsed: 350ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43
-- query_id: 01c2eff0-3202-6d97-0014-58160001f10e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.941615508+00:00
-- finished_at: 2026-03-10T15:44:22.833568196+00:00
-- elapsed: 891ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187
-- query_id: 01c2eff0-3202-6db6-0014-581600020116
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:44:21.934490223+00:00
-- finished_at: 2026-03-10T15:44:22.850695507+00:00
-- elapsed: 916ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c
-- query_id: 01c2eff0-3202-6d97-0014-58160001f106
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c", "profile_name": "user", "target_name": "dev"} */;

============================== d1fb564c-e0d1-4de0-bcc6-1be6672938ca ==============================
-- created_at: 2026-03-10T15:51:34.211667774+00:00
-- finished_at: 2026-03-10T15:51:34.510170377+00:00
-- elapsed: 298ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2eff7-3202-6dbd-0014-58160002111a
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-10T15:51:36.456080741+00:00
-- finished_at: 2026-03-10T15:51:36.755870659+00:00
-- elapsed: 299ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4
-- query_id: 01c2eff7-3202-6d97-0014-58160001f112
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_QUARTER
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_QUARTER is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.445685570+00:00
-- finished_at: 2026-03-10T15:51:36.771071966+00:00
-- elapsed: 325ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_DAY_OF_WEEK.890fa98e34
-- query_id: 01c2eff7-3202-6dbd-0014-58160002111e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_OF_WEEK
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where DAY_OF_WEEK is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_DAY_OF_WEEK.890fa98e34", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.466496342+00:00
-- finished_at: 2026-03-10T15:51:36.778178539+00:00
-- elapsed: 311ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_EVENT_QUARTER__1__2__3__4.74a0370526
-- query_id: 01c2eff7-3202-6dbd-0014-581600021122
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_EVENT_QUARTER__1__2__3__4.74a0370526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.487257521+00:00
-- finished_at: 2026-03-10T15:51:36.805544223+00:00
-- elapsed: 318ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e
-- query_id: 01c2eff7-3202-6dbd-0014-581600021126
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.641391804+00:00
-- finished_at: 2026-03-10T15:51:36.927493242+00:00
-- elapsed: 286ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187
-- query_id: 01c2eff7-3202-6d97-0014-58160001f116
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.647262147+00:00
-- finished_at: 2026-03-10T15:51:36.933562995+00:00
-- elapsed: 286ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a
-- query_id: 01c2eff7-3202-6dbd-0014-58160002112a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.669481039+00:00
-- finished_at: 2026-03-10T15:51:36.945933731+00:00
-- elapsed: 276ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73
-- query_id: 01c2eff7-3202-6db6-0014-58160002011a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.670927857+00:00
-- finished_at: 2026-03-10T15:51:36.947566859+00:00
-- elapsed: 276ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5
-- query_id: 01c2eff7-3202-6db6-0014-58160002011e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.669809804+00:00
-- finished_at: 2026-03-10T15:51:36.949666811+00:00
-- elapsed: 279ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02
-- query_id: 01c2eff7-3202-6db6-0014-581600020122
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_MONTH
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_MONTH is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.672414206+00:00
-- finished_at: 2026-03-10T15:51:36.953742490+00:00
-- elapsed: 281ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57
-- query_id: 01c2eff7-3202-6c48-0014-581600022122
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.649719161+00:00
-- finished_at: 2026-03-10T15:51:36.970163994+00:00
-- elapsed: 320ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7
-- query_id: 01c2eff7-3202-6c48-0014-581600022116
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_YEAR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.664323967+00:00
-- finished_at: 2026-03-10T15:51:36.972247593+00:00
-- elapsed: 307ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd
-- query_id: 01c2eff7-3202-6c48-0014-58160002211e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.689362485+00:00
-- finished_at: 2026-03-10T15:51:36.973092144+00:00
-- elapsed: 283ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70
-- query_id: 01c2eff7-3202-6dc3-0014-581600024012
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.692434348+00:00
-- finished_at: 2026-03-10T15:51:36.985177094+00:00
-- elapsed: 292ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43
-- query_id: 01c2eff7-3202-6c48-0014-581600022126
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.719036930+00:00
-- finished_at: 2026-03-10T15:51:36.994072241+00:00
-- elapsed: 275ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296
-- query_id: 01c2eff7-3202-6d9c-0014-581600023182
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.681537479+00:00
-- finished_at: 2026-03-10T15:51:37.000464294+00:00
-- elapsed: 318ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526
-- query_id: 01c2eff7-3202-6d9c-0014-58160002317e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.722661547+00:00
-- finished_at: 2026-03-10T15:51:37.009365006+00:00
-- elapsed: 286ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62
-- query_id: 01c2eff7-3202-6d97-0014-58160001f12a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.708806309+00:00
-- finished_at: 2026-03-10T15:51:37.012129641+00:00
-- elapsed: 303ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380
-- query_id: 01c2eff7-3202-6d97-0014-58160001f126
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.719029261+00:00
-- finished_at: 2026-03-10T15:51:37.012965042+00:00
-- elapsed: 293ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a
-- query_id: 01c2eff7-3202-6d9c-0014-58160002318a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.709230024+00:00
-- finished_at: 2026-03-10T15:51:37.012963593+00:00
-- elapsed: 303ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561
-- query_id: 01c2eff7-3202-6d97-0014-58160001f122
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.724789935+00:00
-- finished_at: 2026-03-10T15:51:37.013837205+00:00
-- elapsed: 289ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da
-- query_id: 01c2eff7-3202-6db9-0014-58160001e19a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.729770370+00:00
-- finished_at: 2026-03-10T15:51:37.016445149+00:00
-- elapsed: 286ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177
-- query_id: 01c2eff7-3202-6dbd-0014-581600021132
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.642185369+00:00
-- finished_at: 2026-03-10T15:51:37.019854876+00:00
-- elapsed: 377ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_HOUR.76ec27905a
-- query_id: 01c2eff7-3202-6c48-0014-58160002211a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_HOUR
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_HOUR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_HOUR.76ec27905a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.721268689+00:00
-- finished_at: 2026-03-10T15:51:37.023970115+00:00
-- elapsed: 302ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4
-- query_id: 01c2eff7-3202-6db9-0014-58160001e196
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.697728290+00:00
-- finished_at: 2026-03-10T15:51:37.024758042+00:00
-- elapsed: 327ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c
-- query_id: 01c2eff7-3202-6db9-0014-58160001e192
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.675186180+00:00
-- finished_at: 2026-03-10T15:51:37.049314284+00:00
-- elapsed: 374ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_DAY_NAME.9dd925a4e2
-- query_id: 01c2eff7-3202-6d97-0014-58160001f11e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_NAME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where DAY_NAME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_DAY_NAME.9dd925a4e2", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.675689786+00:00
-- finished_at: 2026-03-10T15:51:37.066041961+00:00
-- elapsed: 390ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_ee9c969df76ee41259a90537e9c9d129.543a8ae63b
-- query_id: 01c2eff7-3202-6d97-0014-58160001f11a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_MONTH as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by EVENT_MONTH

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6','7','8','9','10','11','12'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_ee9c969df76ee41259a90537e9c9d129.543a8ae63b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.784778816+00:00
-- finished_at: 2026-03-10T15:51:37.069063362+00:00
-- elapsed: 284ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b
-- query_id: 01c2eff7-3202-6d97-0014-58160001f12e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TYPE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TYPE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.658700647+00:00
-- finished_at: 2026-03-10T15:51:37.128120138+00:00
-- elapsed: 469ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_0a4a62d43b3c5bb84f106667b523ab53.7686e7eae2
-- query_id: 01c2eff7-3202-6dbd-0014-58160002112e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_OF_WEEK as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by DAY_OF_WEEK

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_0a4a62d43b3c5bb84f106667b523ab53.7686e7eae2", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.720164577+00:00
-- finished_at: 2026-03-10T15:51:37.183878631+00:00
-- elapsed: 463ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_DATE.728f2410bc
-- query_id: 01c2eff7-3202-6d9c-0014-581600023186
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_DATE.728f2410bc", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.701580557+00:00
-- finished_at: 2026-03-10T15:51:37.201106445+00:00
-- elapsed: 499ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_674a138e330566a305c49db295b6b2e8.9baf28cdf8
-- query_id: 01c2eff7-3202-6db6-0014-58160002012a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_HOUR as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by EVENT_HOUR

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_674a138e330566a305c49db295b6b2e8.9baf28cdf8", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:51:36.690608369+00:00
-- finished_at: 2026-03-10T15:51:37.386001014+00:00
-- elapsed: 695ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_206d1dd4470d87c187a8c7e68f7cc25a.c42471ca4a
-- query_id: 01c2eff7-3202-6db6-0014-581600020126
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_206d1dd4470d87c187a8c7e68f7cc25a.c42471ca4a", "profile_name": "user", "target_name": "dev"} */;

============================== 208e4b17-7ea1-4dfa-b544-5a51fb4a8e98 ==============================
-- created_at: 2026-03-10T15:52:19.801942327+00:00
-- finished_at: 2026-03-10T15:52:20.067864894+00:00
-- elapsed: 265ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2eff8-3202-6d97-0014-58160001f132
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-10T15:52:22.225564086+00:00
-- finished_at: 2026-03-10T15:52:22.506506222+00:00
-- elapsed: 280ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2eff8-3202-6c48-0014-58160002212a
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:23.808531857+00:00
-- finished_at: 2026-03-10T15:52:24.089469383+00:00
-- elapsed: 280ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2eff8-3202-6dbd-0014-581600021136
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T15:52:23.804205656+00:00
-- finished_at: 2026-03-10T15:52:24.121938673+00:00
-- elapsed: 317ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2eff8-3202-6db9-0014-58160001e1a2
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T15:52:23.807298408+00:00
-- finished_at: 2026-03-10T15:52:24.129464846+00:00
-- elapsed: 322ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2eff8-3202-6db9-0014-58160001e19e
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T15:52:24.124225698+00:00
-- finished_at: 2026-03-10T15:52:26.594314406+00:00
-- elapsed: 2.5s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2eff8-3202-6d9c-0014-58160002318e
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    
    
    
    as (

SELECT DISTINCT
    EVENT_DATE,
    DAY_NAME,
    DAY_OF_WEEK,
    IS_WEEKEND,
    MONTH(EVENT_DATE)                                        AS MONTH,
    QUARTER(EVENT_DATE)                                      AS QUARTER,
    YEAR(EVENT_DATE)                                         AS YEAR
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
ORDER BY EVENT_DATE
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_dates", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:24.131625339+00:00
-- finished_at: 2026-03-10T15:52:29.834770289+00:00
-- elapsed: 5.7s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2eff8-3202-6db6-0014-58160002012e
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_products
    
    
    
    as (

SELECT DISTINCT
    PRODUCT_ID,
    CATEGORY_ID,
    CATEGORY_CODE,
    CATEGORY_L1,
    CATEGORY_LEAF,
    BRAND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_products", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:24.092128342+00:00
-- finished_at: 2026-03-10T15:52:38.956457385+00:00
-- elapsed: 14.9s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2eff8-3202-6d97-0014-58160001f136
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    
    
    
    as (

SELECT
    EVENT_TIME,
    EVENT_DATE,
    EVENT_HOUR,
    PRODUCT_ID,
    USER_ID,
    USER_SESSION,
    PRICE,
    IS_VIEW,
    IS_CART,
    IS_PURCHASE,
    IS_WEEKEND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.fct_events", "profile_name": "user", "target_name": "dev"} */;

============================== 69588618-6c87-44b5-be72-ed0f840aba4e ==============================
-- created_at: 2026-03-10T15:52:50.182287+00:00
-- finished_at: 2026-03-10T15:52:50.450401519+00:00
-- elapsed: 268ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2eff8-3202-6d9c-0014-581600023192
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."DIM_DATES";
-- created_at: 2026-03-10T15:52:50.238023528+00:00
-- finished_at: 2026-03-10T15:52:50.523028176+00:00
-- elapsed: 285ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2eff8-3202-6db6-0014-581600020132
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."DIM_PRODUCTS";
-- created_at: 2026-03-10T15:52:50.457917931+00:00
-- finished_at: 2026-03-10T15:52:50.729527672+00:00
-- elapsed: 271ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2eff8-3202-6dbd-0014-58160002113a
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."FCT_EVENTS";
-- created_at: 2026-03-10T15:52:52.438151986+00:00
-- finished_at: 2026-03-10T15:52:52.797979305+00:00
-- elapsed: 359ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_CODE.0d8dad3555
-- query_id: 01c2eff8-3202-6db6-0014-58160002013a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_CODE.0d8dad3555", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.419387335+00:00
-- finished_at: 2026-03-10T15:52:52.829282760+00:00
-- elapsed: 409ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_IS_WEEKEND.c97a7435a9
-- query_id: 01c2eff8-3202-6db6-0014-581600020136
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_IS_WEEKEND.c97a7435a9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.491502424+00:00
-- finished_at: 2026-03-10T15:52:52.850320163+00:00
-- elapsed: 358ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_DAY_OF_WEEK__0__1__2__3__4__5__6.7efae6f4dd
-- query_id: 01c2eff8-3202-6db6-0014-58160002013e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_OF_WEEK as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by DAY_OF_WEEK

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_DAY_OF_WEEK__0__1__2__3__4__5__6.7efae6f4dd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.526152055+00:00
-- finished_at: 2026-03-10T15:52:52.927861290+00:00
-- elapsed: 401ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_IS_WEEKEND__true__false.8dd2703a3a
-- query_id: 01c2eff8-3202-6d9c-0014-581600023196
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_WEEKEND as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by IS_WEEKEND

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_IS_WEEKEND__true__false.8dd2703a3a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.619108912+00:00
-- finished_at: 2026-03-10T15:52:52.939312738+00:00
-- elapsed: 320ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_USER_ID.60ee434024
-- query_id: 01c2eff8-3202-6d9c-0014-5816000231a6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_USER_ID.60ee434024", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.556012209+00:00
-- finished_at: 2026-03-10T15:52:52.965228271+00:00
-- elapsed: 409ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_TIME.6fb849a6a6
-- query_id: 01c2eff8-3202-6d9c-0014-5816000231a2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_TIME.6fb849a6a6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.538047607+00:00
-- finished_at: 2026-03-10T15:52:52.970176106+00:00
-- elapsed: 432ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_ID.579c7768dd
-- query_id: 01c2eff8-3202-6d9c-0014-58160002319e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_ID
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_ID.579c7768dd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.518088515+00:00
-- finished_at: 2026-03-10T15:52:52.970979475+00:00
-- elapsed: 452ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_PRODUCT_ID.1f28c486d7
-- query_id: 01c2eff8-3202-6c48-0014-58160002212e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_PRODUCT_ID.1f28c486d7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.698618237+00:00
-- finished_at: 2026-03-10T15:52:53.008206713+00:00
-- elapsed: 309ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_USER_SESSION.19d896670e
-- query_id: 01c2eff8-3202-6dc3-0014-58160002401a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_USER_SESSION.19d896670e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.719553702+00:00
-- finished_at: 2026-03-10T15:52:53.103504149+00:00
-- elapsed: 383ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_QUARTER__1__2__3__4.4182f7a3b9
-- query_id: 01c2eff8-3202-6c48-0014-581600022146
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        QUARTER as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by QUARTER

)

select *
from all_values
where value_field not in (
    '1','2','3','4'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_QUARTER__1__2__3__4.4182f7a3b9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.747611091+00:00
-- finished_at: 2026-03-10T15:52:53.106652092+00:00
-- elapsed: 359ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_DAY_OF_WEEK.11e0aa6c4d
-- query_id: 01c2eff8-3202-6db6-0014-581600020142
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_OF_WEEK
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where DAY_OF_WEEK is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_DAY_OF_WEEK.11e0aa6c4d", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.753071614+00:00
-- finished_at: 2026-03-10T15:52:53.107573191+00:00
-- elapsed: 354ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_BRAND.a5ec5821ff
-- query_id: 01c2eff8-3202-6c48-0014-58160002214a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_BRAND.a5ec5821ff", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.698543875+00:00
-- finished_at: 2026-03-10T15:52:53.109524741+00:00
-- elapsed: 410ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_QUARTER.6c5112ec42
-- query_id: 01c2eff8-3202-6c48-0014-58160002213e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select QUARTER
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where QUARTER is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_QUARTER.6c5112ec42", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.690738122+00:00
-- finished_at: 2026-03-10T15:52:53.131388448+00:00
-- elapsed: 440ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_PRICE.feae84d4e9
-- query_id: 01c2eff8-3202-6dbd-0014-581600021142
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_PRICE.feae84d4e9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.702403323+00:00
-- finished_at: 2026-03-10T15:52:53.134047867+00:00
-- elapsed: 431ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_fct_events_PRICE___0.437bc2a8d4
-- query_id: 01c2eff8-3202-6c48-0014-581600022142
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_fct_events_PRICE___0.437bc2a8d4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.672572321+00:00
-- finished_at: 2026-03-10T15:52:53.136035036+00:00
-- elapsed: 463ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_WEEKEND.929546128f
-- query_id: 01c2eff8-3202-6c48-0014-581600022136
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_WEEKEND.929546128f", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.682257799+00:00
-- finished_at: 2026-03-10T15:52:53.140443885+00:00
-- elapsed: 458ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_VIEW.5f15874792
-- query_id: 01c2eff8-3202-6c48-0014-58160002213a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_VIEW.5f15874792", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.735513602+00:00
-- finished_at: 2026-03-10T15:52:53.142531276+00:00
-- elapsed: 407ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_PURCHASE.b9b980b060
-- query_id: 01c2eff8-3202-6dbd-0014-581600021146
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_PURCHASE.b9b980b060", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.665242661+00:00
-- finished_at: 2026-03-10T15:52:53.148242834+00:00
-- elapsed: 483ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_251de65f5724bcf7d23364b11dc72fed.66c9ba7fe7
-- query_id: 01c2eff8-3202-6c48-0014-581600022132
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_NAME as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by DAY_NAME

)

select *
from all_values
where value_field not in (
    'Mon','Tue','Wed','Thu','Fri','Sat','Sun'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_251de65f5724bcf7d23364b11dc72fed.66c9ba7fe7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.732122439+00:00
-- finished_at: 2026-03-10T15:52:53.264866375+00:00
-- elapsed: 532ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_L1.5043e574fa
-- query_id: 01c2eff8-3202-6dbd-0014-58160002114a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_L1.5043e574fa", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.746696846+00:00
-- finished_at: 2026-03-10T15:52:53.314662674+00:00
-- elapsed: 567ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.unique_dim_dates_EVENT_DATE.46b102f080
-- query_id: 01c2eff8-3202-6c48-0014-58160002214e
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.unique_dim_dates_EVENT_DATE.46b102f080", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.693713906+00:00
-- finished_at: 2026-03-10T15:52:53.318731437+00:00
-- elapsed: 625ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_PRODUCT_ID.de5c71a3af
-- query_id: 01c2eff8-3202-6db9-0014-58160001e1a6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_PRODUCT_ID.de5c71a3af", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.748854581+00:00
-- finished_at: 2026-03-10T15:52:53.393846412+00:00
-- elapsed: 644ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_DAY_NAME.437c012c02
-- query_id: 01c2eff8-3202-6dc3-0014-58160002401e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_NAME
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where DAY_NAME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_DAY_NAME.437c012c02", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.706613877+00:00
-- finished_at: 2026-03-10T15:52:53.407386775+00:00
-- elapsed: 700ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_LEAF.060e9e5228
-- query_id: 01c2eff8-3202-6db9-0014-58160001e1ae
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_LEAF.060e9e5228", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.702973598+00:00
-- finished_at: 2026-03-10T15:52:53.430014999+00:00
-- elapsed: 727ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_CART.c05a32eb8a
-- query_id: 01c2eff8-3202-6db9-0014-58160001e1aa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_CART.c05a32eb8a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.724035939+00:00
-- finished_at: 2026-03-10T15:52:53.454338395+00:00
-- elapsed: 730ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_HOUR.6ef38f0fd6
-- query_id: 01c2eff8-3202-6d97-0014-58160001f142
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_HOUR
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_HOUR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_HOUR.6ef38f0fd6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.519867570+00:00
-- finished_at: 2026-03-10T15:52:53.506119332+00:00
-- elapsed: 986ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.relationships_fct_events_EVENT_DATE__EVENT_DATE__ref_dim_dates_.3f7b055ece
-- query_id: 01c2eff8-3202-6d9c-0014-58160002319a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.relationships_fct_events_EVENT_DATE__EVENT_DATE__ref_dim_dates_.3f7b055ece", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.731416906+00:00
-- finished_at: 2026-03-10T15:52:53.631860310+00:00
-- elapsed: 900ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_da1fae6cd48ffa25502c8c26b839eb18.b4192735d6
-- query_id: 01c2eff8-3202-6db9-0014-58160001e1b6
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_da1fae6cd48ffa25502c8c26b839eb18.b4192735d6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.751105500+00:00
-- finished_at: 2026-03-10T15:52:53.633464813+00:00
-- elapsed: 882ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_PURCHASE__0__1.692b30d4c4
-- query_id: 01c2eff8-3202-6dbd-0014-58160002114e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_PURCHASE__0__1.692b30d4c4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.535091513+00:00
-- finished_at: 2026-03-10T15:52:53.672894500+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.relationships_fct_events_14461f4a74831fe72206b0fc8a0392ab.bb7991c1fa
-- query_id: 01c2eff8-3202-6dc3-0014-581600024016
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select PRODUCT_ID as from_field
    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    where PRODUCT_ID is not null
),

parent as (
    select PRODUCT_ID as to_field
    from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.relationships_fct_events_14461f4a74831fe72206b0fc8a0392ab.bb7991c1fa", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.830604295+00:00
-- finished_at: 2026-03-10T15:52:53.704941079+00:00
-- elapsed: 874ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.unique_dim_products_PRODUCT_ID.5b3502e45d
-- query_id: 01c2eff8-3202-6c48-0014-581600022152
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    PRODUCT_ID as unique_field,
    count(*) as n_records

from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where PRODUCT_ID is not null
group by PRODUCT_ID
having count(*) > 1



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.unique_dim_products_PRODUCT_ID.5b3502e45d", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.684576182+00:00
-- finished_at: 2026-03-10T15:52:53.714103452+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_VIEW__0__1.5fe37935f3
-- query_id: 01c2eff8-3202-6dbd-0014-58160002113e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_VIEW__0__1.5fe37935f3", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.703987219+00:00
-- finished_at: 2026-03-10T15:52:53.732185234+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_WEEKEND__true__false.b010ff5ce4
-- query_id: 01c2eff8-3202-6db9-0014-58160001e1b2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_WEEKEND as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_WEEKEND

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_WEEKEND__true__false.b010ff5ce4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.818080867+00:00
-- finished_at: 2026-03-10T15:52:53.737546195+00:00
-- elapsed: 919ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_MONTH.371af8f104
-- query_id: 01c2eff8-3202-6d97-0014-58160001f146
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select MONTH
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where MONTH is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_MONTH.371af8f104", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.862626355+00:00
-- finished_at: 2026-03-10T15:52:53.739042532+00:00
-- elapsed: 876ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_EVENT_DATE.4e052aed85
-- query_id: 01c2eff8-3202-6d97-0014-58160001f14a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_EVENT_DATE.4e052aed85", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.772210747+00:00
-- finished_at: 2026-03-10T15:52:53.757605574+00:00
-- elapsed: 985ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_DATE.db2e1e2234
-- query_id: 01c2eff8-3202-6d9c-0014-5816000231aa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_DATE.db2e1e2234", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.686061962+00:00
-- finished_at: 2026-03-10T15:52:53.759701195+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_CART__0__1.5c40c41c0c
-- query_id: 01c2eff8-3202-6d97-0014-58160001f13a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_CART as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_CART

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_CART__0__1.5c40c41c0c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.697996188+00:00
-- finished_at: 2026-03-10T15:52:53.761503438+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_789f24d42f36ec2f925c6d85d0999705.663b6a250f
-- query_id: 01c2eff8-3202-6d97-0014-58160001f13e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_HOUR as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by EVENT_HOUR

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_789f24d42f36ec2f925c6d85d0999705.663b6a250f", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:52:52.881867224+00:00
-- finished_at: 2026-03-10T15:52:53.773991326+00:00
-- elapsed: 892ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_YEAR.6998c47062
-- query_id: 01c2eff8-3202-6d97-0014-58160001f14e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where YEAR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_YEAR.6998c47062", "profile_name": "user", "target_name": "dev"} */;

============================== 91d1f888-7923-46ff-b568-59d6dac96043 ==============================
-- created_at: 2026-03-10T15:54:39.524144175+00:00
-- finished_at: 2026-03-10T15:54:39.845317743+00:00
-- elapsed: 321ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2effa-3202-6d97-0014-58160001f152
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."RAW"."EVENTS_RAW";
-- created_at: 2026-03-10T15:54:41.915375807+00:00
-- finished_at: 2026-03-10T15:54:42.210128529+00:00
-- elapsed: 294ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2effa-3202-6dbd-0014-581600021152
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:54:43.437694422+00:00
-- finished_at: 2026-03-10T15:54:43.761115477+00:00
-- elapsed: 323ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2effa-3202-6d97-0014-58160001f156
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T15:54:43.764363202+00:00
-- finished_at: 2026-03-10T15:55:15.228279352+00:00
-- elapsed: 31.5s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2effa-3202-6db6-0014-581600020146
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    
    
    
    as (

WITH source AS (
    SELECT * FROM ECOMMERCE_ANALYTICS.RAW.EVENTS_RAW
),
brand_fixed AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        COALESCE(
            BRAND,
            FIRST_VALUE(BRAND) IGNORE NULLS OVER (
                PARTITION BY PRODUCT_ID
                ORDER BY EVENT_TIME
            ),
            'unknown'
        )                        AS BRAND,
        COALESCE(
            CATEGORY_CODE, 
            'unknown'
        )                        AS CATEGORY_CODE
    FROM source
    WHERE USER_SESSION IS NOT NULL
),
cleaned AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        BRAND,
        CATEGORY_CODE,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.', 1), ''),
            'unknown')           AS CATEGORY_L1,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.',
                ARRAY_SIZE(SPLIT(CATEGORY_CODE, '.'))), ''),
            'unknown')           AS CATEGORY_LEAF,
        DATE(EVENT_TIME)         AS EVENT_DATE,
        HOUR(EVENT_TIME)         AS EVENT_HOUR,
        DAYNAME(EVENT_TIME)      AS DAY_NAME,
        DAYOFWEEK(EVENT_TIME)    AS DAY_OF_WEEK,
        CASE WHEN DAYOFWEEK(EVENT_TIME)
             IN (0, 6) THEN TRUE
             ELSE FALSE END      AS IS_WEEKEND,
        MONTH(EVENT_TIME)        AS EVENT_MONTH,
        QUARTER(EVENT_TIME)      AS EVENT_QUARTER,
        YEAR(EVENT_TIME)         AS EVENT_YEAR,
        CASE WHEN EVENT_TYPE = 'view'
             THEN 1 ELSE 0 END   AS IS_VIEW,
        CASE WHEN EVENT_TYPE = 'cart'
             THEN 1 ELSE 0 END   AS IS_CART,
        CASE WHEN EVENT_TYPE = 'purchase'
             THEN 1 ELSE 0 END   AS IS_PURCHASE
    FROM brand_fixed
)
SELECT * FROM cleaned
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.stg_events_clean", "profile_name": "user", "target_name": "dev"} */;

============================== b213effa-432f-4bbc-9550-81d4fdc98245 ==============================
-- created_at: 2026-03-10T15:55:30.115116037+00:00
-- finished_at: 2026-03-10T15:55:30.438229012+00:00
-- elapsed: 323ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2effb-3202-6db9-0014-58160001e1ba
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-10T15:55:32.365512631+00:00
-- finished_at: 2026-03-10T15:55:32.766563354+00:00
-- elapsed: 401ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e
-- query_id: 01c2effb-3202-6db9-0014-58160001e1c2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.544997071+00:00
-- finished_at: 2026-03-10T15:55:32.925453274+00:00
-- elapsed: 380ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296
-- query_id: 01c2effb-3202-6db9-0014-58160001e1ca
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.365551853+00:00
-- finished_at: 2026-03-10T15:55:32.950505852+00:00
-- elapsed: 584ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57
-- query_id: 01c2effb-3202-6db9-0014-58160001e1c6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.606781903+00:00
-- finished_at: 2026-03-10T15:55:33.034055007+00:00
-- elapsed: 427ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_HOUR.76ec27905a
-- query_id: 01c2effb-3202-6db9-0014-58160001e1ce
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_HOUR
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_HOUR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_HOUR.76ec27905a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.542886468+00:00
-- finished_at: 2026-03-10T15:55:33.051980062+00:00
-- elapsed: 509ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70
-- query_id: 01c2effb-3202-6d97-0014-58160001f15a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.622764540+00:00
-- finished_at: 2026-03-10T15:55:33.115639503+00:00
-- elapsed: 492ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380
-- query_id: 01c2effb-3202-6dc3-0014-58160002402a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.572686912+00:00
-- finished_at: 2026-03-10T15:55:33.120374998+00:00
-- elapsed: 547ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a
-- query_id: 01c2effb-3202-6dc3-0014-581600024022
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.606876183+00:00
-- finished_at: 2026-03-10T15:55:33.122310007+00:00
-- elapsed: 515ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_EVENT_QUARTER__1__2__3__4.74a0370526
-- query_id: 01c2effb-3202-6d97-0014-58160001f15e
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_EVENT_QUARTER__1__2__3__4.74a0370526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.666320577+00:00
-- finished_at: 2026-03-10T15:55:33.211121423+00:00
-- elapsed: 544ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4
-- query_id: 01c2effb-3202-6dc3-0014-58160002402e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.548636082+00:00
-- finished_at: 2026-03-10T15:55:33.226211819+00:00
-- elapsed: 677ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561
-- query_id: 01c2effb-3202-6d9c-0014-5816000231ae
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.623295339+00:00
-- finished_at: 2026-03-10T15:55:33.229177529+00:00
-- elapsed: 605ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c
-- query_id: 01c2effb-3202-6db9-0014-58160001e1d2
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.593088406+00:00
-- finished_at: 2026-03-10T15:55:33.241689035+00:00
-- elapsed: 648ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da
-- query_id: 01c2effb-3202-6dc3-0014-581600024026
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.656214737+00:00
-- finished_at: 2026-03-10T15:55:33.257355606+00:00
-- elapsed: 601ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_ee9c969df76ee41259a90537e9c9d129.543a8ae63b
-- query_id: 01c2effb-3202-6d9c-0014-5816000231ba
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_MONTH as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by EVENT_MONTH

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6','7','8','9','10','11','12'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_ee9c969df76ee41259a90537e9c9d129.543a8ae63b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.639519589+00:00
-- finished_at: 2026-03-10T15:55:33.259161115+00:00
-- elapsed: 619ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_DATE.728f2410bc
-- query_id: 01c2effb-3202-6d9c-0014-5816000231b6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_DATE.728f2410bc", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.658080421+00:00
-- finished_at: 2026-03-10T15:55:33.308505381+00:00
-- elapsed: 650ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_0a4a62d43b3c5bb84f106667b523ab53.7686e7eae2
-- query_id: 01c2effb-3202-6d97-0014-58160001f162
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_OF_WEEK as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by DAY_OF_WEEK

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_0a4a62d43b3c5bb84f106667b523ab53.7686e7eae2", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.629275346+00:00
-- finished_at: 2026-03-10T15:55:33.354023302+00:00
-- elapsed: 724ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526
-- query_id: 01c2effb-3202-6db6-0014-58160002014a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.621938910+00:00
-- finished_at: 2026-03-10T15:55:33.393306307+00:00
-- elapsed: 771ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7
-- query_id: 01c2effb-3202-6db6-0014-58160002014e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_YEAR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.365781490+00:00
-- finished_at: 2026-03-10T15:55:33.417015153+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_206d1dd4470d87c187a8c7e68f7cc25a.c42471ca4a
-- query_id: 01c2effb-3202-6db9-0014-58160001e1be
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_206d1dd4470d87c187a8c7e68f7cc25a.c42471ca4a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.623984469+00:00
-- finished_at: 2026-03-10T15:55:33.451125649+00:00
-- elapsed: 827ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd
-- query_id: 01c2effb-3202-6c48-0014-581600022162
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.580473768+00:00
-- finished_at: 2026-03-10T15:55:33.451896153+00:00
-- elapsed: 871ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4
-- query_id: 01c2effb-3202-6c48-0014-581600022156
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_QUARTER
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_QUARTER is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.614765571+00:00
-- finished_at: 2026-03-10T15:55:33.466248887+00:00
-- elapsed: 851ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187
-- query_id: 01c2effb-3202-6d9c-0014-5816000231b2
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.621631742+00:00
-- finished_at: 2026-03-10T15:55:33.472201090+00:00
-- elapsed: 850ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43
-- query_id: 01c2effb-3202-6c48-0014-58160002215e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.658185344+00:00
-- finished_at: 2026-03-10T15:55:33.484298518+00:00
-- elapsed: 826ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177
-- query_id: 01c2effb-3202-6c48-0014-581600022166
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.727081983+00:00
-- finished_at: 2026-03-10T15:55:33.507920293+00:00
-- elapsed: 780ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5
-- query_id: 01c2effb-3202-6d9c-0014-5816000231c2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.745867720+00:00
-- finished_at: 2026-03-10T15:55:33.660918747+00:00
-- elapsed: 915ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b
-- query_id: 01c2effb-3202-6d97-0014-58160001f166
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TYPE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TYPE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.636059219+00:00
-- finished_at: 2026-03-10T15:55:33.697249382+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_674a138e330566a305c49db295b6b2e8.9baf28cdf8
-- query_id: 01c2effb-3202-6db6-0014-581600020152
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_HOUR as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by EVENT_HOUR

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_674a138e330566a305c49db295b6b2e8.9baf28cdf8", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.729636260+00:00
-- finished_at: 2026-03-10T15:55:33.709109287+00:00
-- elapsed: 979ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73
-- query_id: 01c2effb-3202-6d9c-0014-5816000231be
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.627650573+00:00
-- finished_at: 2026-03-10T15:55:33.709873923+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62
-- query_id: 01c2effb-3202-6c48-0014-58160002215a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.656544175+00:00
-- finished_at: 2026-03-10T15:55:34.395344581+00:00
-- elapsed: 1.7s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02
-- query_id: 01c2effb-3202-6dbd-0014-58160002115a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_MONTH
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_MONTH is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.660805880+00:00
-- finished_at: 2026-03-10T15:55:34.411956160+00:00
-- elapsed: 1.8s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_DAY_OF_WEEK.890fa98e34
-- query_id: 01c2effb-3202-6dbd-0014-58160002115e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_OF_WEEK
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where DAY_OF_WEEK is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_DAY_OF_WEEK.890fa98e34", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.682993154+00:00
-- finished_at: 2026-03-10T15:55:34.425217635+00:00
-- elapsed: 1.7s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_DAY_NAME.9dd925a4e2
-- query_id: 01c2effb-3202-6dbd-0014-581600021162
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_NAME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where DAY_NAME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_DAY_NAME.9dd925a4e2", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:32.627591598+00:00
-- finished_at: 2026-03-10T15:55:34.426849538+00:00
-- elapsed: 1.8s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a
-- query_id: 01c2effb-3202-6dbd-0014-581600021156
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a", "profile_name": "user", "target_name": "dev"} */;

============================== 06a20a09-8c82-4abc-aa22-93c9eb5123e8 ==============================
-- created_at: 2026-03-10T15:55:54.090724610+00:00
-- finished_at: 2026-03-10T15:55:54.349786525+00:00
-- elapsed: 259ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2effb-3202-6db6-0014-581600020156
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-10T15:55:56.547342890+00:00
-- finished_at: 2026-03-10T15:55:56.843286839+00:00
-- elapsed: 295ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2effb-3202-6c48-0014-58160002216a
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:58.072391523+00:00
-- finished_at: 2026-03-10T15:55:58.337279322+00:00
-- elapsed: 264ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2effb-3202-6dc3-0014-581600024032
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T15:55:58.137811412+00:00
-- finished_at: 2026-03-10T15:55:58.470354534+00:00
-- elapsed: 332ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2effb-3202-6dbd-0014-581600021166
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T15:55:58.193430608+00:00
-- finished_at: 2026-03-10T15:55:58.493929981+00:00
-- elapsed: 300ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2effb-3202-6dc3-0014-581600024036
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T15:55:58.340005314+00:00
-- finished_at: 2026-03-10T15:56:00.628987610+00:00
-- elapsed: 2.3s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2effb-3202-6c48-0014-58160002216e
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    
    
    
    as (

SELECT DISTINCT
    EVENT_DATE,
    DAY_NAME,
    DAY_OF_WEEK,
    IS_WEEKEND,
    MONTH(EVENT_DATE)                                        AS MONTH,
    QUARTER(EVENT_DATE)                                      AS QUARTER,
    YEAR(EVENT_DATE)                                         AS YEAR
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
ORDER BY EVENT_DATE
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_dates", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:58.472828989+00:00
-- finished_at: 2026-03-10T15:56:04.250913316+00:00
-- elapsed: 5.8s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2effb-3202-6dc3-0014-58160002403a
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_products
    
    
    
    as (

SELECT DISTINCT
    PRODUCT_ID,
    CATEGORY_ID,
    CATEGORY_CODE,
    CATEGORY_L1,
    CATEGORY_LEAF,
    BRAND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_products", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:55:58.496386379+00:00
-- finished_at: 2026-03-10T15:56:13.863832426+00:00
-- elapsed: 15.4s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2effb-3202-6c48-0014-581600022172
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    
    
    
    as (

SELECT
    EVENT_TIME,
    EVENT_DATE,
    EVENT_HOUR,
    PRODUCT_ID,
    USER_ID,
    USER_SESSION,
    PRICE,
    IS_VIEW,
    IS_CART,
    IS_PURCHASE,
    IS_WEEKEND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.fct_events", "profile_name": "user", "target_name": "dev"} */;

============================== bbb9dfbd-0031-4c2f-80c5-506fb9449e4f ==============================
-- created_at: 2026-03-10T15:56:33.662721386+00:00
-- finished_at: 2026-03-10T15:56:33.925648779+00:00
-- elapsed: 262ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2effc-3202-6c48-0014-581600022176
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."RAW"."EVENTS_RAW";
-- created_at: 2026-03-10T15:56:36.024963575+00:00
-- finished_at: 2026-03-10T15:56:36.301567967+00:00
-- elapsed: 276ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2effc-3202-6d9c-0014-5816000231c6
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:56:37.694226726+00:00
-- finished_at: 2026-03-10T15:56:38.000326018+00:00
-- elapsed: 306ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2effc-3202-6d9c-0014-5816000231ca
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T15:56:38.003875971+00:00
-- finished_at: 2026-03-10T15:57:09.397297230+00:00
-- elapsed: 31.4s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2effc-3202-6dbd-0014-58160002116a
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    
    
    
    as (

WITH source AS (
    SELECT * FROM ECOMMERCE_ANALYTICS.RAW.EVENTS_RAW
),
brand_fixed AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        COALESCE(
            BRAND,
            FIRST_VALUE(BRAND) IGNORE NULLS OVER (
                PARTITION BY PRODUCT_ID
                ORDER BY EVENT_TIME
            ),
            'unknown'
        )                        AS BRAND,
        COALESCE(
            CATEGORY_CODE, 
            'unknown'
        )                        AS CATEGORY_CODE
    FROM source
    WHERE USER_SESSION IS NOT NULL
),
cleaned AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        BRAND,
        CATEGORY_CODE,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.', 1), ''),
            'unknown')           AS CATEGORY_L1,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.',
                ARRAY_SIZE(SPLIT(CATEGORY_CODE, '.'))), ''),
            'unknown')           AS CATEGORY_LEAF,
        DATE(EVENT_TIME)         AS EVENT_DATE,
        HOUR(EVENT_TIME)         AS EVENT_HOUR,
        DAYNAME(EVENT_TIME)      AS DAY_NAME,
        DAYOFWEEK(EVENT_TIME)    AS DAY_OF_WEEK,
        CASE WHEN DAYOFWEEK(EVENT_TIME)
             IN (0, 6) THEN TRUE
             ELSE FALSE END      AS IS_WEEKEND,
        MONTH(EVENT_TIME)        AS EVENT_MONTH,
        QUARTER(EVENT_TIME)      AS EVENT_QUARTER,
        YEAR(EVENT_TIME)         AS EVENT_YEAR,
        CASE WHEN EVENT_TYPE = 'view'
             THEN 1 ELSE 0 END   AS IS_VIEW,
        CASE WHEN EVENT_TYPE = 'cart'
             THEN 1 ELSE 0 END   AS IS_CART,
        CASE WHEN EVENT_TYPE = 'purchase'
             THEN 1 ELSE 0 END   AS IS_PURCHASE
    FROM brand_fixed
)
SELECT * FROM cleaned
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.stg_events_clean", "profile_name": "user", "target_name": "dev"} */;

============================== 09fec49a-eb7a-40f8-aca2-8a088ed9e6d5 ==============================
-- created_at: 2026-03-10T15:58:02.394118440+00:00
-- finished_at: 2026-03-10T15:58:02.669416828+00:00
-- elapsed: 275ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2effe-3202-6dbd-0014-58160002116e
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-10T15:58:04.597342638+00:00
-- finished_at: 2026-03-10T15:58:04.960260416+00:00
-- elapsed: 362ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380
-- query_id: 01c2effe-3202-6dbd-0014-581600021176
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.594044237+00:00
-- finished_at: 2026-03-10T15:58:04.967889758+00:00
-- elapsed: 373ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd
-- query_id: 01c2effe-3202-6dbd-0014-581600021172
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.644842627+00:00
-- finished_at: 2026-03-10T15:58:04.972618354+00:00
-- elapsed: 327ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43
-- query_id: 01c2effe-3202-6dbd-0014-581600021186
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.618113808+00:00
-- finished_at: 2026-03-10T15:58:05.007312772+00:00
-- elapsed: 389ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_HOUR.76ec27905a
-- query_id: 01c2effe-3202-6dbd-0014-58160002117e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_HOUR
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_HOUR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_HOUR.76ec27905a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.614675294+00:00
-- finished_at: 2026-03-10T15:58:05.010677499+00:00
-- elapsed: 396ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5
-- query_id: 01c2effe-3202-6dbd-0014-58160002117a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.622706689+00:00
-- finished_at: 2026-03-10T15:58:05.070037551+00:00
-- elapsed: 447ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_DAY_NAME.9dd925a4e2
-- query_id: 01c2effe-3202-6d97-0014-58160001f16a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_NAME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where DAY_NAME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_DAY_NAME.9dd925a4e2", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.800000966+00:00
-- finished_at: 2026-03-10T15:58:05.099752564+00:00
-- elapsed: 299ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70
-- query_id: 01c2effe-3202-6d97-0014-58160001f16e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.869463567+00:00
-- finished_at: 2026-03-10T15:58:05.175465310+00:00
-- elapsed: 306ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02
-- query_id: 01c2effe-3202-6dbd-0014-58160002118a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_MONTH
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_MONTH is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.858578764+00:00
-- finished_at: 2026-03-10T15:58:05.183268979+00:00
-- elapsed: 324ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_DAY_OF_WEEK.890fa98e34
-- query_id: 01c2effe-3202-6c48-0014-581600022192
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_OF_WEEK
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where DAY_OF_WEEK is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_DAY_OF_WEEK.890fa98e34", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.813178569+00:00
-- finished_at: 2026-03-10T15:58:05.196854563+00:00
-- elapsed: 383ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526
-- query_id: 01c2effe-3202-6c48-0014-58160002217e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.802560337+00:00
-- finished_at: 2026-03-10T15:58:05.200653769+00:00
-- elapsed: 398ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_ee9c969df76ee41259a90537e9c9d129.543a8ae63b
-- query_id: 01c2effe-3202-6c48-0014-581600022182
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_MONTH as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by EVENT_MONTH

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6','7','8','9','10','11','12'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_ee9c969df76ee41259a90537e9c9d129.543a8ae63b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.844113241+00:00
-- finished_at: 2026-03-10T15:58:05.201468711+00:00
-- elapsed: 357ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a
-- query_id: 01c2effe-3202-6c48-0014-58160002218a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.797671730+00:00
-- finished_at: 2026-03-10T15:58:05.205491202+00:00
-- elapsed: 407ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da
-- query_id: 01c2effe-3202-6db9-0014-58160001e1d6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.618605406+00:00
-- finished_at: 2026-03-10T15:58:05.215611520+00:00
-- elapsed: 597ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73
-- query_id: 01c2effe-3202-6dbd-0014-581600021182
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.878379826+00:00
-- finished_at: 2026-03-10T15:58:05.226225560+00:00
-- elapsed: 347ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4
-- query_id: 01c2effe-3202-6db6-0014-581600020162
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.841352441+00:00
-- finished_at: 2026-03-10T15:58:05.249147086+00:00
-- elapsed: 407ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561
-- query_id: 01c2effe-3202-6db6-0014-58160002015e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.900806517+00:00
-- finished_at: 2026-03-10T15:58:05.254111112+00:00
-- elapsed: 353ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4
-- query_id: 01c2effe-3202-6dbd-0014-58160002118e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_QUARTER
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_QUARTER is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.851162076+00:00
-- finished_at: 2026-03-10T15:58:05.309251062+00:00
-- elapsed: 458ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177
-- query_id: 01c2effe-3202-6dc3-0014-581600024042
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.837184632+00:00
-- finished_at: 2026-03-10T15:58:05.430378796+00:00
-- elapsed: 593ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296
-- query_id: 01c2effe-3202-6d9c-0014-5816000231d2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.842948573+00:00
-- finished_at: 2026-03-10T15:58:05.511634457+00:00
-- elapsed: 668ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_DATE.728f2410bc
-- query_id: 01c2effe-3202-6d9c-0014-5816000231d6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_DATE.728f2410bc", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.833742799+00:00
-- finished_at: 2026-03-10T15:58:05.568197572+00:00
-- elapsed: 734ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_EVENT_QUARTER__1__2__3__4.74a0370526
-- query_id: 01c2effe-3202-6d9c-0014-5816000231da
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_EVENT_QUARTER__1__2__3__4.74a0370526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.832722044+00:00
-- finished_at: 2026-03-10T15:58:05.618955507+00:00
-- elapsed: 786ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a
-- query_id: 01c2effe-3202-6d9c-0014-5816000231ce
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.771822747+00:00
-- finished_at: 2026-03-10T15:58:05.627457406+00:00
-- elapsed: 855ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c
-- query_id: 01c2effe-3202-6c48-0014-58160002217a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.889745825+00:00
-- finished_at: 2026-03-10T15:58:05.631086367+00:00
-- elapsed: 741ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_674a138e330566a305c49db295b6b2e8.9baf28cdf8
-- query_id: 01c2effe-3202-6dc3-0014-581600024046
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_HOUR as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by EVENT_HOUR

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_674a138e330566a305c49db295b6b2e8.9baf28cdf8", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.842817198+00:00
-- finished_at: 2026-03-10T15:58:05.637897951+00:00
-- elapsed: 795ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57
-- query_id: 01c2effe-3202-6db6-0014-58160002015a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.835457409+00:00
-- finished_at: 2026-03-10T15:58:05.639708621+00:00
-- elapsed: 804ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62
-- query_id: 01c2effe-3202-6c48-0014-581600022186
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.961176149+00:00
-- finished_at: 2026-03-10T15:58:05.655231976+00:00
-- elapsed: 694ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e
-- query_id: 01c2effe-3202-6c48-0014-581600022196
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.956917969+00:00
-- finished_at: 2026-03-10T15:58:05.736628367+00:00
-- elapsed: 779ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b
-- query_id: 01c2effe-3202-6db6-0014-581600020166
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TYPE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TYPE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.972803691+00:00
-- finished_at: 2026-03-10T15:58:05.779086595+00:00
-- elapsed: 806ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7
-- query_id: 01c2effe-3202-6d9c-0014-5816000231de
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_YEAR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.834911224+00:00
-- finished_at: 2026-03-10T15:58:05.867848594+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187
-- query_id: 01c2effe-3202-6dc3-0014-58160002403e
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:05.007654739+00:00
-- finished_at: 2026-03-10T15:58:05.954064336+00:00
-- elapsed: 946ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_0a4a62d43b3c5bb84f106667b523ab53.7686e7eae2
-- query_id: 01c2effe-3202-6d9c-0014-5816000231e2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_OF_WEEK as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by DAY_OF_WEEK

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_0a4a62d43b3c5bb84f106667b523ab53.7686e7eae2", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:04.844163487+00:00
-- finished_at: 2026-03-10T15:58:06.047481658+00:00
-- elapsed: 1.2s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_206d1dd4470d87c187a8c7e68f7cc25a.c42471ca4a
-- query_id: 01c2effe-3202-6c48-0014-58160002218e
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_206d1dd4470d87c187a8c7e68f7cc25a.c42471ca4a", "profile_name": "user", "target_name": "dev"} */;

============================== 34eb3493-577d-4313-aad3-cb23b7ae32fd ==============================
-- created_at: 2026-03-10T15:58:19.688183725+00:00
-- finished_at: 2026-03-10T15:58:19.976585003+00:00
-- elapsed: 288ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2effe-3202-6d9c-0014-5816000231e6
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-10T15:58:22.084834749+00:00
-- finished_at: 2026-03-10T15:58:22.356658321+00:00
-- elapsed: 271ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2effe-3202-6d97-0014-58160001f172
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:23.649257004+00:00
-- finished_at: 2026-03-10T15:58:23.943899038+00:00
-- elapsed: 294ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2effe-3202-6dc3-0014-58160002404a
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T15:58:23.649671452+00:00
-- finished_at: 2026-03-10T15:58:23.945583890+00:00
-- elapsed: 295ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2effe-3202-6dbd-0014-581600021192
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T15:58:23.662315133+00:00
-- finished_at: 2026-03-10T15:58:23.987621760+00:00
-- elapsed: 325ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2effe-3202-6d97-0014-58160001f176
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T15:58:23.947822200+00:00
-- finished_at: 2026-03-10T15:58:26.425121772+00:00
-- elapsed: 2.5s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2effe-3202-6d97-0014-58160001f17a
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    
    
    
    as (

SELECT DISTINCT
    EVENT_DATE,
    DAY_NAME,
    DAY_OF_WEEK,
    IS_WEEKEND,
    MONTH(EVENT_DATE)                                        AS MONTH,
    QUARTER(EVENT_DATE)                                      AS QUARTER,
    YEAR(EVENT_DATE)                                         AS YEAR
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
ORDER BY EVENT_DATE
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_dates", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:23.989819209+00:00
-- finished_at: 2026-03-10T15:58:29.585719199+00:00
-- elapsed: 5.6s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2effe-3202-6db6-0014-58160002016a
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_products
    
    
    
    as (

SELECT DISTINCT
    PRODUCT_ID,
    CATEGORY_ID,
    CATEGORY_CODE,
    CATEGORY_L1,
    CATEGORY_LEAF,
    BRAND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_products", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:23.947226806+00:00
-- finished_at: 2026-03-10T15:58:38.658844505+00:00
-- elapsed: 14.7s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2effe-3202-6c48-0014-58160002219a
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    
    
    
    as (

SELECT
    EVENT_TIME,
    EVENT_DATE,
    EVENT_HOUR,
    PRODUCT_ID,
    USER_ID,
    USER_SESSION,
    PRICE,
    IS_VIEW,
    IS_CART,
    IS_PURCHASE,
    IS_WEEKEND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.fct_events", "profile_name": "user", "target_name": "dev"} */;

============================== 31560082-6573-4a32-91da-4b33198ee2df ==============================
-- created_at: 2026-03-10T15:58:49.594165031+00:00
-- finished_at: 2026-03-10T15:58:49.893864396+00:00
-- elapsed: 299ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2effe-3202-6db9-0014-58160001e1da
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."DIM_DATES";
-- created_at: 2026-03-10T15:58:49.712931450+00:00
-- finished_at: 2026-03-10T15:58:50.011950387+00:00
-- elapsed: 299ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2effe-3202-6db9-0014-58160001e1de
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."DIM_PRODUCTS";
-- created_at: 2026-03-10T15:58:49.901579531+00:00
-- finished_at: 2026-03-10T15:58:50.191330553+00:00
-- elapsed: 289ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2effe-3202-6dc3-0014-58160002404e
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."FCT_EVENTS";
-- created_at: 2026-03-10T15:58:51.964869342+00:00
-- finished_at: 2026-03-10T15:58:52.344483275+00:00
-- elapsed: 379ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_ID.579c7768dd
-- query_id: 01c2effe-3202-6db9-0014-58160001e1e2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_ID
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_ID.579c7768dd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:51.974508558+00:00
-- finished_at: 2026-03-10T15:58:52.372814925+00:00
-- elapsed: 398ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_QUARTER.6c5112ec42
-- query_id: 01c2effe-3202-6db9-0014-58160001e1ea
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select QUARTER
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where QUARTER is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_QUARTER.6c5112ec42", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:51.964418772+00:00
-- finished_at: 2026-03-10T15:58:52.384225352+00:00
-- elapsed: 419ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_USER_ID.60ee434024
-- query_id: 01c2effe-3202-6db9-0014-58160001e1e6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_USER_ID.60ee434024", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:51.976928880+00:00
-- finished_at: 2026-03-10T15:58:52.397151023+00:00
-- elapsed: 420ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_WEEKEND.929546128f
-- query_id: 01c2effe-3202-6d9c-0014-5816000231ea
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_WEEKEND.929546128f", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.147719479+00:00
-- finished_at: 2026-03-10T15:58:52.454268032+00:00
-- elapsed: 306ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_DATE.db2e1e2234
-- query_id: 01c2effe-3202-6db9-0014-58160001e1ee
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_DATE.db2e1e2234", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.121088575+00:00
-- finished_at: 2026-03-10T15:58:52.494066274+00:00
-- elapsed: 372ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_PURCHASE.b9b980b060
-- query_id: 01c2effe-3202-6c48-0014-58160002219e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_PURCHASE.b9b980b060", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.137594602+00:00
-- finished_at: 2026-03-10T15:58:52.499039339+00:00
-- elapsed: 361ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_fct_events_PRICE___0.437bc2a8d4
-- query_id: 01c2effe-3202-6db6-0014-58160002016e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_fct_events_PRICE___0.437bc2a8d4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.149580957+00:00
-- finished_at: 2026-03-10T15:58:52.505038829+00:00
-- elapsed: 355ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_USER_SESSION.19d896670e
-- query_id: 01c2effe-3202-6db6-0014-581600020172
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_USER_SESSION.19d896670e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.161771531+00:00
-- finished_at: 2026-03-10T15:58:52.512474130+00:00
-- elapsed: 350ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_EVENT_DATE.4e052aed85
-- query_id: 01c2effe-3202-6dc3-0014-58160002405a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_EVENT_DATE.4e052aed85", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.225093510+00:00
-- finished_at: 2026-03-10T15:58:52.528149272+00:00
-- elapsed: 303ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_HOUR.6ef38f0fd6
-- query_id: 01c2effe-3202-6dc3-0014-58160002405e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_HOUR
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_HOUR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_HOUR.6ef38f0fd6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.170315118+00:00
-- finished_at: 2026-03-10T15:58:52.533810727+00:00
-- elapsed: 363ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_CODE.0d8dad3555
-- query_id: 01c2effe-3202-6db6-0014-581600020176
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_CODE.0d8dad3555", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.183718314+00:00
-- finished_at: 2026-03-10T15:58:52.564298735+00:00
-- elapsed: 380ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_LEAF.060e9e5228
-- query_id: 01c2effe-3202-6c48-0014-5816000221a2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_LEAF.060e9e5228", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.158542984+00:00
-- finished_at: 2026-03-10T15:58:52.567800209+00:00
-- elapsed: 409ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_CART.c05a32eb8a
-- query_id: 01c2effe-3202-6d97-0014-58160001f182
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_CART.c05a32eb8a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.137522072+00:00
-- finished_at: 2026-03-10T15:58:52.578674099+00:00
-- elapsed: 441ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_DAY_OF_WEEK__0__1__2__3__4__5__6.7efae6f4dd
-- query_id: 01c2effe-3202-6dc3-0014-581600024052
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_OF_WEEK as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by DAY_OF_WEEK

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_DAY_OF_WEEK__0__1__2__3__4__5__6.7efae6f4dd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.204451195+00:00
-- finished_at: 2026-03-10T15:58:52.628779234+00:00
-- elapsed: 424ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_QUARTER__1__2__3__4.4182f7a3b9
-- query_id: 01c2effe-3202-6db6-0014-58160002017a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        QUARTER as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by QUARTER

)

select *
from all_values
where value_field not in (
    '1','2','3','4'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_QUARTER__1__2__3__4.4182f7a3b9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.146196763+00:00
-- finished_at: 2026-03-10T15:58:52.640098892+00:00
-- elapsed: 493ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_da1fae6cd48ffa25502c8c26b839eb18.b4192735d6
-- query_id: 01c2effe-3202-6d9c-0014-5816000231ee
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_da1fae6cd48ffa25502c8c26b839eb18.b4192735d6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.261782800+00:00
-- finished_at: 2026-03-10T15:58:52.789446127+00:00
-- elapsed: 527ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_IS_WEEKEND__true__false.8dd2703a3a
-- query_id: 01c2effe-3202-6dc3-0014-581600024066
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_WEEKEND as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by IS_WEEKEND

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_IS_WEEKEND__true__false.8dd2703a3a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.294370235+00:00
-- finished_at: 2026-03-10T15:58:52.803286334+00:00
-- elapsed: 508ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_PRODUCT_ID.de5c71a3af
-- query_id: 01c2effe-3202-6dc3-0014-58160002406a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_PRODUCT_ID.de5c71a3af", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.228455924+00:00
-- finished_at: 2026-03-10T15:58:52.841660244+00:00
-- elapsed: 613ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_CART__0__1.5c40c41c0c
-- query_id: 01c2effe-3202-6dc3-0014-581600024062
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_CART as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_CART

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_CART__0__1.5c40c41c0c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.192317797+00:00
-- finished_at: 2026-03-10T15:58:52.875368627+00:00
-- elapsed: 683ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_L1.5043e574fa
-- query_id: 01c2effe-3202-6d9c-0014-5816000231f6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_L1.5043e574fa", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.314198609+00:00
-- finished_at: 2026-03-10T15:58:52.921594419+00:00
-- elapsed: 607ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_MONTH.371af8f104
-- query_id: 01c2effe-3202-6d97-0014-58160001f186
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select MONTH
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where MONTH is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_MONTH.371af8f104", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.181741240+00:00
-- finished_at: 2026-03-10T15:58:52.933779408+00:00
-- elapsed: 752ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_BRAND.a5ec5821ff
-- query_id: 01c2effe-3202-6d9c-0014-5816000231f2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_BRAND.a5ec5821ff", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.225207867+00:00
-- finished_at: 2026-03-10T15:58:52.939305286+00:00
-- elapsed: 714ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_789f24d42f36ec2f925c6d85d0999705.663b6a250f
-- query_id: 01c2effe-3202-6c48-0014-5816000221a6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_HOUR as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by EVENT_HOUR

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_789f24d42f36ec2f925c6d85d0999705.663b6a250f", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.324534121+00:00
-- finished_at: 2026-03-10T15:58:52.999035101+00:00
-- elapsed: 674ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_VIEW.5f15874792
-- query_id: 01c2effe-3202-6d97-0014-58160001f192
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_VIEW.5f15874792", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.316298986+00:00
-- finished_at: 2026-03-10T15:58:53.011880548+00:00
-- elapsed: 695ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_DAY_OF_WEEK.11e0aa6c4d
-- query_id: 01c2effe-3202-6dc3-0014-58160002406e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_OF_WEEK
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where DAY_OF_WEEK is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_DAY_OF_WEEK.11e0aa6c4d", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.326601039+00:00
-- finished_at: 2026-03-10T15:58:53.016466530+00:00
-- elapsed: 689ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_TIME.6fb849a6a6
-- query_id: 01c2effe-3202-6d97-0014-58160001f18e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_TIME.6fb849a6a6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.301116003+00:00
-- finished_at: 2026-03-10T15:58:53.029132620+00:00
-- elapsed: 728ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_PRICE.feae84d4e9
-- query_id: 01c2effe-3202-6d9c-0014-5816000231fa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_PRICE.feae84d4e9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.148636931+00:00
-- finished_at: 2026-03-10T15:58:53.035046155+00:00
-- elapsed: 886ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.relationships_fct_events_EVENT_DATE__EVENT_DATE__ref_dim_dates_.3f7b055ece
-- query_id: 01c2effe-3202-6d97-0014-58160001f17e
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.relationships_fct_events_EVENT_DATE__EVENT_DATE__ref_dim_dates_.3f7b055ece", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.320941051+00:00
-- finished_at: 2026-03-10T15:58:53.037776082+00:00
-- elapsed: 716ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_DAY_NAME.437c012c02
-- query_id: 01c2effe-3202-6d97-0014-58160001f196
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_NAME
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where DAY_NAME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_DAY_NAME.437c012c02", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.139101635+00:00
-- finished_at: 2026-03-10T15:58:53.040466110+00:00
-- elapsed: 901ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.relationships_fct_events_14461f4a74831fe72206b0fc8a0392ab.bb7991c1fa
-- query_id: 01c2effe-3202-6dc3-0014-581600024056
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select PRODUCT_ID as from_field
    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    where PRODUCT_ID is not null
),

parent as (
    select PRODUCT_ID as to_field
    from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.relationships_fct_events_14461f4a74831fe72206b0fc8a0392ab.bb7991c1fa", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.684011222+00:00
-- finished_at: 2026-03-10T15:58:53.047594016+00:00
-- elapsed: 363ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_PRODUCT_ID.1f28c486d7
-- query_id: 01c2effe-3202-6d9c-0014-5816000231fe
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_PRODUCT_ID.1f28c486d7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:51.993260534+00:00
-- finished_at: 2026-03-10T15:58:53.051301781+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_YEAR.6998c47062
-- query_id: 01c2effe-3202-6dbd-0014-58160002119a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where YEAR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_YEAR.6998c47062", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.191865336+00:00
-- finished_at: 2026-03-10T15:58:53.054347606+00:00
-- elapsed: 862ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_IS_WEEKEND.c97a7435a9
-- query_id: 01c2effe-3202-6dbd-0014-58160002119e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_IS_WEEKEND.c97a7435a9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.124044892+00:00
-- finished_at: 2026-03-10T15:58:53.126910875+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_251de65f5724bcf7d23364b11dc72fed.66c9ba7fe7
-- query_id: 01c2effe-3202-6dbd-0014-581600021196
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_NAME as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by DAY_NAME

)

select *
from all_values
where value_field not in (
    'Mon','Tue','Wed','Thu','Fri','Sat','Sun'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_251de65f5724bcf7d23364b11dc72fed.66c9ba7fe7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.325860626+00:00
-- finished_at: 2026-03-10T15:58:53.178467451+00:00
-- elapsed: 852ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_VIEW__0__1.5fe37935f3
-- query_id: 01c2effe-3202-6d97-0014-58160001f19a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_VIEW__0__1.5fe37935f3", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:52.326152128+00:00
-- finished_at: 2026-03-10T15:58:53.184252548+00:00
-- elapsed: 858ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.unique_dim_dates_EVENT_DATE.46b102f080
-- query_id: 01c2effe-3202-6d97-0014-58160001f18a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.unique_dim_dates_EVENT_DATE.46b102f080", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:53.304685288+00:00
-- finished_at: 2026-03-10T15:58:53.731705160+00:00
-- elapsed: 427ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_WEEKEND__true__false.b010ff5ce4
-- query_id: 01c2effe-3202-6db9-0014-58160001e1f2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_WEEKEND as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_WEEKEND

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_WEEKEND__true__false.b010ff5ce4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:53.313699848+00:00
-- finished_at: 2026-03-10T15:58:53.905287605+00:00
-- elapsed: 591ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.unique_dim_products_PRODUCT_ID.5b3502e45d
-- query_id: 01c2effe-3202-6d9c-0014-581600023202
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    PRODUCT_ID as unique_field,
    count(*) as n_records

from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where PRODUCT_ID is not null
group by PRODUCT_ID
having count(*) > 1



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.unique_dim_products_PRODUCT_ID.5b3502e45d", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T15:58:53.296169613+00:00
-- finished_at: 2026-03-10T15:58:53.936375488+00:00
-- elapsed: 640ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_PURCHASE__0__1.692b30d4c4
-- query_id: 01c2effe-3202-6dc3-0014-581600024072
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_PURCHASE__0__1.692b30d4c4", "profile_name": "user", "target_name": "dev"} */;

============================== 70c70eb8-5869-4cfc-91ed-fa90c8c1bfa0 ==============================
-- created_at: 2026-03-10T16:04:22.205141859+00:00
-- finished_at: 2026-03-10T16:04:22.581865699+00:00
-- elapsed: 376ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f004-3202-6d9c-0014-58160002320e
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."RAW"."EVENTS_RAW";
-- created_at: 2026-03-10T16:04:24.436947875+00:00
-- finished_at: 2026-03-10T16:04:24.731569371+00:00
-- elapsed: 294ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f004-3202-6d9c-0014-581600023212
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:04:26.279330298+00:00
-- finished_at: 2026-03-10T16:04:26.626975480+00:00
-- elapsed: 347ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2f004-3202-6c48-0014-5816000221be
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T16:04:26.630111531+00:00
-- finished_at: 2026-03-10T16:04:58.259772665+00:00
-- elapsed: 31.6s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2f004-3202-6db9-0014-58160001e20a
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    
    
    
    as (

WITH source AS (
    SELECT * FROM ECOMMERCE_ANALYTICS.RAW.EVENTS_RAW
),
brand_fixed AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        COALESCE(
            BRAND,
            FIRST_VALUE(BRAND) IGNORE NULLS OVER (
                PARTITION BY PRODUCT_ID
                ORDER BY EVENT_TIME
            ),
            'unknown'
        )                        AS BRAND,
        COALESCE(
            CATEGORY_CODE, 
            'unknown'
        )                        AS CATEGORY_CODE
    FROM source
    WHERE USER_SESSION IS NOT NULL
),
cleaned AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        BRAND,
        CATEGORY_CODE,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.', 1), ''),
            'unknown')           AS CATEGORY_L1,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.',
                ARRAY_SIZE(SPLIT(CATEGORY_CODE, '.'))), ''),
            'unknown')           AS CATEGORY_LEAF,
        DATE(EVENT_TIME)         AS EVENT_DATE,
        HOUR(EVENT_TIME)         AS EVENT_HOUR,
        DAYNAME(EVENT_TIME)      AS DAY_NAME,
        DAYOFWEEK(EVENT_TIME)    AS DAY_OF_WEEK,
        CASE WHEN DAYOFWEEK(EVENT_TIME)
             IN (0, 6) THEN TRUE
             ELSE FALSE END      AS IS_WEEKEND,
        MONTH(EVENT_TIME)        AS EVENT_MONTH,
        QUARTER(EVENT_TIME)      AS EVENT_QUARTER,
        YEAR(EVENT_TIME)         AS EVENT_YEAR,
        CASE WHEN EVENT_TYPE = 'view'
             THEN 1 ELSE 0 END   AS IS_VIEW,
        CASE WHEN EVENT_TYPE = 'cart'
             THEN 1 ELSE 0 END   AS IS_CART,
        CASE WHEN EVENT_TYPE = 'purchase'
             THEN 1 ELSE 0 END   AS IS_PURCHASE
    FROM brand_fixed
)
SELECT * FROM cleaned
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.stg_events_clean", "profile_name": "user", "target_name": "dev"} */;

============================== ce9f3bc9-616d-4185-a936-dfb81bf9de91 ==============================
-- created_at: 2026-03-10T16:05:35.109871287+00:00
-- finished_at: 2026-03-10T16:05:35.410774311+00:00
-- elapsed: 300ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f005-3202-6dbd-0014-5816000211be
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-10T16:05:37.367043099+00:00
-- finished_at: 2026-03-10T16:05:37.695529140+00:00
-- elapsed: 328ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd
-- query_id: 01c2f005-3202-6dbd-0014-5816000211c6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.348794046+00:00
-- finished_at: 2026-03-10T16:05:37.699174676+00:00
-- elapsed: 350ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70
-- query_id: 01c2f005-3202-6dbd-0014-5816000211c2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.384752815+00:00
-- finished_at: 2026-03-10T16:05:37.724090798+00:00
-- elapsed: 339ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296
-- query_id: 01c2f005-3202-6dbd-0014-5816000211ca
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.387155592+00:00
-- finished_at: 2026-03-10T16:05:37.726013737+00:00
-- elapsed: 338ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02
-- query_id: 01c2f005-3202-6dbd-0014-5816000211ce
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_MONTH
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_MONTH is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.398075310+00:00
-- finished_at: 2026-03-10T16:05:37.846997677+00:00
-- elapsed: 448ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4
-- query_id: 01c2f005-3202-6db9-0014-58160001e212
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.545402132+00:00
-- finished_at: 2026-03-10T16:05:37.848613728+00:00
-- elapsed: 303ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da
-- query_id: 01c2f005-3202-6db9-0014-58160001e216
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.561308230+00:00
-- finished_at: 2026-03-10T16:05:38.086084292+00:00
-- elapsed: 524ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561
-- query_id: 01c2f005-3202-6db6-0014-58160002018e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.638369422+00:00
-- finished_at: 2026-03-10T16:05:38.105306479+00:00
-- elapsed: 466ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380
-- query_id: 01c2f005-3202-6d9c-0014-58160002321e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.651460700+00:00
-- finished_at: 2026-03-10T16:05:38.112365979+00:00
-- elapsed: 460ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e
-- query_id: 01c2f005-3202-6dc3-0014-581600024082
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.580237088+00:00
-- finished_at: 2026-03-10T16:05:38.136441951+00:00
-- elapsed: 556ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526
-- query_id: 01c2f005-3202-6db6-0014-581600020192
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.721680120+00:00
-- finished_at: 2026-03-10T16:05:38.258469688+00:00
-- elapsed: 536ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4
-- query_id: 01c2f005-3202-6d9c-0014-58160002322a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_QUARTER
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_QUARTER is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.665493191+00:00
-- finished_at: 2026-03-10T16:05:38.315867037+00:00
-- elapsed: 650ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_DAY_NAME.9dd925a4e2
-- query_id: 01c2f005-3202-6d9c-0014-581600023222
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_NAME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where DAY_NAME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_DAY_NAME.9dd925a4e2", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.630257608+00:00
-- finished_at: 2026-03-10T16:05:38.375731986+00:00
-- elapsed: 745ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_DATE.728f2410bc
-- query_id: 01c2f005-3202-6d9c-0014-58160002321a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_DATE.728f2410bc", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.644238004+00:00
-- finished_at: 2026-03-10T16:05:38.383802406+00:00
-- elapsed: 739ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57
-- query_id: 01c2f005-3202-6dc3-0014-58160002407e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.692746368+00:00
-- finished_at: 2026-03-10T16:05:38.386691626+00:00
-- elapsed: 693ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b
-- query_id: 01c2f005-3202-6d9c-0014-581600023226
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TYPE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TYPE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.592915577+00:00
-- finished_at: 2026-03-10T16:05:38.390565377+00:00
-- elapsed: 797ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_DAY_OF_WEEK.890fa98e34
-- query_id: 01c2f005-3202-6c48-0014-5816000221c6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_OF_WEEK
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where DAY_OF_WEEK is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_DAY_OF_WEEK.890fa98e34", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.593370784+00:00
-- finished_at: 2026-03-10T16:05:38.408160904+00:00
-- elapsed: 814ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73
-- query_id: 01c2f005-3202-6dc3-0014-58160002407a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.649138258+00:00
-- finished_at: 2026-03-10T16:05:38.421950688+00:00
-- elapsed: 772ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43
-- query_id: 01c2f005-3202-6c48-0014-5816000221ca
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.661194647+00:00
-- finished_at: 2026-03-10T16:05:38.427043700+00:00
-- elapsed: 765ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c
-- query_id: 01c2f005-3202-6dc3-0014-581600024086
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.940915095+00:00
-- finished_at: 2026-03-10T16:05:38.428934445+00:00
-- elapsed: 488ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177
-- query_id: 01c2f005-3202-6db9-0014-58160001e21e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.583416165+00:00
-- finished_at: 2026-03-10T16:05:38.434879764+00:00
-- elapsed: 851ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5
-- query_id: 01c2f005-3202-6c48-0014-5816000221c2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.589148966+00:00
-- finished_at: 2026-03-10T16:05:38.444032376+00:00
-- elapsed: 854ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_206d1dd4470d87c187a8c7e68f7cc25a.c42471ca4a
-- query_id: 01c2f005-3202-6db9-0014-58160001e21a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_206d1dd4470d87c187a8c7e68f7cc25a.c42471ca4a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.734834175+00:00
-- finished_at: 2026-03-10T16:05:38.444733653+00:00
-- elapsed: 709ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a
-- query_id: 01c2f005-3202-6c48-0014-5816000221ce
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.977087545+00:00
-- finished_at: 2026-03-10T16:05:38.468009285+00:00
-- elapsed: 490ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_HOUR.76ec27905a
-- query_id: 01c2f005-3202-6d9c-0014-58160002322e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_HOUR
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_HOUR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_HOUR.76ec27905a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.988612527+00:00
-- finished_at: 2026-03-10T16:05:38.471801819+00:00
-- elapsed: 483ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7
-- query_id: 01c2f005-3202-6dbd-0014-5816000211d6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_YEAR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.985295531+00:00
-- finished_at: 2026-03-10T16:05:38.481734919+00:00
-- elapsed: 496ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_ee9c969df76ee41259a90537e9c9d129.543a8ae63b
-- query_id: 01c2f005-3202-6d9c-0014-581600023232
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_MONTH as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by EVENT_MONTH

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6','7','8','9','10','11','12'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_ee9c969df76ee41259a90537e9c9d129.543a8ae63b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.982236590+00:00
-- finished_at: 2026-03-10T16:05:38.492230758+00:00
-- elapsed: 509ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_EVENT_QUARTER__1__2__3__4.74a0370526
-- query_id: 01c2f005-3202-6db9-0014-58160001e226
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_EVENT_QUARTER__1__2__3__4.74a0370526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.975282207+00:00
-- finished_at: 2026-03-10T16:05:38.685385826+00:00
-- elapsed: 710ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_674a138e330566a305c49db295b6b2e8.9baf28cdf8
-- query_id: 01c2f005-3202-6db9-0014-58160001e22a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_HOUR as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by EVENT_HOUR

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_674a138e330566a305c49db295b6b2e8.9baf28cdf8", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.973312095+00:00
-- finished_at: 2026-03-10T16:05:38.753674585+00:00
-- elapsed: 780ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_0a4a62d43b3c5bb84f106667b523ab53.7686e7eae2
-- query_id: 01c2f005-3202-6db9-0014-58160001e222
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_OF_WEEK as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by DAY_OF_WEEK

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_0a4a62d43b3c5bb84f106667b523ab53.7686e7eae2", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.971861118+00:00
-- finished_at: 2026-03-10T16:05:38.786278959+00:00
-- elapsed: 814ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187
-- query_id: 01c2f005-3202-6db6-0014-581600020196
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:37.982885122+00:00
-- finished_at: 2026-03-10T16:05:38.800768682+00:00
-- elapsed: 817ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62
-- query_id: 01c2f005-3202-6db9-0014-58160001e22e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:38.387390006+00:00
-- finished_at: 2026-03-10T16:05:38.828213121+00:00
-- elapsed: 440ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a
-- query_id: 01c2f005-3202-6d97-0014-58160001f1a6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a", "profile_name": "user", "target_name": "dev"} */;

============================== 2ed8e752-8605-4a22-befa-038ef1e73c4e ==============================
-- created_at: 2026-03-10T16:05:54.015174834+00:00
-- finished_at: 2026-03-10T16:05:54.281182130+00:00
-- elapsed: 266ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f005-3202-6db6-0014-58160002019a
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-10T16:05:56.432025230+00:00
-- finished_at: 2026-03-10T16:05:57.098892904+00:00
-- elapsed: 666ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f005-3202-6dbd-0014-5816000211da
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:58.178210644+00:00
-- finished_at: 2026-03-10T16:05:58.457407300+00:00
-- elapsed: 279ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2f005-3202-6db6-0014-58160002019e
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T16:05:58.438135366+00:00
-- finished_at: 2026-03-10T16:05:58.777107214+00:00
-- elapsed: 338ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2f005-3202-6db9-0014-58160001e232
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T16:05:58.452043859+00:00
-- finished_at: 2026-03-10T16:05:58.778460748+00:00
-- elapsed: 326ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2f005-3202-6db9-0014-58160001e236
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-10T16:05:58.779980765+00:00
-- finished_at: 2026-03-10T16:06:02.035148911+00:00
-- elapsed: 3.3s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2f005-3202-6d9c-0014-581600023236
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    
    
    
    as (

SELECT DISTINCT
    EVENT_DATE,
    DAY_NAME,
    DAY_OF_WEEK,
    IS_WEEKEND,
    MONTH(EVENT_DATE)                                        AS MONTH,
    QUARTER(EVENT_DATE)                                      AS QUARTER,
    YEAR(EVENT_DATE)                                         AS YEAR
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
ORDER BY EVENT_DATE
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_dates", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:58.460528072+00:00
-- finished_at: 2026-03-10T16:06:03.844766373+00:00
-- elapsed: 5.4s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2f005-3202-6c48-0014-5816000221d2
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_products
    
    
    
    as (

SELECT DISTINCT
    PRODUCT_ID,
    CATEGORY_ID,
    CATEGORY_CODE,
    CATEGORY_L1,
    CATEGORY_LEAF,
    BRAND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_products", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:05:58.780874350+00:00
-- finished_at: 2026-03-10T16:06:13.255665561+00:00
-- elapsed: 14.5s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2f005-3202-6dbd-0014-5816000211de
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    
    
    
    as (

SELECT
    EVENT_TIME,
    EVENT_DATE,
    EVENT_HOUR,
    PRODUCT_ID,
    USER_ID,
    USER_SESSION,
    PRICE,
    IS_VIEW,
    IS_CART,
    IS_PURCHASE,
    IS_WEEKEND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.fct_events", "profile_name": "user", "target_name": "dev"} */;

============================== 5e169dca-054b-4a8f-950a-43eef755690b ==============================
-- created_at: 2026-03-10T16:06:30.646969241+00:00
-- finished_at: 2026-03-10T16:06:30.924983781+00:00
-- elapsed: 278ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f006-3202-6d97-0014-58160001f1aa
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."DIM_DATES";
-- created_at: 2026-03-10T16:06:30.687207408+00:00
-- finished_at: 2026-03-10T16:06:30.978708864+00:00
-- elapsed: 291ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f006-3202-6d97-0014-58160001f1ae
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."DIM_PRODUCTS";
-- created_at: 2026-03-10T16:06:30.932358417+00:00
-- finished_at: 2026-03-10T16:06:31.232990930+00:00
-- elapsed: 300ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f006-3202-6db6-0014-5816000201a2
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."FCT_EVENTS";
-- created_at: 2026-03-10T16:06:32.895718993+00:00
-- finished_at: 2026-03-10T16:06:33.381827294+00:00
-- elapsed: 486ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_VIEW.5f15874792
-- query_id: 01c2f006-3202-6d9c-0014-58160002323a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_VIEW.5f15874792", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:32.996712141+00:00
-- finished_at: 2026-03-10T16:06:33.388637219+00:00
-- elapsed: 391ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_QUARTER.6c5112ec42
-- query_id: 01c2f006-3202-6db6-0014-5816000201aa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select QUARTER
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where QUARTER is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_QUARTER.6c5112ec42", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:32.990746276+00:00
-- finished_at: 2026-03-10T16:06:33.393932106+00:00
-- elapsed: 403ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_PRODUCT_ID.1f28c486d7
-- query_id: 01c2f006-3202-6db6-0014-5816000201a6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_PRODUCT_ID.1f28c486d7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:32.979449591+00:00
-- finished_at: 2026-03-10T16:06:33.413415620+00:00
-- elapsed: 433ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_LEAF.060e9e5228
-- query_id: 01c2f006-3202-6c48-0014-5816000221d6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_LEAF.060e9e5228", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:32.984970242+00:00
-- finished_at: 2026-03-10T16:06:33.473240658+00:00
-- elapsed: 488ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_DAY_OF_WEEK__0__1__2__3__4__5__6.7efae6f4dd
-- query_id: 01c2f006-3202-6d9c-0014-58160002323e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_OF_WEEK as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by DAY_OF_WEEK

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_DAY_OF_WEEK__0__1__2__3__4__5__6.7efae6f4dd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.192508889+00:00
-- finished_at: 2026-03-10T16:06:33.489725130+00:00
-- elapsed: 297ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_PRICE.feae84d4e9
-- query_id: 01c2f006-3202-6dc3-0014-581600024092
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_PRICE.feae84d4e9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.197858192+00:00
-- finished_at: 2026-03-10T16:06:33.564942479+00:00
-- elapsed: 367ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_TIME.6fb849a6a6
-- query_id: 01c2f006-3202-6dc3-0014-581600024096
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_TIME.6fb849a6a6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.198837353+00:00
-- finished_at: 2026-03-10T16:06:33.591286142+00:00
-- elapsed: 392ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_USER_ID.60ee434024
-- query_id: 01c2f006-3202-6db6-0014-5816000201b6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_USER_ID.60ee434024", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.206301456+00:00
-- finished_at: 2026-03-10T16:06:33.602020374+00:00
-- elapsed: 395ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_EVENT_DATE.4e052aed85
-- query_id: 01c2f006-3202-6dc3-0014-58160002409a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_EVENT_DATE.4e052aed85", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.173532595+00:00
-- finished_at: 2026-03-10T16:06:33.637569452+00:00
-- elapsed: 464ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_PURCHASE__0__1.692b30d4c4
-- query_id: 01c2f006-3202-6dc3-0014-58160002408e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_PURCHASE__0__1.692b30d4c4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.177521038+00:00
-- finished_at: 2026-03-10T16:06:33.701429843+00:00
-- elapsed: 523ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_YEAR.6998c47062
-- query_id: 01c2f006-3202-6db9-0014-58160001e246
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where YEAR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_YEAR.6998c47062", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.220220211+00:00
-- finished_at: 2026-03-10T16:06:33.751069732+00:00
-- elapsed: 530ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_251de65f5724bcf7d23364b11dc72fed.66c9ba7fe7
-- query_id: 01c2f006-3202-6d97-0014-58160001f1ba
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_NAME as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by DAY_NAME

)

select *
from all_values
where value_field not in (
    'Mon','Tue','Wed','Thu','Fri','Sat','Sun'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_251de65f5724bcf7d23364b11dc72fed.66c9ba7fe7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.168516004+00:00
-- finished_at: 2026-03-10T16:06:33.792605204+00:00
-- elapsed: 624ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_DAY_OF_WEEK.11e0aa6c4d
-- query_id: 01c2f006-3202-6db9-0014-58160001e242
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_OF_WEEK
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where DAY_OF_WEEK is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_DAY_OF_WEEK.11e0aa6c4d", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.135126621+00:00
-- finished_at: 2026-03-10T16:06:33.793396534+00:00
-- elapsed: 658ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_IS_WEEKEND.c97a7435a9
-- query_id: 01c2f006-3202-6db9-0014-58160001e23e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_IS_WEEKEND.c97a7435a9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.227445771+00:00
-- finished_at: 2026-03-10T16:06:33.796978720+00:00
-- elapsed: 569ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_VIEW__0__1.5fe37935f3
-- query_id: 01c2f006-3202-6db6-0014-5816000201ba
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_VIEW__0__1.5fe37935f3", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.235050428+00:00
-- finished_at: 2026-03-10T16:06:33.874655061+00:00
-- elapsed: 639ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.unique_dim_products_PRODUCT_ID.5b3502e45d
-- query_id: 01c2f006-3202-6db6-0014-5816000201be
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    PRODUCT_ID as unique_field,
    count(*) as n_records

from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where PRODUCT_ID is not null
group by PRODUCT_ID
having count(*) > 1



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.unique_dim_products_PRODUCT_ID.5b3502e45d", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.198053760+00:00
-- finished_at: 2026-03-10T16:06:33.886259133+00:00
-- elapsed: 688ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.unique_dim_dates_EVENT_DATE.46b102f080
-- query_id: 01c2f006-3202-6d97-0014-58160001f1b6
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.unique_dim_dates_EVENT_DATE.46b102f080", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.264662790+00:00
-- finished_at: 2026-03-10T16:06:33.897946915+00:00
-- elapsed: 633ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_PRODUCT_ID.de5c71a3af
-- query_id: 01c2f006-3202-6dc3-0014-5816000240a6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_PRODUCT_ID.de5c71a3af", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.254263158+00:00
-- finished_at: 2026-03-10T16:06:33.930918693+00:00
-- elapsed: 676ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_QUARTER__1__2__3__4.4182f7a3b9
-- query_id: 01c2f006-3202-6d97-0014-58160001f1c2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        QUARTER as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by QUARTER

)

select *
from all_values
where value_field not in (
    '1','2','3','4'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_QUARTER__1__2__3__4.4182f7a3b9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.186814148+00:00
-- finished_at: 2026-03-10T16:06:33.958310508+00:00
-- elapsed: 771ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_PURCHASE.b9b980b060
-- query_id: 01c2f006-3202-6d97-0014-58160001f1b2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_PURCHASE.b9b980b060", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.169632315+00:00
-- finished_at: 2026-03-10T16:06:33.960010565+00:00
-- elapsed: 790ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_CART__0__1.5c40c41c0c
-- query_id: 01c2f006-3202-6db6-0014-5816000201ae
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_CART as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_CART

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_CART__0__1.5c40c41c0c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:32.952652047+00:00
-- finished_at: 2026-03-10T16:06:33.981006244+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.relationships_fct_events_EVENT_DATE__EVENT_DATE__ref_dim_dates_.3f7b055ece
-- query_id: 01c2f006-3202-6dc3-0014-58160002408a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.relationships_fct_events_EVENT_DATE__EVENT_DATE__ref_dim_dates_.3f7b055ece", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.132131438+00:00
-- finished_at: 2026-03-10T16:06:34.006695564+00:00
-- elapsed: 874ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_MONTH.371af8f104
-- query_id: 01c2f006-3202-6dbd-0014-5816000211e2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select MONTH
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where MONTH is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_MONTH.371af8f104", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.166412633+00:00
-- finished_at: 2026-03-10T16:06:34.007465701+00:00
-- elapsed: 841ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_fct_events_PRICE___0.437bc2a8d4
-- query_id: 01c2f006-3202-6c48-0014-5816000221de
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_fct_events_PRICE___0.437bc2a8d4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.271726957+00:00
-- finished_at: 2026-03-10T16:06:34.020050881+00:00
-- elapsed: 748ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_USER_SESSION.19d896670e
-- query_id: 01c2f006-3202-6db6-0014-5816000201c2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_USER_SESSION.19d896670e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.240234839+00:00
-- finished_at: 2026-03-10T16:06:34.043411769+00:00
-- elapsed: 803ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_IS_WEEKEND__true__false.8dd2703a3a
-- query_id: 01c2f006-3202-6db9-0014-58160001e24a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_WEEKEND as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by IS_WEEKEND

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_IS_WEEKEND__true__false.8dd2703a3a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.293974482+00:00
-- finished_at: 2026-03-10T16:06:34.093995043+00:00
-- elapsed: 800ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_WEEKEND.929546128f
-- query_id: 01c2f006-3202-6dc3-0014-5816000240aa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_WEEKEND.929546128f", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.237623022+00:00
-- finished_at: 2026-03-10T16:06:34.097080365+00:00
-- elapsed: 859ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_BRAND.a5ec5821ff
-- query_id: 01c2f006-3202-6d97-0014-58160001f1be
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_BRAND.a5ec5821ff", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.313834886+00:00
-- finished_at: 2026-03-10T16:06:34.114585701+00:00
-- elapsed: 800ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_DAY_NAME.437c012c02
-- query_id: 01c2f006-3202-6db9-0014-58160001e25e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_NAME
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where DAY_NAME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_DAY_NAME.437c012c02", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.206571193+00:00
-- finished_at: 2026-03-10T16:06:34.140926456+00:00
-- elapsed: 934ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_DATE.db2e1e2234
-- query_id: 01c2f006-3202-6db9-0014-58160001e252
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_DATE.db2e1e2234", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.281791265+00:00
-- finished_at: 2026-03-10T16:06:34.196577921+00:00
-- elapsed: 914ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_CART.c05a32eb8a
-- query_id: 01c2f006-3202-6db9-0014-58160001e25a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_CART.c05a32eb8a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.199895744+00:00
-- finished_at: 2026-03-10T16:06:34.201352250+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_L1.5043e574fa
-- query_id: 01c2f006-3202-6dc3-0014-5816000240a2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_L1.5043e574fa", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.268421606+00:00
-- finished_at: 2026-03-10T16:06:34.209975097+00:00
-- elapsed: 941ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_HOUR.6ef38f0fd6
-- query_id: 01c2f006-3202-6dbd-0014-5816000211e6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_HOUR
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_HOUR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_HOUR.6ef38f0fd6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.247231931+00:00
-- finished_at: 2026-03-10T16:06:34.221690045+00:00
-- elapsed: 974ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_da1fae6cd48ffa25502c8c26b839eb18.b4192735d6
-- query_id: 01c2f006-3202-6c48-0014-5816000221e2
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_da1fae6cd48ffa25502c8c26b839eb18.b4192735d6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.199825452+00:00
-- finished_at: 2026-03-10T16:06:34.236160315+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_CODE.0d8dad3555
-- query_id: 01c2f006-3202-6dc3-0014-58160002409e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_CODE.0d8dad3555", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.236474075+00:00
-- finished_at: 2026-03-10T16:06:34.256295654+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_ID.579c7768dd
-- query_id: 01c2f006-3202-6db9-0014-58160001e24e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_ID
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_ID.579c7768dd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.187738239+00:00
-- finished_at: 2026-03-10T16:06:34.274189151+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.relationships_fct_events_14461f4a74831fe72206b0fc8a0392ab.bb7991c1fa
-- query_id: 01c2f006-3202-6db6-0014-5816000201b2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select PRODUCT_ID as from_field
    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    where PRODUCT_ID is not null
),

parent as (
    select PRODUCT_ID as to_field
    from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.relationships_fct_events_14461f4a74831fe72206b0fc8a0392ab.bb7991c1fa", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.168916222+00:00
-- finished_at: 2026-03-10T16:06:34.282883998+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_WEEKEND__true__false.b010ff5ce4
-- query_id: 01c2f006-3202-6c48-0014-5816000221da
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_WEEKEND as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_WEEKEND

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_WEEKEND__true__false.b010ff5ce4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:06:33.244218021+00:00
-- finished_at: 2026-03-10T16:06:34.299371679+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_789f24d42f36ec2f925c6d85d0999705.663b6a250f
-- query_id: 01c2f006-3202-6db9-0014-58160001e256
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_HOUR as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by EVENT_HOUR

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_789f24d42f36ec2f925c6d85d0999705.663b6a250f", "profile_name": "user", "target_name": "dev"} */;

============================== 7bad505f-205e-473d-8dd3-1a7c04b14af6 ==============================
-- created_at: 2026-03-10T16:07:03.350274077+00:00
-- finished_at: 2026-03-10T16:07:03.651639081+00:00
-- elapsed: 301ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f007-3202-6db6-0014-5816000201c6
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."RAW"."EVENTS_RAW";

============================== 8f09ecf6-7886-4870-b6cd-e98a0e9b2010 ==============================
-- created_at: 2026-03-11T04:42:05.810242391+00:00
-- finished_at: 2026-03-11T04:42:06.094384255+00:00
-- elapsed: 284ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f2fa-3202-6e14-0014-58160002d1f6
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."RAW"."EVENTS_RAW";
-- created_at: 2026-03-11T04:42:07.910400620+00:00
-- finished_at: 2026-03-11T04:42:08.176004865+00:00
-- elapsed: 265ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f2fa-3202-6e14-0014-58160002d1fa
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;

============================== 5a7641a9-1a0b-40ff-ba27-d775aa1e2b78 ==============================
-- created_at: 2026-03-11T04:44:52.336519581+00:00
-- finished_at: 2026-03-11T04:44:52.599184854+00:00
-- elapsed: 262ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f2fc-3202-6e14-0014-58160002d1fe
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."RAW"."EVENTS_RAW";
-- created_at: 2026-03-11T04:44:54.473843208+00:00
-- finished_at: 2026-03-11T04:44:54.739677882+00:00
-- elapsed: 265ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f2fc-3202-6e14-0014-58160002d202
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:44:56.091906729+00:00
-- finished_at: 2026-03-11T04:44:56.371189227+00:00
-- elapsed: 279ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2f2fc-3202-6e30-0014-58160002f092
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-11T04:44:56.374442457+00:00
-- finished_at: 2026-03-11T04:45:28.341622719+00:00
-- elapsed: 32.0s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2f2fc-3202-6e14-0014-58160002d206
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    
    
    
    as (

WITH source AS (
    SELECT * FROM ECOMMERCE_ANALYTICS.RAW.EVENTS_RAW
),
brand_fixed AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        COALESCE(
            BRAND,
            FIRST_VALUE(BRAND) IGNORE NULLS OVER (
                PARTITION BY PRODUCT_ID
                ORDER BY EVENT_TIME
            ),
            'unknown'
        )                        AS BRAND,
        COALESCE(
            CATEGORY_CODE, 
            'unknown'
        )                        AS CATEGORY_CODE
    FROM source
    WHERE USER_SESSION IS NOT NULL
        AND PRODUCT_ID NOT IN (
            SELECT PRODUCT_ID
            FROM ECOMMERCE_ANALYTICS.RAW.EVENTS_RAW
            WHERE BRAND IS NOT NULL
            GROUP BY PRODUCT_ID
            HAVING COUNT(DISTINCT BRAND) > 1
        )
),
cleaned AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        BRAND,
        CATEGORY_CODE,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.', 1), ''),
            'unknown')           AS CATEGORY_L1,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.',
                ARRAY_SIZE(SPLIT(CATEGORY_CODE, '.'))), ''),
            'unknown')           AS CATEGORY_LEAF,
        DATE(EVENT_TIME)         AS EVENT_DATE,
        HOUR(EVENT_TIME)         AS EVENT_HOUR,
        DAYNAME(EVENT_TIME)      AS DAY_NAME,
        DAYOFWEEK(EVENT_TIME)    AS DAY_OF_WEEK,
        CASE WHEN DAYOFWEEK(EVENT_TIME)
             IN (0, 6) THEN TRUE
             ELSE FALSE END      AS IS_WEEKEND,
        MONTH(EVENT_TIME)        AS EVENT_MONTH,
        QUARTER(EVENT_TIME)      AS EVENT_QUARTER,
        YEAR(EVENT_TIME)         AS EVENT_YEAR,
        CASE WHEN EVENT_TYPE = 'view'
             THEN 1 ELSE 0 END   AS IS_VIEW,
        CASE WHEN EVENT_TYPE = 'cart'
             THEN 1 ELSE 0 END   AS IS_CART,
        CASE WHEN EVENT_TYPE = 'purchase'
             THEN 1 ELSE 0 END   AS IS_PURCHASE
    FROM brand_fixed
)
SELECT * FROM cleaned
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.stg_events_clean", "profile_name": "user", "target_name": "dev"} */;

============================== fecb7818-9a3a-4519-ac33-c073c3ada247 ==============================
-- created_at: 2026-03-11T04:47:47.925573547+00:00
-- finished_at: 2026-03-11T04:47:48.218693966+00:00
-- elapsed: 293ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d20a
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-11T04:47:50.133101860+00:00
-- finished_at: 2026-03-11T04:47:50.481090349+00:00
-- elapsed: 347ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d236
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.095399188+00:00
-- finished_at: 2026-03-11T04:47:50.483325954+00:00
-- elapsed: 387ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_DAY_NAME.9dd925a4e2
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d21a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_NAME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where DAY_NAME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_DAY_NAME.9dd925a4e2", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.109328517+00:00
-- finished_at: 2026-03-11T04:47:50.490765162+00:00
-- elapsed: 381ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d222
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.091046527+00:00
-- finished_at: 2026-03-11T04:47:50.496712483+00:00
-- elapsed: 405ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d212
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.126461099+00:00
-- finished_at: 2026-03-11T04:47:50.501348383+00:00
-- elapsed: 374ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_DATE.728f2410bc
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d232
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_DATE.728f2410bc", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.097105619+00:00
-- finished_at: 2026-03-11T04:47:50.514325520+00:00
-- elapsed: 417ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_HOUR.76ec27905a
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d216
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_HOUR
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_HOUR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_HOUR.76ec27905a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.074738173+00:00
-- finished_at: 2026-03-11T04:47:50.518597724+00:00
-- elapsed: 443ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d20e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.115598623+00:00
-- finished_at: 2026-03-11T04:47:50.534541121+00:00
-- elapsed: 418ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d226
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.123429146+00:00
-- finished_at: 2026-03-11T04:47:50.543599698+00:00
-- elapsed: 420ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d22a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_YEAR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.139355287+00:00
-- finished_at: 2026-03-11T04:47:50.561046685+00:00
-- elapsed: 421ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d23a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.124669716+00:00
-- finished_at: 2026-03-11T04:47:50.562682922+00:00
-- elapsed: 438ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d22e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TYPE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TYPE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.189187656+00:00
-- finished_at: 2026-03-11T04:47:50.757935502+00:00
-- elapsed: 568ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d246
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_MONTH
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_MONTH is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.299303309+00:00
-- finished_at: 2026-03-11T04:47:50.893849099+00:00
-- elapsed: 594ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177
-- query_id: 01c2f2ff-3202-6e30-0014-58160002f09a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.143580866+00:00
-- finished_at: 2026-03-11T04:47:50.927609449+00:00
-- elapsed: 784ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a
-- query_id: 01c2f2ff-3202-6e30-0014-58160002f096
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.106285075+00:00
-- finished_at: 2026-03-11T04:47:50.950301848+00:00
-- elapsed: 844ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d21e
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.311617737+00:00
-- finished_at: 2026-03-11T04:47:50.964604656+00:00
-- elapsed: 652ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43
-- query_id: 01c2f2ff-3202-6e30-0014-58160002f0a6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.322592393+00:00
-- finished_at: 2026-03-11T04:47:50.969610167+00:00
-- elapsed: 647ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_DAY_OF_WEEK.890fa98e34
-- query_id: 01c2f2ff-3202-6e30-0014-58160002f0a2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_OF_WEEK
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where DAY_OF_WEEK is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_DAY_OF_WEEK.890fa98e34", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.312596662+00:00
-- finished_at: 2026-03-11T04:47:50.971183806+00:00
-- elapsed: 658ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561
-- query_id: 01c2f2ff-3202-6e30-0014-58160002f09e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.149499577+00:00
-- finished_at: 2026-03-11T04:47:50.985718657+00:00
-- elapsed: 836ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d23e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.153360958+00:00
-- finished_at: 2026-03-11T04:47:50.993689296+00:00
-- elapsed: 840ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_674a138e330566a305c49db295b6b2e8.9baf28cdf8
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d242
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_HOUR as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by EVENT_HOUR

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_674a138e330566a305c49db295b6b2e8.9baf28cdf8", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.209338079+00:00
-- finished_at: 2026-03-11T04:47:50.995849653+00:00
-- elapsed: 786ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d24e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.344511601+00:00
-- finished_at: 2026-03-11T04:47:51.046361506+00:00
-- elapsed: 701ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5
-- query_id: 01c2f2ff-3202-6e30-0014-58160002f0b2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.347759449+00:00
-- finished_at: 2026-03-11T04:47:51.057374912+00:00
-- elapsed: 709ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_EVENT_QUARTER__1__2__3__4.74a0370526
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d256
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_EVENT_QUARTER__1__2__3__4.74a0370526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.339961297+00:00
-- finished_at: 2026-03-11T04:47:51.094137621+00:00
-- elapsed: 754ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380
-- query_id: 01c2f2ff-3202-6e30-0014-58160002f0ae
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.361474789+00:00
-- finished_at: 2026-03-11T04:47:51.152674272+00:00
-- elapsed: 791ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd
-- query_id: 01c2f2ff-3202-6e30-0014-58160002f0b6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.382611434+00:00
-- finished_at: 2026-03-11T04:47:51.189119055+00:00
-- elapsed: 806ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_ee9c969df76ee41259a90537e9c9d129.543a8ae63b
-- query_id: 01c2f2ff-3202-6e30-0014-58160002f0be
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_MONTH as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by EVENT_MONTH

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6','7','8','9','10','11','12'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_ee9c969df76ee41259a90537e9c9d129.543a8ae63b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.357350385+00:00
-- finished_at: 2026-03-11T04:47:51.213368929+00:00
-- elapsed: 856ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4
-- query_id: 01c2f2ff-3202-6e30-0014-58160002f0ba
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_QUARTER
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_QUARTER is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.398081491+00:00
-- finished_at: 2026-03-11T04:47:51.241327093+00:00
-- elapsed: 843ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70
-- query_id: 01c2f2ff-3202-6e30-0014-58160002f0c2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.342785174+00:00
-- finished_at: 2026-03-11T04:47:51.327212528+00:00
-- elapsed: 984ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_0a4a62d43b3c5bb84f106667b523ab53.7686e7eae2
-- query_id: 01c2f2ff-3202-6e30-0014-58160002f0aa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_OF_WEEK as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by DAY_OF_WEEK

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_0a4a62d43b3c5bb84f106667b523ab53.7686e7eae2", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.339838479+00:00
-- finished_at: 2026-03-11T04:47:51.414008614+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d252
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.203705443+00:00
-- finished_at: 2026-03-11T04:47:51.455863573+00:00
-- elapsed: 1.3s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_206d1dd4470d87c187a8c7e68f7cc25a.c42471ca4a
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d24a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_206d1dd4470d87c187a8c7e68f7cc25a.c42471ca4a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:47:50.428886481+00:00
-- finished_at: 2026-03-11T04:47:51.461129135+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187
-- query_id: 01c2f2ff-3202-6e14-0014-58160002d25a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187", "profile_name": "user", "target_name": "dev"} */;

============================== a7bf2965-5fa9-449b-98e2-2d748013ec33 ==============================
-- created_at: 2026-03-11T04:48:31.028490181+00:00
-- finished_at: 2026-03-11T04:48:31.269186770+00:00
-- elapsed: 240ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f300-3202-6e30-0014-58160002f0c6
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-11T04:48:33.318699219+00:00
-- finished_at: 2026-03-11T04:48:33.568938879+00:00
-- elapsed: 250ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f300-3202-6e14-0014-58160002d25e
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:48:34.580499035+00:00
-- finished_at: 2026-03-11T04:48:34.838255248+00:00
-- elapsed: 257ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2f300-3202-6e30-0014-58160002f0ca
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-11T04:48:34.627836739+00:00
-- finished_at: 2026-03-11T04:48:34.922264662+00:00
-- elapsed: 294ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2f300-3202-6e30-0014-58160002f0ce
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-11T04:48:34.642723835+00:00
-- finished_at: 2026-03-11T04:48:34.940460801+00:00
-- elapsed: 297ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2f300-3202-6e14-0014-58160002d262
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-11T04:48:34.942974996+00:00
-- finished_at: 2026-03-11T04:48:38.293746824+00:00
-- elapsed: 3.4s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2f300-3202-6e30-0014-58160002f0d2
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    
    
    
    as (

SELECT DISTINCT
    EVENT_DATE,
    DAY_NAME,
    DAY_OF_WEEK,
    IS_WEEKEND,
    MONTH(EVENT_DATE)                                        AS MONTH,
    QUARTER(EVENT_DATE)                                      AS QUARTER,
    YEAR(EVENT_DATE)                                         AS YEAR
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
ORDER BY EVENT_DATE
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_dates", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:48:34.840845407+00:00
-- finished_at: 2026-03-11T04:48:40.489908946+00:00
-- elapsed: 5.6s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2f300-3202-6e14-0014-58160002d26a
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_products
    
    
    
    as (

SELECT DISTINCT
    PRODUCT_ID,
    CATEGORY_ID,
    CATEGORY_CODE,
    CATEGORY_L1,
    CATEGORY_LEAF,
    BRAND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_products", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:48:34.924924299+00:00
-- finished_at: 2026-03-11T04:48:48.954371416+00:00
-- elapsed: 14.0s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2f300-3202-6e14-0014-58160002d266
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    
    
    
    as (

SELECT
    EVENT_TIME,
    EVENT_DATE,
    EVENT_HOUR,
    PRODUCT_ID,
    USER_ID,
    USER_SESSION,
    PRICE,
    IS_VIEW,
    IS_CART,
    IS_PURCHASE,
    IS_WEEKEND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.fct_events", "profile_name": "user", "target_name": "dev"} */;

============================== 8f31bb32-1ee1-441d-9480-dc3ba6503349 ==============================
-- created_at: 2026-03-11T04:49:02.538814470+00:00
-- finished_at: 2026-03-11T04:49:02.831580651+00:00
-- elapsed: 292ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f301-3202-6e30-0014-58160002f0d6
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."DIM_DATES";
-- created_at: 2026-03-11T04:49:02.658994758+00:00
-- finished_at: 2026-03-11T04:49:02.915063696+00:00
-- elapsed: 256ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f301-3202-6e14-0014-58160002d26e
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."DIM_PRODUCTS";
-- created_at: 2026-03-11T04:49:02.839206625+00:00
-- finished_at: 2026-03-11T04:49:03.124001114+00:00
-- elapsed: 284ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f301-3202-6e30-0014-58160002f0da
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."FCT_EVENTS";
-- created_at: 2026-03-11T04:49:04.820455823+00:00
-- finished_at: 2026-03-11T04:49:05.184413525+00:00
-- elapsed: 363ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_USER_ID.60ee434024
-- query_id: 01c2f301-3202-6e30-0014-58160002f0de
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_USER_ID.60ee434024", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.861542153+00:00
-- finished_at: 2026-03-11T04:49:05.188149246+00:00
-- elapsed: 326ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_USER_SESSION.19d896670e
-- query_id: 01c2f301-3202-6e30-0014-58160002f0f6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_USER_SESSION.19d896670e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.870848252+00:00
-- finished_at: 2026-03-11T04:49:05.189065591+00:00
-- elapsed: 318ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_HOUR.6ef38f0fd6
-- query_id: 01c2f301-3202-6e30-0014-58160002f106
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_HOUR
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_HOUR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_HOUR.6ef38f0fd6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.864691091+00:00
-- finished_at: 2026-03-11T04:49:05.206021277+00:00
-- elapsed: 341ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_TIME.6fb849a6a6
-- query_id: 01c2f301-3202-6e14-0014-58160002d28e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_TIME.6fb849a6a6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.873211687+00:00
-- finished_at: 2026-03-11T04:49:05.212937930+00:00
-- elapsed: 339ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_L1.5043e574fa
-- query_id: 01c2f301-3202-6e30-0014-58160002f10e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_L1.5043e574fa", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.847323909+00:00
-- finished_at: 2026-03-11T04:49:05.217303944+00:00
-- elapsed: 369ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_CODE.0d8dad3555
-- query_id: 01c2f301-3202-6e30-0014-58160002f0e2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_CODE.0d8dad3555", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.822762132+00:00
-- finished_at: 2026-03-11T04:49:05.311352304+00:00
-- elapsed: 488ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_DATE.db2e1e2234
-- query_id: 01c2f301-3202-6e14-0014-58160002d276
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_DATE.db2e1e2234", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.897272410+00:00
-- finished_at: 2026-03-11T04:49:05.331509276+00:00
-- elapsed: 434ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_PRODUCT_ID.1f28c486d7
-- query_id: 01c2f301-3202-6e30-0014-58160002f11e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_PRODUCT_ID.1f28c486d7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.878838513+00:00
-- finished_at: 2026-03-11T04:49:05.336802961+00:00
-- elapsed: 457ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_LEAF.060e9e5228
-- query_id: 01c2f301-3202-6e30-0014-58160002f112
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_LEAF.060e9e5228", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.866310079+00:00
-- finished_at: 2026-03-11T04:49:05.337708711+00:00
-- elapsed: 471ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_PRICE.feae84d4e9
-- query_id: 01c2f301-3202-6e14-0014-58160002d286
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_PRICE.feae84d4e9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.864168+00:00
-- finished_at: 2026-03-11T04:49:05.347987029+00:00
-- elapsed: 483ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_fct_events_PRICE___0.437bc2a8d4
-- query_id: 01c2f301-3202-6e14-0014-58160002d282
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_fct_events_PRICE___0.437bc2a8d4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.820243563+00:00
-- finished_at: 2026-03-11T04:49:05.358194285+00:00
-- elapsed: 537ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_PURCHASE.b9b980b060
-- query_id: 01c2f301-3202-6e14-0014-58160002d27a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_PURCHASE.b9b980b060", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.881833644+00:00
-- finished_at: 2026-03-11T04:49:05.384703568+00:00
-- elapsed: 502ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_VIEW.5f15874792
-- query_id: 01c2f301-3202-6e14-0014-58160002d29e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_VIEW.5f15874792", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.845965731+00:00
-- finished_at: 2026-03-11T04:49:05.387360078+00:00
-- elapsed: 541ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_VIEW__0__1.5fe37935f3
-- query_id: 01c2f301-3202-6e30-0014-58160002f0ea
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_VIEW__0__1.5fe37935f3", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.880007061+00:00
-- finished_at: 2026-03-11T04:49:05.427356659+00:00
-- elapsed: 547ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_CART.c05a32eb8a
-- query_id: 01c2f301-3202-6e14-0014-58160002d296
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_CART.c05a32eb8a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.882058122+00:00
-- finished_at: 2026-03-11T04:49:05.453462245+00:00
-- elapsed: 571ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.unique_dim_products_PRODUCT_ID.5b3502e45d
-- query_id: 01c2f301-3202-6e30-0014-58160002f116
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    PRODUCT_ID as unique_field,
    count(*) as n_records

from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where PRODUCT_ID is not null
group by PRODUCT_ID
having count(*) > 1



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.unique_dim_products_PRODUCT_ID.5b3502e45d", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.910285777+00:00
-- finished_at: 2026-03-11T04:49:05.461183346+00:00
-- elapsed: 550ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_WEEKEND.929546128f
-- query_id: 01c2f301-3202-6e14-0014-58160002d2a6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_WEEKEND.929546128f", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.905084543+00:00
-- finished_at: 2026-03-11T04:49:05.469620368+00:00
-- elapsed: 564ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_PRODUCT_ID.de5c71a3af
-- query_id: 01c2f301-3202-6e14-0014-58160002d2aa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_PRODUCT_ID.de5c71a3af", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.914294591+00:00
-- finished_at: 2026-03-11T04:49:05.473842841+00:00
-- elapsed: 559ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_BRAND.a5ec5821ff
-- query_id: 01c2f301-3202-6e14-0014-58160002d2b2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_BRAND.a5ec5821ff", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.866723929+00:00
-- finished_at: 2026-03-11T04:49:05.475718185+00:00
-- elapsed: 608ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_WEEKEND__true__false.b010ff5ce4
-- query_id: 01c2f301-3202-6e30-0014-58160002f102
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_WEEKEND as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_WEEKEND

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_WEEKEND__true__false.b010ff5ce4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.864470648+00:00
-- finished_at: 2026-03-11T04:49:05.525602879+00:00
-- elapsed: 661ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_DAY_OF_WEEK.11e0aa6c4d
-- query_id: 01c2f301-3202-6e30-0014-58160002f10a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_OF_WEEK
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where DAY_OF_WEEK is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_DAY_OF_WEEK.11e0aa6c4d", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.884453472+00:00
-- finished_at: 2026-03-11T04:49:05.532505971+00:00
-- elapsed: 648ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_ID.579c7768dd
-- query_id: 01c2f301-3202-6e14-0014-58160002d2a2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_ID
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_ID.579c7768dd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.847941342+00:00
-- finished_at: 2026-03-11T04:49:05.539379324+00:00
-- elapsed: 691ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_DAY_NAME.437c012c02
-- query_id: 01c2f301-3202-6e30-0014-58160002f0e6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_NAME
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where DAY_NAME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_DAY_NAME.437c012c02", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.862673467+00:00
-- finished_at: 2026-03-11T04:49:05.544149889+00:00
-- elapsed: 681ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_QUARTER.6c5112ec42
-- query_id: 01c2f301-3202-6e30-0014-58160002f0fe
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select QUARTER
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where QUARTER is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_QUARTER.6c5112ec42", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.840380613+00:00
-- finished_at: 2026-03-11T04:49:05.561219780+00:00
-- elapsed: 720ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_789f24d42f36ec2f925c6d85d0999705.663b6a250f
-- query_id: 01c2f301-3202-6e14-0014-58160002d27e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_HOUR as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by EVENT_HOUR

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_789f24d42f36ec2f925c6d85d0999705.663b6a250f", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.864609878+00:00
-- finished_at: 2026-03-11T04:49:05.599362478+00:00
-- elapsed: 734ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.unique_dim_dates_EVENT_DATE.46b102f080
-- query_id: 01c2f301-3202-6e30-0014-58160002f0fa
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.unique_dim_dates_EVENT_DATE.46b102f080", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.867323282+00:00
-- finished_at: 2026-03-11T04:49:05.618027557+00:00
-- elapsed: 750ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_QUARTER__1__2__3__4.4182f7a3b9
-- query_id: 01c2f301-3202-6e14-0014-58160002d28a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        QUARTER as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by QUARTER

)

select *
from all_values
where value_field not in (
    '1','2','3','4'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_QUARTER__1__2__3__4.4182f7a3b9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.889610045+00:00
-- finished_at: 2026-03-11T04:49:05.630794253+00:00
-- elapsed: 741ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_IS_WEEKEND__true__false.8dd2703a3a
-- query_id: 01c2f301-3202-6e30-0014-58160002f11a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_WEEKEND as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by IS_WEEKEND

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_IS_WEEKEND__true__false.8dd2703a3a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.928568607+00:00
-- finished_at: 2026-03-11T04:49:05.719441113+00:00
-- elapsed: 790ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_da1fae6cd48ffa25502c8c26b839eb18.b4192735d6
-- query_id: 01c2f301-3202-6e14-0014-58160002d2be
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_da1fae6cd48ffa25502c8c26b839eb18.b4192735d6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.847029514+00:00
-- finished_at: 2026-03-11T04:49:05.736009581+00:00
-- elapsed: 888ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_DAY_OF_WEEK__0__1__2__3__4__5__6.7efae6f4dd
-- query_id: 01c2f301-3202-6e30-0014-58160002f0ee
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_OF_WEEK as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by DAY_OF_WEEK

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_DAY_OF_WEEK__0__1__2__3__4__5__6.7efae6f4dd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.861452824+00:00
-- finished_at: 2026-03-11T04:49:05.745788573+00:00
-- elapsed: 884ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_251de65f5724bcf7d23364b11dc72fed.66c9ba7fe7
-- query_id: 01c2f301-3202-6e30-0014-58160002f0f2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_NAME as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by DAY_NAME

)

select *
from all_values
where value_field not in (
    'Mon','Tue','Wed','Thu','Fri','Sat','Sun'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_251de65f5724bcf7d23364b11dc72fed.66c9ba7fe7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.878838158+00:00
-- finished_at: 2026-03-11T04:49:05.750712795+00:00
-- elapsed: 871ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_YEAR.6998c47062
-- query_id: 01c2f301-3202-6e14-0014-58160002d292
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where YEAR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_YEAR.6998c47062", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.910286028+00:00
-- finished_at: 2026-03-11T04:49:05.809484404+00:00
-- elapsed: 899ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_PURCHASE__0__1.692b30d4c4
-- query_id: 01c2f301-3202-6e30-0014-58160002f122
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_PURCHASE__0__1.692b30d4c4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.908262703+00:00
-- finished_at: 2026-03-11T04:49:05.844017768+00:00
-- elapsed: 935ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_MONTH.371af8f104
-- query_id: 01c2f301-3202-6e14-0014-58160002d2ae
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select MONTH
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where MONTH is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_MONTH.371af8f104", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.927148071+00:00
-- finished_at: 2026-03-11T04:49:05.862919726+00:00
-- elapsed: 935ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_EVENT_DATE.4e052aed85
-- query_id: 01c2f301-3202-6e14-0014-58160002d2b6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_EVENT_DATE.4e052aed85", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.929574568+00:00
-- finished_at: 2026-03-11T04:49:05.875584166+00:00
-- elapsed: 946ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_CART__0__1.5c40c41c0c
-- query_id: 01c2f301-3202-6e14-0014-58160002d2ba
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_CART as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_CART

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_CART__0__1.5c40c41c0c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.942016981+00:00
-- finished_at: 2026-03-11T04:49:05.877226807+00:00
-- elapsed: 935ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_IS_WEEKEND.c97a7435a9
-- query_id: 01c2f301-3202-6e30-0014-58160002f126
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_IS_WEEKEND.c97a7435a9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.825939261+00:00
-- finished_at: 2026-03-11T04:49:05.959691755+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.relationships_fct_events_EVENT_DATE__EVENT_DATE__ref_dim_dates_.3f7b055ece
-- query_id: 01c2f301-3202-6e14-0014-58160002d272
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.relationships_fct_events_EVENT_DATE__EVENT_DATE__ref_dim_dates_.3f7b055ece", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-11T04:49:04.884448725+00:00
-- finished_at: 2026-03-11T04:49:06.009433162+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.relationships_fct_events_14461f4a74831fe72206b0fc8a0392ab.bb7991c1fa
-- query_id: 01c2f301-3202-6e14-0014-58160002d29a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select PRODUCT_ID as from_field
    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    where PRODUCT_ID is not null
),

parent as (
    select PRODUCT_ID as to_field
    from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.relationships_fct_events_14461f4a74831fe72206b0fc8a0392ab.bb7991c1fa", "profile_name": "user", "target_name": "dev"} */;

============================== 431720e9-b089-4e6f-853f-5ebdda9f1330 ==============================
-- created_at: 2026-03-13T04:44:50.684985077+00:00
-- finished_at: 2026-03-13T04:44:50.979370558+00:00
-- elapsed: 294ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2fe3c-3202-6e14-0014-581600096fbe
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-13T04:44:53.046074885+00:00
-- finished_at: 2026-03-13T04:44:53.324118050+00:00
-- elapsed: 278ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2fe3c-3202-7000-0014-58160009896a
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:44:54.329080022+00:00
-- finished_at: 2026-03-13T04:44:54.640664045+00:00
-- elapsed: 311ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2fe3c-3202-6e14-0014-581600096fc2
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-13T04:44:54.388378894+00:00
-- finished_at: 2026-03-13T04:44:54.666773535+00:00
-- elapsed: 278ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2fe3c-3202-6e14-0014-581600096fc6
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-13T04:44:54.393477372+00:00
-- finished_at: 2026-03-13T04:44:54.685341246+00:00
-- elapsed: 291ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2fe3c-3202-7000-0014-58160009896e
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-13T04:44:54.687711547+00:00
-- finished_at: 2026-03-13T04:44:57.783303422+00:00
-- elapsed: 3.1s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2fe3c-3202-6e14-0014-581600096fce
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    
    
    
    as (

SELECT DISTINCT
    EVENT_DATE,
    DAY_NAME,
    DAY_OF_WEEK,
    IS_WEEKEND,
    MONTH(EVENT_DATE)                                        AS MONTH,
    QUARTER(EVENT_DATE)                                      AS QUARTER,
    YEAR(EVENT_DATE)                                         AS YEAR
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
ORDER BY EVENT_DATE
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_dates", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:44:54.669220002+00:00
-- finished_at: 2026-03-13T04:45:00.001231656+00:00
-- elapsed: 5.3s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2fe3c-3202-7000-0014-581600098972
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_products
    
    
    
    as (

SELECT DISTINCT
    PRODUCT_ID,
    CATEGORY_ID,
    CATEGORY_CODE,
    CATEGORY_L1,
    CATEGORY_LEAF,
    BRAND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_products", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:44:54.643293969+00:00
-- finished_at: 2026-03-13T04:45:08.728890511+00:00
-- elapsed: 14.1s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2fe3c-3202-6e14-0014-581600096fca
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    
    
    
    as (

SELECT
    EVENT_TIME,
    EVENT_DATE,
    EVENT_HOUR,
    PRODUCT_ID,
    USER_ID,
    USER_SESSION,
    PRICE,
    IS_VIEW,
    IS_CART,
    IS_PURCHASE
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.fct_events", "profile_name": "user", "target_name": "dev"} */;

============================== 47e0dbfb-7b03-49d1-b441-8fbdefcd2b01 ==============================
-- created_at: 2026-03-13T04:45:43.566824583+00:00
-- finished_at: 2026-03-13T04:45:43.852243274+00:00
-- elapsed: 285ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2fe3d-3202-6e14-0014-581600096fd2
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."RAW"."EVENTS_RAW";
-- created_at: 2026-03-13T04:45:45.886164367+00:00
-- finished_at: 2026-03-13T04:45:46.151598752+00:00
-- elapsed: 265ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2fe3d-3202-6e14-0014-581600096fd6
-- desc: execute adapter call
show terse schemas in database ECOMMERCE_ANALYTICS
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:45:47.286938269+00:00
-- finished_at: 2026-03-13T04:45:47.562881386+00:00
-- elapsed: 275ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2fe3d-3202-6e14-0014-581600096fda
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "ECOMMERCE_ANALYTICS"."DBT_DEV" LIMIT 10000;
-- created_at: 2026-03-13T04:45:47.566045156+00:00
-- finished_at: 2026-03-13T04:46:19.572386642+00:00
-- elapsed: 32.0s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.stg_events_clean
-- query_id: 01c2fe3d-3202-6e14-0014-581600096fde
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    
    
    
    as (

WITH source AS (
    SELECT * FROM ECOMMERCE_ANALYTICS.RAW.EVENTS_RAW
),
brand_fixed AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        COALESCE(
            BRAND,
            FIRST_VALUE(BRAND) IGNORE NULLS OVER (
                PARTITION BY PRODUCT_ID
                ORDER BY EVENT_TIME
            ),
            'unknown'
        )                        AS BRAND,
        COALESCE(
            CATEGORY_CODE, 
            'unknown'
        )                        AS CATEGORY_CODE
    FROM source
    WHERE USER_SESSION IS NOT NULL
        AND PRODUCT_ID NOT IN (
            SELECT PRODUCT_ID
            FROM ECOMMERCE_ANALYTICS.RAW.EVENTS_RAW
            WHERE BRAND IS NOT NULL
            GROUP BY PRODUCT_ID
            HAVING COUNT(DISTINCT BRAND) > 1
        )
),
cleaned AS (
    SELECT
        EVENT_TIME,
        EVENT_TYPE,
        PRODUCT_ID,
        CATEGORY_ID,
        USER_ID,
        USER_SESSION,
        PRICE,
        BRAND,
        CATEGORY_CODE,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.', 1), ''),
            'unknown')           AS CATEGORY_L1,
        COALESCE(
            NULLIF(SPLIT_PART(CATEGORY_CODE, '.',
                ARRAY_SIZE(SPLIT(CATEGORY_CODE, '.'))), ''),
            'unknown')           AS CATEGORY_LEAF,
        DATE(EVENT_TIME)         AS EVENT_DATE,
        HOUR(EVENT_TIME)         AS EVENT_HOUR,
        DAYNAME(EVENT_TIME)      AS DAY_NAME,
        DAYOFWEEK(EVENT_TIME)    AS DAY_OF_WEEK,
        CASE WHEN DAYOFWEEK(EVENT_TIME)
             IN (0, 6) THEN TRUE
             ELSE FALSE END      AS IS_WEEKEND,
        MONTH(EVENT_TIME)        AS EVENT_MONTH,
        QUARTER(EVENT_TIME)      AS EVENT_QUARTER,
        YEAR(EVENT_TIME)         AS EVENT_YEAR,
        CASE WHEN EVENT_TYPE = 'view'
             THEN 1 ELSE 0 END   AS IS_VIEW,
        CASE WHEN EVENT_TYPE = 'cart'
             THEN 1 ELSE 0 END   AS IS_CART,
        CASE WHEN EVENT_TYPE = 'purchase'
             THEN 1 ELSE 0 END   AS IS_PURCHASE
    FROM brand_fixed
)
SELECT * FROM cleaned
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.stg_events_clean", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:46:20.777400212+00:00
-- finished_at: 2026-03-13T04:46:23.757143848+00:00
-- elapsed: 3.0s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_dates
-- query_id: 01c2fe3e-3202-6e14-0014-581600096fe2
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    
    
    
    as (

SELECT DISTINCT
    EVENT_DATE,
    DAY_NAME,
    DAY_OF_WEEK,
    IS_WEEKEND,
    MONTH(EVENT_DATE)                                        AS MONTH,
    QUARTER(EVENT_DATE)                                      AS QUARTER,
    YEAR(EVENT_DATE)                                         AS YEAR
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
ORDER BY EVENT_DATE
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_dates", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:46:20.779455789+00:00
-- finished_at: 2026-03-13T04:46:25.956681986+00:00
-- elapsed: 5.2s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.dim_products
-- query_id: 01c2fe3e-3202-7000-0014-58160009897a
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.dim_products
    
    
    
    as (

SELECT DISTINCT
    PRODUCT_ID,
    CATEGORY_ID,
    CATEGORY_CODE,
    CATEGORY_L1,
    CATEGORY_LEAF,
    BRAND
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.dim_products", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:46:19.577182420+00:00
-- finished_at: 2026-03-13T04:46:33.063026200+00:00
-- elapsed: 13.5s
-- outcome: success
-- dialect: snowflake
-- node_id: model.ecommerce_analytics.fct_events
-- query_id: 01c2fe3e-3202-7000-0014-581600098976
-- desc: execute adapter call
create or replace transient  table ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    
    
    
    as (

SELECT
    EVENT_TIME,
    EVENT_DATE,
    EVENT_HOUR,
    PRODUCT_ID,
    USER_ID,
    USER_SESSION,
    PRICE,
    IS_VIEW,
    IS_CART,
    IS_PURCHASE
FROM ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    )

/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.ecommerce_analytics.fct_events", "profile_name": "user", "target_name": "dev"} */;

============================== e6398904-b102-485a-b0f3-c64b9406d700 ==============================
-- created_at: 2026-03-13T04:47:04.990824596+00:00
-- finished_at: 2026-03-13T04:47:05.250767493+00:00
-- elapsed: 259ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2fe3f-3202-6e14-0014-581600096fe6
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."DIM_PRODUCTS";
-- created_at: 2026-03-13T04:47:04.990376070+00:00
-- finished_at: 2026-03-13T04:47:05.252047827+00:00
-- elapsed: 261ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2fe3f-3202-7000-0014-58160009897e
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."DIM_DATES";
-- created_at: 2026-03-13T04:47:05.258100996+00:00
-- finished_at: 2026-03-13T04:47:05.497284878+00:00
-- elapsed: 239ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2fe3f-3202-7000-0014-581600098982
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."STG_EVENTS_CLEAN";
-- created_at: 2026-03-13T04:47:05.257743872+00:00
-- finished_at: 2026-03-13T04:47:05.981798870+00:00
-- elapsed: 724ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2fe3f-3202-7000-0014-581600098986
-- desc: Get table schema
describe table "ECOMMERCE_ANALYTICS"."DBT_DEV"."FCT_EVENTS";
-- created_at: 2026-03-13T04:47:07.393114785+00:00
-- finished_at: 2026-03-13T04:47:07.683621487+00:00
-- elapsed: 290ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a
-- query_id: 01c2fe3f-3202-7000-0014-581600098992
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_CODE.6f5894698a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.408364190+00:00
-- finished_at: 2026-03-13T04:47:07.690802269+00:00
-- elapsed: 282ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5
-- query_id: 01c2fe3f-3202-7000-0014-581600098996
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_BRAND.867f655bd5", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.420283212+00:00
-- finished_at: 2026-03-13T04:47:07.703519988+00:00
-- elapsed: 283ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_HOUR.76ec27905a
-- query_id: 01c2fe3f-3202-6e14-0014-581600096ffa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_HOUR
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_HOUR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_HOUR.76ec27905a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.421008729+00:00
-- finished_at: 2026-03-13T04:47:07.726028788+00:00
-- elapsed: 305ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02
-- query_id: 01c2fe3f-3202-7000-0014-58160009899e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_MONTH
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_MONTH is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_MONTH.97c7e16c02", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.421036385+00:00
-- finished_at: 2026-03-13T04:47:07.727082364+00:00
-- elapsed: 306ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561
-- query_id: 01c2fe3f-3202-7000-0014-5816000989a2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_CART.4cf260d561", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.428526666+00:00
-- finished_at: 2026-03-13T04:47:07.729508164+00:00
-- elapsed: 300ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_DAY_NAME.9dd925a4e2
-- query_id: 01c2fe3f-3202-7000-0014-5816000989a6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_NAME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where DAY_NAME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_DAY_NAME.9dd925a4e2", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.434823457+00:00
-- finished_at: 2026-03-13T04:47:07.731853579+00:00
-- elapsed: 297ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7
-- query_id: 01c2fe3f-3202-6e14-0014-581600099012
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_YEAR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_YEAR.7986bfa1b7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.446534026+00:00
-- finished_at: 2026-03-13T04:47:07.742700045+00:00
-- elapsed: 296ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_DATE.728f2410bc
-- query_id: 01c2fe3f-3202-7000-0014-5816000989ae
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_DATE.728f2410bc", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.435636595+00:00
-- finished_at: 2026-03-13T04:47:07.745085683+00:00
-- elapsed: 309ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd
-- query_id: 01c2fe3f-3202-6e14-0014-581600097002
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_WEEKEND.9aac7288fd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.438227704+00:00
-- finished_at: 2026-03-13T04:47:07.748755108+00:00
-- elapsed: 310ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da
-- query_id: 01c2fe3f-3202-7000-0014-5816000989aa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_VIEW.297175b5da", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.460731243+00:00
-- finished_at: 2026-03-13T04:47:07.753228711+00:00
-- elapsed: 292ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380
-- query_id: 01c2fe3f-3202-7000-0014-5816000989ba
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_LEAF.cec2da0380", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.443555326+00:00
-- finished_at: 2026-03-13T04:47:07.767399793+00:00
-- elapsed: 323ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4
-- query_id: 01c2fe3f-3202-6e14-0014-581600099026
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRICE.f523c46fd4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.439638804+00:00
-- finished_at: 2026-03-13T04:47:07.825512478+00:00
-- elapsed: 385ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526
-- query_id: 01c2fe3f-3202-7000-0014-5816000989b6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_PRODUCT_ID.84c04e7526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.386377487+00:00
-- finished_at: 2026-03-13T04:47:07.852728953+00:00
-- elapsed: 466ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_DATE.db2e1e2234
-- query_id: 01c2fe3f-3202-7000-0014-58160009898a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_DATE.db2e1e2234", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.468737131+00:00
-- finished_at: 2026-03-13T04:47:07.891228901+00:00
-- elapsed: 422ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_USER_ID.60ee434024
-- query_id: 01c2fe3f-3202-7000-0014-5816000989c2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_USER_ID.60ee434024", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.390573163+00:00
-- finished_at: 2026-03-13T04:47:07.929321230+00:00
-- elapsed: 538ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_ID.579c7768dd
-- query_id: 01c2fe3f-3202-7000-0014-58160009898e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_ID
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_ID.579c7768dd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.414919913+00:00
-- finished_at: 2026-03-13T04:47:07.959807285+00:00
-- elapsed: 544ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.unique_dim_dates_EVENT_DATE.46b102f080
-- query_id: 01c2fe3f-3202-7000-0014-58160009899a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.unique_dim_dates_EVENT_DATE.46b102f080", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.445014594+00:00
-- finished_at: 2026-03-13T04:47:07.971164679+00:00
-- elapsed: 526ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_MONTH.371af8f104
-- query_id: 01c2fe3f-3202-7000-0014-5816000989b2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select MONTH
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where MONTH is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_MONTH.371af8f104", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.410146491+00:00
-- finished_at: 2026-03-13T04:47:08.037929166+00:00
-- elapsed: 627ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_674a138e330566a305c49db295b6b2e8.9baf28cdf8
-- query_id: 01c2fe3f-3202-6e14-0014-581600096ff2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_HOUR as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by EVENT_HOUR

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_674a138e330566a305c49db295b6b2e8.9baf28cdf8", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.493315279+00:00
-- finished_at: 2026-03-13T04:47:08.041372232+00:00
-- elapsed: 548ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_DAY_OF_WEEK__0__1__2__3__4__5__6.7efae6f4dd
-- query_id: 01c2fe3f-3202-7000-0014-5816000989c6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_OF_WEEK as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by DAY_OF_WEEK

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_DAY_OF_WEEK__0__1__2__3__4__5__6.7efae6f4dd", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.485814576+00:00
-- finished_at: 2026-03-13T04:47:08.064402011+00:00
-- elapsed: 578ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_DAY_OF_WEEK.11e0aa6c4d
-- query_id: 01c2fe3f-3202-7000-0014-5816000989ca
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_OF_WEEK
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where DAY_OF_WEEK is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_DAY_OF_WEEK.11e0aa6c4d", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.484957381+00:00
-- finished_at: 2026-03-13T04:47:08.065336896+00:00
-- elapsed: 580ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_L1.5043e574fa
-- query_id: 01c2fe3f-3202-7000-0014-5816000989ce
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_L1.5043e574fa", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.436471148+00:00
-- finished_at: 2026-03-13T04:47:08.068395437+00:00
-- elapsed: 631ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62
-- query_id: 01c2fe3f-3202-6e14-0014-58160009900e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_PURCHASE__0__1.a2a469dc62", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.409941925+00:00
-- finished_at: 2026-03-13T04:47:08.070571428+00:00
-- elapsed: 660ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_IS_WEEKEND.c97a7435a9
-- query_id: 01c2fe3f-3202-6e14-0014-581600096ffe
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_WEEKEND
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where IS_WEEKEND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_IS_WEEKEND.c97a7435a9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.443600291+00:00
-- finished_at: 2026-03-13T04:47:08.074409550+00:00
-- elapsed: 630ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c
-- query_id: 01c2fe3f-3202-6e14-0014-58160009902a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_WEEKEND__true__false.fffbbbf59c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.448145776+00:00
-- finished_at: 2026-03-13T04:47:08.075453406+00:00
-- elapsed: 627ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_da1fae6cd48ffa25502c8c26b839eb18.b4192735d6
-- query_id: 01c2fe3f-3202-6e14-0014-58160009901a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_da1fae6cd48ffa25502c8c26b839eb18.b4192735d6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.468295319+00:00
-- finished_at: 2026-03-13T04:47:08.076617643+00:00
-- elapsed: 608ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_QUARTER.6c5112ec42
-- query_id: 01c2fe3f-3202-6e14-0014-581600099032
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select QUARTER
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where QUARTER is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_QUARTER.6c5112ec42", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.471091484+00:00
-- finished_at: 2026-03-13T04:47:08.077652729+00:00
-- elapsed: 606ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_VIEW__0__1.5fe37935f3
-- query_id: 01c2fe3f-3202-7000-0014-5816000989be
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_VIEW__0__1.5fe37935f3", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.419878869+00:00
-- finished_at: 2026-03-13T04:47:08.091026372+00:00
-- elapsed: 671ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_DAY_NAME.437c012c02
-- query_id: 01c2fe3f-3202-6e14-0014-581600096ff6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_NAME
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where DAY_NAME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_DAY_NAME.437c012c02", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.501166534+00:00
-- finished_at: 2026-03-13T04:47:08.100871513+00:00
-- elapsed: 599ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b
-- query_id: 01c2fe3f-3202-7000-0014-5816000989d6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TYPE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TYPE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TYPE.9b71ecb01b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.400114353+00:00
-- finished_at: 2026-03-13T04:47:08.101841928+00:00
-- elapsed: 701ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_EVENT_DATE.4e052aed85
-- query_id: 01c2fe3f-3202-6e14-0014-581600096fee
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_DATE
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where EVENT_DATE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_EVENT_DATE.4e052aed85", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.501878837+00:00
-- finished_at: 2026-03-13T04:47:08.103819628+00:00
-- elapsed: 601ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_EVENT_QUARTER__1__2__3__4.74a0370526
-- query_id: 01c2fe3f-3202-7000-0014-5816000989d2
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_EVENT_QUARTER__1__2__3__4.74a0370526", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.509552247+00:00
-- finished_at: 2026-03-13T04:47:08.111435129+00:00
-- elapsed: 601ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177
-- query_id: 01c2fe3f-3202-7000-0014-5816000989de
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_stg_events_clean_PRICE___0.30b5188177", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.494418088+00:00
-- finished_at: 2026-03-13T04:47:08.121608695+00:00
-- elapsed: 627ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43
-- query_id: 01c2fe3f-3202-6e14-0014-581600099042
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_ID.0631b08d43", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.393362834+00:00
-- finished_at: 2026-03-13T04:47:08.144253900+00:00
-- elapsed: 750ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_HOUR.6ef38f0fd6
-- query_id: 01c2fe3f-3202-6e14-0014-581600096fea
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_HOUR
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_HOUR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_HOUR.6ef38f0fd6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.494126158+00:00
-- finished_at: 2026-03-13T04:47:08.145320822+00:00
-- elapsed: 651ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_PURCHASE.b9b980b060
-- query_id: 01c2fe3f-3202-6e14-0014-581600099046
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_PURCHASE.b9b980b060", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.510681760+00:00
-- finished_at: 2026-03-13T04:47:08.154534798+00:00
-- elapsed: 643ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_CART.c05a32eb8a
-- query_id: 01c2fe3f-3202-7000-0014-5816000989e6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_CART
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_CART is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_CART.c05a32eb8a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.438816824+00:00
-- finished_at: 2026-03-13T04:47:08.163232542+00:00
-- elapsed: 724ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_251de65f5724bcf7d23364b11dc72fed.66c9ba7fe7
-- query_id: 01c2fe3f-3202-6e14-0014-581600099006
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_NAME as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by DAY_NAME

)

select *
from all_values
where value_field not in (
    'Mon','Tue','Wed','Thu','Fri','Sat','Sun'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_251de65f5724bcf7d23364b11dc72fed.66c9ba7fe7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.487493824+00:00
-- finished_at: 2026-03-13T04:47:08.174411651+00:00
-- elapsed: 686ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_IS_WEEKEND__true__false.8dd2703a3a
-- query_id: 01c2fe3f-3202-6e14-0014-58160009903e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_WEEKEND as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by IS_WEEKEND

)

select *
from all_values
where value_field not in (
    'True','False'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_IS_WEEKEND__true__false.8dd2703a3a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.515009287+00:00
-- finished_at: 2026-03-13T04:47:08.198584399+00:00
-- elapsed: 683ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_IS_VIEW.5f15874792
-- query_id: 01c2fe3f-3202-6e14-0014-581600099052
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_VIEW
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where IS_VIEW is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_IS_VIEW.5f15874792", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.523470582+00:00
-- finished_at: 2026-03-13T04:47:08.207480340+00:00
-- elapsed: 684ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4
-- query_id: 01c2fe3f-3202-6e14-0014-581600099056
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_QUARTER
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_QUARTER is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_QUARTER.50d20671b4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.525294496+00:00
-- finished_at: 2026-03-13T04:47:08.234458263+00:00
-- elapsed: 709ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_LEAF.060e9e5228
-- query_id: 01c2fe3f-3202-7000-0014-5816000989ea
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_LEAF
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_LEAF is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_LEAF.060e9e5228", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.464835871+00:00
-- finished_at: 2026-03-13T04:47:08.238441188+00:00
-- elapsed: 773ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_PRICE.feae84d4e9
-- query_id: 01c2fe3f-3202-6e14-0014-581600099036
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRICE
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where PRICE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_PRICE.feae84d4e9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.522731234+00:00
-- finished_at: 2026-03-13T04:47:08.241790940+00:00
-- elapsed: 719ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_dim_dates_QUARTER__1__2__3__4.4182f7a3b9
-- query_id: 01c2fe3f-3202-7000-0014-5816000989e2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        QUARTER as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
    group by QUARTER

)

select *
from all_values
where value_field not in (
    '1','2','3','4'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_dim_dates_QUARTER__1__2__3__4.4182f7a3b9", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.443581278+00:00
-- finished_at: 2026-03-13T04:47:08.255726440+00:00
-- elapsed: 812ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_EVENT_TIME.6fb849a6a6
-- query_id: 01c2fe3f-3202-6e14-0014-581600099022
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_EVENT_TIME.6fb849a6a6", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.527142005+00:00
-- finished_at: 2026-03-13T04:47:08.347451265+00:00
-- elapsed: 820ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70
-- query_id: 01c2fe3f-3202-6e14-0014-581600099062
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_EVENT_TIME.cb6d86cd70", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.509334322+00:00
-- finished_at: 2026-03-13T04:47:08.460243398+00:00
-- elapsed: 950ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57
-- query_id: 01c2fe3f-3202-6e14-0014-58160009904e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_VIEW as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by IS_VIEW

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_VIEW__0__1.4e0747bb57", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.508417482+00:00
-- finished_at: 2026-03-13T04:47:08.482072662+00:00
-- elapsed: 973ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_PRODUCT_ID.1f28c486d7
-- query_id: 01c2fe3f-3202-6e14-0014-58160009904a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_PRODUCT_ID.1f28c486d7", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.528066127+00:00
-- finished_at: 2026-03-13T04:47:08.512566114+00:00
-- elapsed: 984ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_DAY_OF_WEEK.890fa98e34
-- query_id: 01c2fe3f-3202-6e14-0014-58160009905a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_OF_WEEK
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where DAY_OF_WEEK is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_DAY_OF_WEEK.890fa98e34", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.443128641+00:00
-- finished_at: 2026-03-13T04:47:08.513436768+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_PURCHASE__0__1.692b30d4c4
-- query_id: 01c2fe3f-3202-6e14-0014-581600099016
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_PURCHASE as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_PURCHASE

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_PURCHASE__0__1.692b30d4c4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.521017192+00:00
-- finished_at: 2026-03-13T04:47:08.515987138+00:00
-- elapsed: 994ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296
-- query_id: 01c2fe3f-3202-7000-0014-5816000989f2
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_L1
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where CATEGORY_L1 is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_CATEGORY_L1.7ffcc1c296", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.535853116+00:00
-- finished_at: 2026-03-13T04:47:08.529791786+00:00
-- elapsed: 993ms
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e
-- query_id: 01c2fe3f-3202-7000-0014-5816000989fe
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_USER_SESSION.268bea4b6e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.438578253+00:00
-- finished_at: 2026-03-13T04:47:08.532224446+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_BRAND.a5ec5821ff
-- query_id: 01c2fe3f-3202-6e14-0014-58160009901e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select BRAND
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where BRAND is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_BRAND.a5ec5821ff", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.532748847+00:00
-- finished_at: 2026-03-13T04:47:08.535334389+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73
-- query_id: 01c2fe3f-3202-7000-0014-5816000989ee
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_clean_IS_CART__0__1.072f35bd73", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.518655665+00:00
-- finished_at: 2026-03-13T04:47:08.556350435+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_789f24d42f36ec2f925c6d85d0999705.663b6a250f
-- query_id: 01c2fe3f-3202-7000-0014-5816000989da
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_HOUR as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by EVENT_HOUR

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_789f24d42f36ec2f925c6d85d0999705.663b6a250f", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.545326583+00:00
-- finished_at: 2026-03-13T04:47:08.564142764+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.dbt_utils_expression_is_true_fct_events_PRICE___0.437bc2a8d4
-- query_id: 01c2fe3f-3202-7000-0014-5816000989fa
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events

where not(PRICE >= 0)


  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.dbt_utils_expression_is_true_fct_events_PRICE___0.437bc2a8d4", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.537608414+00:00
-- finished_at: 2026-03-13T04:47:08.577529172+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a
-- query_id: 01c2fe3f-3202-6e14-0014-58160009906a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select IS_PURCHASE
from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
where IS_PURCHASE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_stg_events_clean_IS_PURCHASE.5e01b4d01a", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.529489206+00:00
-- finished_at: 2026-03-13T04:47:08.588563406+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_products_CATEGORY_CODE.0d8dad3555
-- query_id: 01c2fe3f-3202-6e14-0014-58160009905e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select CATEGORY_CODE
from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where CATEGORY_CODE is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_products_CATEGORY_CODE.0d8dad3555", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.437356775+00:00
-- finished_at: 2026-03-13T04:47:08.599793359+00:00
-- elapsed: 1.2s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_fct_events_IS_CART__0__1.5c40c41c0c
-- query_id: 01c2fe3f-3202-6e14-0014-58160009900a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        IS_CART as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    group by IS_CART

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_fct_events_IS_CART__0__1.5c40c41c0c", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.550843331+00:00
-- finished_at: 2026-03-13T04:47:08.600643070+00:00
-- elapsed: 1.0s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_USER_SESSION.19d896670e
-- query_id: 01c2fe3f-3202-6e14-0014-58160009906e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_SESSION
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where USER_SESSION is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_USER_SESSION.19d896670e", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.547555950+00:00
-- finished_at: 2026-03-13T04:47:08.627453150+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_ee9c969df76ee41259a90537e9c9d129.543a8ae63b
-- query_id: 01c2fe3f-3202-6e14-0014-581600099072
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        EVENT_MONTH as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by EVENT_MONTH

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6','7','8','9','10','11','12'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_ee9c969df76ee41259a90537e9c9d129.543a8ae63b", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.563996711+00:00
-- finished_at: 2026-03-13T04:47:08.645109146+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_fct_events_PRODUCT_ID.de5c71a3af
-- query_id: 01c2fe3f-3202-7000-0014-581600098a02
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PRODUCT_ID
from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
where PRODUCT_ID is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_fct_events_PRODUCT_ID.de5c71a3af", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.452009104+00:00
-- finished_at: 2026-03-13T04:47:08.698510419+00:00
-- elapsed: 1.2s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.unique_dim_products_PRODUCT_ID.5b3502e45d
-- query_id: 01c2fe3f-3202-6e14-0014-58160009902e
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    PRODUCT_ID as unique_field,
    count(*) as n_records

from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
where PRODUCT_ID is not null
group by PRODUCT_ID
having count(*) > 1



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.unique_dim_products_PRODUCT_ID.5b3502e45d", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.565845707+00:00
-- finished_at: 2026-03-13T04:47:08.701583693+00:00
-- elapsed: 1.1s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.not_null_dim_dates_YEAR.6998c47062
-- query_id: 01c2fe3f-3202-6e14-0014-581600099076
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select YEAR
from ECOMMERCE_ANALYTICS.dbt_dev.dim_dates
where YEAR is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.not_null_dim_dates_YEAR.6998c47062", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.535747092+00:00
-- finished_at: 2026-03-13T04:47:08.939911564+00:00
-- elapsed: 1.4s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_0a4a62d43b3c5bb84f106667b523ab53.7686e7eae2
-- query_id: 01c2fe3f-3202-7000-0014-5816000989f6
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        DAY_OF_WEEK as value_field,
        count(*) as n_records

    from ECOMMERCE_ANALYTICS.dbt_dev.stg_events_clean
    group by DAY_OF_WEEK

)

select *
from all_values
where value_field not in (
    '0','1','2','3','4','5','6'
)



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_0a4a62d43b3c5bb84f106667b523ab53.7686e7eae2", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.572043794+00:00
-- finished_at: 2026-03-13T04:47:09.174461465+00:00
-- elapsed: 1.6s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.relationships_fct_events_EVENT_DATE__EVENT_DATE__ref_dim_dates_.3f7b055ece
-- query_id: 01c2fe3f-3202-7000-0014-581600098a06
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.relationships_fct_events_EVENT_DATE__EVENT_DATE__ref_dim_dates_.3f7b055ece", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.579017225+00:00
-- finished_at: 2026-03-13T04:47:09.236560319+00:00
-- elapsed: 1.7s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187
-- query_id: 01c2fe3f-3202-6e14-0014-58160009907a
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_cbc2848c913527b3828c4a99a9b39c74.bd978ee187", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.465714221+00:00
-- finished_at: 2026-03-13T04:47:09.239979188+00:00
-- elapsed: 1.8s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.relationships_fct_events_14461f4a74831fe72206b0fc8a0392ab.bb7991c1fa
-- query_id: 01c2fe3f-3202-6e14-0014-58160009903a
-- desc: execute adapter call
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select PRODUCT_ID as from_field
    from ECOMMERCE_ANALYTICS.dbt_dev.fct_events
    where PRODUCT_ID is not null
),

parent as (
    select PRODUCT_ID as to_field
    from ECOMMERCE_ANALYTICS.dbt_dev.dim_products
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.relationships_fct_events_14461f4a74831fe72206b0fc8a0392ab.bb7991c1fa", "profile_name": "user", "target_name": "dev"} */;
-- created_at: 2026-03-13T04:47:07.548128297+00:00
-- finished_at: 2026-03-13T04:47:09.322536174+00:00
-- elapsed: 1.8s
-- outcome: success
-- dialect: snowflake
-- node_id: test.ecommerce_analytics.accepted_values_stg_events_cle_206d1dd4470d87c187a8c7e68f7cc25a.c42471ca4a
-- query_id: 01c2fe3f-3202-6e14-0014-581600099066
-- desc: execute adapter call
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
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.ecommerce_analytics.accepted_values_stg_events_cle_206d1dd4470d87c187a8c7e68f7cc25a.c42471ca4a", "profile_name": "user", "target_name": "dev"} */;



  
    



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
;




  
{{ config(materialized='table') }}

SELECT DISTINCT
    PRODUCT_ID,
    CATEGORY_ID,
    CATEGORY_CODE,
    CATEGORY_L1,
    CATEGORY_LEAF,
    BRAND
FROM {{ ref('stg_events_clean') }}
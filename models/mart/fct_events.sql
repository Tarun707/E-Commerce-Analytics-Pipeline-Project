{{ config(materialized='table') }}

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
FROM {{ ref('stg_events_clean') }}
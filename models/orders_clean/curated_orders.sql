{{
  config(
    materialized='table'
  )
}}
with curated_orders as (
        select id, user_id, order_date, status
        from {{ ref('clean_orders') }}
        where aggr_marker = 1 and not null_columns
)
select * from curated_orders

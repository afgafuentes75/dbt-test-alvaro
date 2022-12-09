{{
  config(
    materialized='table'
  )
}}
with rejected_orders as (
        select *
        from {{ ref('clean_orders') }}
        where aggr_marker > 1 or null_columns
)
select * from rejected_orders

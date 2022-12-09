{{
  config(
    materialized='table'
  )
}}
with rejected_payments as (
        select *
        from {{ ref('clean_payments') }}
        where aggr_marker > 1 or null_columns
)
select * from rejected_payments

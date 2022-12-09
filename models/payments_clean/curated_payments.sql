{{
  config(
    materialized='table'
  )
}}
with curated_payments as (
        select id, order_id, payment_method, amount
        from {{ ref('clean_payments') }}
        where aggr_marker = 1 and not null_columns
)
select * from curated_payments

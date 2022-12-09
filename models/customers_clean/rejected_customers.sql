{{
  config(
    materialized='table'
  )
}}
with rejected_customers as (
	select * 
        from {{ ref('clean_customers') }}
        where aggr_marker > 1 or null_columns
) 
select * from rejected_customers

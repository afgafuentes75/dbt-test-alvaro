{{
  config(
    materialized='table'
  )
}}
with curated_customers as (
	select id, first_name, last_name 
        from {{ ref('clean_customers') }}
        where aggr_marker = 1 and not null_columns
) 
select * from curated_customers

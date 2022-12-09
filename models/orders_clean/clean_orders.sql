with aggregated as (
    select id, row_number() over(partition by id order by id) as aggr_marker
        from dbt.raw_orders
),
original_data as (
        select id, user_id, order_date, status, id is null or user_id is null or order_date is null or status is null as null_columns
        from dbt.raw_orders
),
sorted_data as (
        select distinct original_data.id, original_data.user_id, original_data.order_date, original_data.status, aggregated.aggr_marker, original_data.null_columns from
        aggregated inner join original_data
        on aggregated.id = original_data.id 
)
select * from sorted_data

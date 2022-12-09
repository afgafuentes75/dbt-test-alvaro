with aggregated as (
    select id, row_number() over(partition by id order by id) as aggr_marker
        from dbt.raw_payments
),
original_data as (
        select id, order_id, payment_method, amount,  id is null or order_id is null or amount is null or payment_method is null as null_columns
        from dbt.raw_payments
),
sorted_data as (
        select distinct original_data.id, original_data.order_id, original_data.payment_method, original_data.amount, aggregated.aggr_marker, original_data.null_columns from
        aggregated inner join original_data
        on aggregated.id = original_data.id 
)
select * from sorted_data

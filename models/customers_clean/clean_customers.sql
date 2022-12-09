with aggregated as (
    select first_name, last_name, row_number() over(partition by first_name, last_name order by id) as aggr_marker
	from dbt.raw_customers
), 
original_data as (
	select id, first_name, last_name,  id is null || first_name is null || last_name is null as null_columns  
	from dbt.raw_customers
), 
sorted_data as (
	select distinct original_data.id, original_data.first_name, original_data.last_name, aggregated.aggr_marker, original_data.null_columns from
	aggregated inner join original_data
	on aggregated.first_name = original_data.first_name and aggregated.last_name = original_data.last_name
)
select * from sorted_data

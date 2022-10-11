/* Bigquery won't let us `where` without `from` so we use this workaround */
with dummy_cte as (
    select 1 as foo
)

select
    cast(null as {{ type_string() }}) as command_invocation_id,
    cast(null as {{ type_string() }}) as node_id,
    cast(null as {{ type_string() }}) as column_name,
    cast(null as {{ type_string() }}) as data_type,
    cast(null as {{ type_array() }}) as tags,
    cast(null as {{ type_json() }}) as meta
from dummy_cte
where 1 = 0

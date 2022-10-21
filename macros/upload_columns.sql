{% macro upload_columns(graph) -%}
    {% set models = [] %}
    {% for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
        {% do models.append(node) %}
    {% endfor %}
    {{ return(adapter.dispatch('get_columns_dml_sql', 'dbt_artifacts')(models)) }}
{%- endmacro %}

{% macro default__get_columns_dml_sql(models) -%}

    {% if models != [] %}
        {% set model_values %}

        {% set adapterArr = ['databricks','spark','snowflake'] %}
        {% if target.type in adapterArr %}

        select
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(1) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(2) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(3) }},
            {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(4) }},
            {{ adapter.dispatch('parse_json', 'dbt_artifacts')(adapter.dispatch('column_identifier', 'dbt_artifacts')(5)) }},
            {{ adapter.dispatch('parse_json', 'dbt_artifacts')(adapter.dispatch('column_identifier', 'dbt_artifacts')(6)) }}
        from values

        {% endif %}

        {% set new_list = [] %}
        {% for model in models  %}
            {% if model.columns %}
            {% do new_list.append( model ) %}
            {% endif %}
        {% endfor %}

        {% for model in new_list -%}
            {% set relation = dbt_artifacts.get_relation( model.name ) %}
            {%- set columns = adapter.get_columns_in_relation(relation) -%}

            {% if target.schema == model.schema %}

            {%- set col_query -%}
            select
                count(1) as row_count
                {% for column in columns %}
                {% set col = model.columns.get(column.name) %}
                , count(distinct {{ column.name }}) as {{ column.name }}_distinct
                , count(1) - count({{ column.name }}) as {{ column.name }}_null
                {% if column.is_number() %}
                , min({{ column.name }}) as {{ column.name }}_min
                , max({{ column.name }}) as {{ column.name }}_max
                , avg({{ column.name }}) as {{ column.name }}_avg
                , sum({{ column.name }}) as {{ column.name }}_sum
                , stddev({{ column.name }}) as {{ column.name }}_stdev
                {% else %}
                , null as {{ column.name }}_min
                , null as {{ column.name }}_max
                , null as {{ column.name }}_avg
                , null as {{ column.name }}_sum
                , null as {{ column.name }}_stdev
                {% endif %}
                {% endfor %}
            from {{relation}}
            {%- endset -%}
            {%- set results = run_query(col_query) -%}

            {% else %}
            {%- set results = none -%}
            {% endif %}

            {% for column in columns %}
                {% set col = model.columns.get(column.name) %}
                (
                    '{{ invocation_id }}' {# command_invocation_id #}
                    , '{{ model.unique_id }}' {# node_id #}
                    , '{{ column.name }}' {# column_name #}
                    , '{{ column.data_type }}' {# data_type #}
                    , '{{ null if col.tags is not defined else tojson(col.tags) }}' {# tags #}
                    , '{{ null if col.meta is not defined else tojson(col.meta) }}' {# meta #}
                    , '{{ null if col.description is not defined else col.description | replace("'","\\'") }}' {# description #}
                    , '{{ "N" if col.name is not defined else "Y" }}' {# is_documented #}
                    {% if results is none %}
                    , null
                    , null
                    , null
                    , null
                    , null
                    , null
                    , null
                    , null
                    {% else %}
                    , {{ results.columns['row_count'].values()[0] or 0 }} {# row_count #}
                    , {{ results.columns[column.name ~ '_distinct'].values()[0] or 0 }} {# row_distinct #}
                    , {{ results.columns[column.name ~ '_null'].values()[0] or 0 }} {# row_null #}
                    , {{ results.columns[column.name ~ '_min'].values()[0] or 0 }} {# row_min #}
                    , {{ results.columns[column.name ~ '_max'].values()[0] or 0 }} {# row_max #}
                    , {{ results.columns[column.name ~ '_avg'].values()[0] or 0 }} {# row_avg #}
                    , {{ results.columns[column.name ~ '_sum'].values()[0] or 0 }} {# row_sum #}
                    , {{ results.columns[column.name ~ '_stdev'].values()[0] or 0 }} {# row_stdev #}
                    {% endif %}
                )
                {%- if not loop.last %},{%- endif %}
            {% endfor %}
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ model_values }}
    {% else %}
        {{ return("") }}
    {% endif %}
{% endmacro -%}

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
            {% for key, value in model.columns.items() %}
                (
                    '{{ invocation_id }}', {# command_invocation_id #}
                    '{{ model.unique_id }}', {# node_id #}
                    '{{ key }}', {# column_name #}
                    '{{ value.data_type }}', {# data_type #}
                    '{{ tojson(value.tags) }}', {# tags #}
                    '{{ tojson(value.meta) }}' {# meta #}
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

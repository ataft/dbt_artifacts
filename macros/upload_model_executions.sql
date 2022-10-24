{% macro upload_model_executions(results) -%}
    {% set models = [] %}
    {% for result in results  %}
        {% if result.node.resource_type == "model" %}
            {% do models.append(result) %}
        {% endif %}
    {% endfor %}
    {{ return(adapter.dispatch('get_model_executions_dml_sql', 'dbt_artifacts')(models)) }}
{%- endmacro %}

{% macro default__get_model_executions_dml_sql(models) -%}
    {% if models != [] %}

        {% set adapterArr = ['databricks','spark','snowflake'] %}
        {% if target.type in adapterArr %}

            {% set model_execution_values %}
            select
                {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(1) }},
                {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(2) }},
                {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(3) }},
                {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(4) }},
                {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(5) }},
                {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(6) }},
                {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(7) }},
                {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(8) }},
                {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(9) }},
                {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(10) }},
                {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(11) }},
                {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(12) }},
                {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(13) }},
                {{ adapter.dispatch('column_identifier', 'dbt_artifacts')(14) }}
            from values
            {% endset %}
            {{ model_execution_values }}

        {% endif %}

        {% set model_execution_items %}
        {% for model in models -%}

            {%- set rowcount_query %}
            select count(*) as "model_rowcount" from {{ model.node.schema }}.{{ model.node.name }}
            {%- endset -%}
            {%- set results = run_query(rowcount_query) -%}
            {%- set model_rowcount = results.columns[0].values()[0] -%}

            (
            '{{ invocation_id }}', {# command_invocation_id #}
            '{{ model.node.unique_id }}', {# node_id #}
            '{{ run_started_at }}', {# run_started_at #}

            {% set config_full_refresh = model.node.config.full_refresh %}
            {% if config_full_refresh is none %}
                {% set config_full_refresh = flags.FULL_REFRESH %}
            {% endif %}
            '{{ config_full_refresh }}', {# was_full_refresh #}

            '{{ model.thread_id }}', {# thread_id #}
            '{{ model.status }}', {# status #}

            {% if model.timing != [] %}
                {% for stage in model.timing if stage.name == "compile" %}
                    {% if loop.length == 0 %}
                        null, {# compile_started_at #}
                    {% else %}
                        '{{ stage.started_at }}', {# compile_started_at #}
                    {% endif %}
                {% endfor %}

                {% for stage in model.timing if stage.name == "execute" %}
                    {% if loop.length == 0 %}
                        null, {# query_completed_at #}
                    {% else %}
                        '{{ stage.completed_at }}', {# query_completed_at #}
                    {% endif %}
                {% endfor %}
            {% else %}
                null, {# compile_started_at #}
                null, {# query_completed_at #}
            {% endif %}

            {{ model.execution_time }}, {# total_node_runtime #}
            cast(nullif('{{ model.adapter_response.rows_affected }}', '') as {{ type_int() }}),
            cast(nullif('{{ model.adapter_response.bytes_processed }}', '') as {{ type_int() }}),
            '{{ model.node.config.materialized }}', {# materialization #}
            '{{ model.node.schema }}', {# schema #}
            '{{ model.node.name }}', {# name #}
            {{ model_rowcount }} {# total rowcount #}
            )
            {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        {% endset %}
        {{ model_execution_items }}

    {% else %}
        {{ return("") }}
    {% endif %}
{% endmacro -%}

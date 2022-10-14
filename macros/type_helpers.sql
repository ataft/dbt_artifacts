{#- BOOLEAN -#}

{% macro type_boolean() %}
    {{ return(adapter.dispatch('type_boolean', 'dbt_artifacts')()) }}
{% endmacro %}

{% macro default__type_boolean() %}
   {{ return(api.Column.translate_type("boolean")) }}
{% endmacro %}

{#- JSON -#}

{% macro type_json() %}
    {{ return(adapter.dispatch('type_json', 'dbt_artifacts')()) }}
{% endmacro %}

{% macro default__type_json() %}
   {{ return(api.Column.translate_type("string")) }}
{% endmacro %}

{% macro snowflake__type_json() %}
   OBJECT
{% endmacro %}

{% macro bigquery__type_json() %}
   JSON
{% endmacro %}

{#- ARRAY -#}

{% macro type_array() %}
    {{ return(adapter.dispatch('type_array', 'dbt_artifacts')()) }}
{% endmacro %}

{% macro default__type_array() %}
   {{ return(api.Column.translate_type("string")) }}
{% endmacro %}

{% macro snowflake__type_array() %}
   ARRAY
{% endmacro %}

{% macro bigquery__type_array() %}
   ARRAY<string>
{% endmacro %}

{#- Redshift -#}

{% macro redshift__type_string() %}
   varchar(256)
{% endmacro %}

{% macro redshift__type_json() %}
   varchar(max)
{% endmacro %}

{% macro redshift__type_array() %}
   varchar(max)
{% endmacro %}

{#- Long String -#}

{%- macro type_longstring() -%}
  {{ return(adapter.dispatch('type_longstring', 'dbt_artifacts')()) }}
{%- endmacro -%}

{% macro default__type_longstring() %}
    string
{% endmacro %}

{%- macro redshift__type_longstring() -%}
    varchar(max)
{%- endmacro -%}

{% macro postgres__type_longstring() %}
    varchar(max)
{% endmacro %}

{% macro snowflake__type_longstring() %}
    varchar
{% endmacro %}

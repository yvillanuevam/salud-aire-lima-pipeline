{#
  Por defecto dbt concatena "<schema del target>_<schema custom>" (STAGING_MARTS).
  Este override usa el schema custom tal cual (MARTS), para que coincida con
  los esquemas y permisos creados en infra/snowflake/01_setup_snowflake.sql.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}

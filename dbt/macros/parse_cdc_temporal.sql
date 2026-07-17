{% macro cdc_timestamp(column) -%}
from_iso8601_timestamp(nullif(trim({{ column }}), ''))
{%- endmacro %}

{% macro cdc_date(column) -%}
coalesce(
    try_cast(regexp_replace(nullif(trim({{ column }}), ''), 'Z$', '') as date),
    try_cast(nullif(trim({{ column }}), '') as date)
)
{%- endmacro %}

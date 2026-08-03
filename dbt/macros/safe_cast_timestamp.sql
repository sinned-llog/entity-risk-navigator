{% macro safe_cast_timestamp(column_name, target_alias=none) %}
    {#
        Safely converts raw timestamp/date text fields into TIMESTAMPTZ.
        Validates the string against ISO pattern (YYYY-MM-DD...) before casting 
        to prevent SQL runtime exceptions on corrupt values (e.g., "unknown", "").

        Params:
        - column_name: The raw source column name.
        - target_alias (optional): Alias for the output column. Defaults to <column_name>_at.
    #}
    {% set alias = target_alias if target_alias else column_name ~ '_at' %}

    case 
        -- Ensures string is present and begins with a standard ISO date sequence (YYYY-MM-DD)
        when {{ column_name }} is not null 
         and {{ column_name }} ~ '^\d{4}-\d{2}-\d{2}' 
            then {{ column_name }}::timestamptz 
        else null 
    end as {{ alias }}
{% endmacro %}
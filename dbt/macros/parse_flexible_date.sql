{% macro parse_flexible_date(column_name) %}
    {#
        Parses incomplete or raw date strings (e.g. "1980", "1980-05", "1980-05-12") 
        without throwing SQL casting exceptions.

        Returns two columns:
        1. <column_name>_year (INTEGER): Extracted year for broad fuzzy matching.
        2. <column_name>_parsed (DATE): Best-effort DATE object (falls back to day/month 01 for partial dates).
    #}

    -- 1. Extracted Year as INTEGER (useful for year-only compliance matching)
    case 
        when {{ column_name }} ~ '^\d{4}' 
            then substring({{ column_name }} from 1 for 4)::integer 
        else null 
    end as {{ column_name }}_year,

    -- 2. Best-effort DATE normalization (prevents pipeline crashes on partial ISO strings)
    case
        -- Full ISO Date: YYYY-MM-DD (e.g., "1980-05-12")
        when {{ column_name }} ~ '^\d{4}-\d{2}-\d{2}' 
            then (substring({{ column_name }} from 1 for 10))::date
            
        -- Year and Month only: YYYY-MM (e.g., "1980-05" -> "1980-05-01")
        when {{ column_name }} ~ '^\d{4}-\d{2}$' 
            then ({{ column_name }} || '-01')::date
            
        -- Year only: YYYY (e.g., "1980" -> "1980-01-01")
        when {{ column_name }} ~ '^\d{4}$' 
            then ({{ column_name }} || '-01-01')::date
            
        else null
    end as {{ column_name }}_parsed
{% endmacro %}
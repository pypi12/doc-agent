{% macro analyze_model(model_name) %}

{# Get model SQL #}
{% set model_sql %}
    select * from {{ ref(model_name) }} limit 1
{% endset %}

{# Execute query to get column names and types #}
{% set results = run_query(model_sql) %}
{% set columns = results.columns %}

{# Log model info #}
{% do log('=== Model Analysis: ' ~ model_name ~ ' ===', info=True) %}

{# Get row count #}
{% set row_count_query %}
    select count(*) as row_count from {{ ref(model_name) }}
{% endset %}
{% set row_count = run_query(row_count_query) %}

{% do log('Row Count: ' ~ row_count[0][0], info=True) %}
{% do log('Column Count: ' ~ columns | length, info=True) %}

{# Analyze model type #}
{% set model_type = '' %}
{% if model_name.startswith('dim_') %}
    {% set model_type = 'Dimension' %}
{% elif model_name.startswith('fct_') %}
    {% set model_type = 'Fact' %}
{% elif model_name.startswith('stg_') %}
    {% set model_type = 'Staging' %}
{% endif %}

{% do log('Model Type: ' ~ model_type, info=True) %}

{# Analyze columns #}
{% do log('', info=True) %}
{% do log('Column Analysis:', info=True) %}

{% set key_columns = [] %}
{% set measure_columns = [] %}
{% set attribute_columns = [] %}
{% set timestamp_columns = [] %}

{% for col in columns %}
    {# Categorize column #}
    {% if col.endswith('_key') or col.endswith('_id') %}
        {% do key_columns.append(col) %}
    {% elif col in ['amount', 'quantity', 'weight', 'price', 'cost'] %}
        {% do measure_columns.append(col) %}
    {% elif col in ['created_at', 'updated_at', 'order_date'] %}
        {% do timestamp_columns.append(col) %}
    {% else %}
        {% do attribute_columns.append(col) %}
    {% endif %}

    {# Get column stats #}
    {% set stats_query %}
        select 
            count(*) as total_count,
            count(distinct {{ col }}) as distinct_count,
            count(case when {{ col }} is null then 1 end) as null_count
        from {{ ref(model_name) }}
    {% endset %}
    {% set stats = run_query(stats_query) %}
    
    {% do log('  ' ~ col ~ ':', info=True) %}
    {% do log('    Distinct Values: ' ~ stats[0][1], info=True) %}
    {% do log('    Null Count: ' ~ stats[0][2], info=True) %}
    {% do log('    Null %: ' ~ ((stats[0][2] / stats[0][0]) * 100) | round(2), info=True) %}
{% endfor %}

{# Model-specific analysis #}
{% do log('', info=True) %}
{% do log('Model Structure:', info=True) %}
{% do log('  Key Columns: ' ~ key_columns | join(', '), info=True) %}
{% do log('  Measure Columns: ' ~ measure_columns | join(', '), info=True) %}
{% do log('  Attribute Columns: ' ~ attribute_columns | join(', '), info=True) %}
{% do log('  Timestamp Columns: ' ~ timestamp_columns | join(', '), info=True) %}

{# Kimball compliance checks #}
{% do log('', info=True) %}
{% do log('Kimball Compliance:', info=True) %}

{% if model_type == 'Dimension' %}
    {% set has_surrogate_key = false %}
    {% set has_natural_key = false %}
    {% for col in key_columns %}
        {% if col.endswith('_key') %}
            {% set has_surrogate_key = true %}
        {% elif col.endswith('_id') %}
            {% set has_natural_key = true %}
        {% endif %}
    {% endfor %}
    
    {% do log('  Has Surrogate Key: ' ~ has_surrogate_key, info=True) %}
    {% do log('  Has Natural Key: ' ~ has_natural_key, info=True) %}
    {% do log('  Has Timestamps: ' ~ (timestamp_columns | length > 0), info=True) %}
    
    {% if not has_surrogate_key %}
        {% do log('  WARNING: Missing surrogate key', info=True) %}
    {% endif %}
    {% if not has_natural_key %}
        {% do log('  WARNING: Consider adding natural key', info=True) %}
    {% endif %}

{% elif model_type == 'Fact' %}
    {% set has_measures = measure_columns | length > 0 %}
    {% set has_foreign_keys = false %}
    {% for col in key_columns %}
        {% if col != 'sales_key' and col.endswith('_key') %}
            {% set has_foreign_keys = true %}
        {% endif %}
    {% endfor %}
    
    {% do log('  Has Measures: ' ~ has_measures, info=True) %}
    {% do log('  Has Foreign Keys: ' ~ has_foreign_keys, info=True) %}
    {% do log('  Has Timestamps: ' ~ (timestamp_columns | length > 0), info=True) %}
    
    {% if not has_measures %}
        {% do log('  WARNING: No measures found in fact table', info=True) %}
    {% endif %}
    {% if not has_foreign_keys %}
        {% do log('  WARNING: No foreign keys to dimensions', info=True) %}
    {% endif %}
{% endif %}

{% endmacro %}

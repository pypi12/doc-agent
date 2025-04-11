{% macro get_column_values(model_name, column_name) %}

    {% set query %}
    SELECT DISTINCT {{ column_name }}
    FROM {{ ref(model_name) }}
    ORDER BY 1
    {% endset %}

    {% set results = run_query(query) %}
    
    {% if execute %}
        {% for row in results %}
            {{ log(row[0], info=True) }}
        {% endfor %}
    {% endif %}

{% endmacro %}

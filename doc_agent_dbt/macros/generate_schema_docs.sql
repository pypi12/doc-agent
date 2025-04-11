{% macro generate_schema_docs(model_name) %}

    {% set relation = ref(model_name) %}

    {% set get_columns_query %}
    SELECT column_name, data_type
    FROM {{ relation.database }}.{{ relation.schema }}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{{ relation.identifier }}'
    {% endset %}

    {% set columns = run_query(get_columns_query) %}
    
    {% if execute %}
        {{ log("Analyzing table: " ~ relation, info=True) }}
        {% for col in columns.rows %}
            {% set col_query %}
            SELECT 
                '{{ col[0] }}' as column_name,
                '{{ col[1] }}' as data_type,
                COUNT(*) as row_count,
                COUNT(DISTINCT `{{ col[0] }}`) as distinct_count,
                COUNTIF(`{{ col[0] }}` IS NULL) as null_count
            FROM {{ relation }}
            {% endset %}

            {% set results = run_query(col_query) %}
            {% set row = results.rows[0] %}
            {{ log(
                "Column: " ~ row[0] ~ 
                ", Type: " ~ row[1] ~ 
                ", Total Rows: " ~ row[2] ~ 
                ", Distinct Values: " ~ row[3] ~ 
                ", Null Count: " ~ row[4], 
                info=True
            ) }}
        {% endfor %}
    {% endif %}

{% endmacro %}

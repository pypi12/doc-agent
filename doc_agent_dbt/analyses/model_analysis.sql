-- Analyze dimension tables
{{ analyze_model('dim_products') }}
{{ analyze_model('dim_dates') }}
{{ analyze_model('dim_geography') }}

-- Analyze fact table
{{ analyze_model('fct_sales') }}

-- Analyze staging model
{{ analyze_model('stg_sales') }}

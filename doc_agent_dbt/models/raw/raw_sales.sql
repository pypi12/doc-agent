{{
    config(
        materialized='view'
    )
}}

SELECT *
FROM `genai-projects-455516.amazon_sales.sales_raw_data`

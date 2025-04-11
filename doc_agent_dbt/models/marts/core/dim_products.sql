with stg_sales as (
    select * from {{ ref('stg_sales') }}
),

products as (
    select distinct
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['product_asin', 'product_sku']) }} as product_key,
        
        -- Natural keys
        product_asin,
        product_sku,
        
        -- Product attributes
        product_style,
        product_category,
        product_size,
        
        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at
        
    from stg_sales
)

select * from products

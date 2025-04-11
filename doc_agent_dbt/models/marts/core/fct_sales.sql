with stg_sales as (
    select * from {{ ref('stg_sales') }}
),

deduped_sales as (
    select *,
        -- Add a counter for each combination of order and product
        row_number() over (
            partition by order_id, product_asin, product_sku
            order by order_date
        ) as order_line_number
    from stg_sales
),

final as (
    select
        -- Surrogate key (transaction grain)
        {{ dbt_utils.generate_surrogate_key([
            'deduped_sales.order_id',
            'deduped_sales.product_asin',
            'deduped_sales.product_sku',
            'deduped_sales.order_line_number'
        ]) }} as sales_key,

        -- Foreign keys to dimensions
        {{ dbt_utils.generate_surrogate_key([
            'deduped_sales.product_asin',
            'deduped_sales.product_sku'
        ]) }} as product_key,
        
        format_date('%Y%m%d', cast(deduped_sales.order_date as date)) as date_key,
        
        {{ dbt_utils.generate_surrogate_key([
            'deduped_sales.ship_city',
            'deduped_sales.ship_state',
            'deduped_sales.ship_postal_code',
            'deduped_sales.ship_country'
        ]) }} as geography_key,
        
        -- Degenerate dimensions (transaction attributes)
        deduped_sales.order_id,
        deduped_sales.order_status,
        deduped_sales.fulfillment_type,
        deduped_sales.sales_channel,
        deduped_sales.shipping_service_level,
        deduped_sales.courier_status,
        deduped_sales.promotion_ids,
        deduped_sales.is_b2b,
        deduped_sales.fulfilled_by,
        
        -- Measures (facts)
        deduped_sales.quantity as order_quantity,
        deduped_sales.amount as order_amount,
        deduped_sales.currency as order_currency,
        
        -- Timestamps
        deduped_sales.order_date,
        
        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at

    from deduped_sales
)

select * from final

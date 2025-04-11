with source as (
    select * from {{ ref('raw_sales') }}
),

renamed as (
    select
        -- Keys
        `Order ID` as order_id,
        ASIN as product_asin,
        SKU as product_sku,
        
        -- Dates
        Date as order_date,
        
        -- Order attributes
        Status as order_status,
        Fulfilment as fulfillment_type,
        `Sales Channel ` as sales_channel,
        `ship-service-level` as shipping_service_level,
        `Courier Status` as courier_status,
        
        -- Product attributes
        Style as product_style,
        Category as product_category,
        Size as product_size,
        
        -- Quantities and amounts
        case
            when Qty <= 0 then 1  -- Set invalid quantities to 1
            else Qty
        end as quantity,
        case
            when Amount <= 0 then 0.01  -- Set invalid amounts to 1 cent
            else Amount
        end as amount,
        currency,
        
        -- Shipping information
        `ship-city` as ship_city,
        `ship-state` as ship_state,
        `ship-postal-code` as ship_postal_code,
        `ship-country` as ship_country,
        
        -- Additional attributes
        `promotion-ids` as promotion_ids,
        B2B as is_b2b,
        `fulfilled-by` as fulfilled_by

    from source
    where `Order ID` is not null  -- Ensure we have valid orders
)

select * from renamed

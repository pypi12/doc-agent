with stg_sales as (
    select * from {{ ref('stg_sales') }}
),

deduplicated as (
    select distinct
        ship_city,
        ship_state,
        ship_postal_code,
        ship_country
    from stg_sales
    where ship_city is not null
        and ship_state is not null
        and ship_postal_code is not null
        and ship_country is not null
),

geography as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'ship_city',
            'ship_state',
            'ship_postal_code',
            'ship_country'
        ]) }} as geography_key,
        
        -- Natural keys and attributes
        ship_city as city,
        ship_state as state,
        ship_postal_code as postal_code,
        ship_country as country,
        
        -- Derived attributes
        case
            when ship_state in ('CA', 'OR', 'WA') then 'West'
            when ship_state in ('NY', 'NJ', 'CT', 'MA', 'RI', 'VT', 'NH', 'ME') then 'Northeast'
            when ship_state in ('FL', 'GA', 'SC', 'NC', 'VA', 'WV', 'KY', 'TN', 'MS', 'AL', 'LA', 'AR') then 'Southeast'
            when ship_state in ('MO', 'IL', 'IN', 'OH', 'MI', 'WI', 'MN', 'IA', 'KS', 'NE', 'SD', 'ND') then 'Midwest'
            else 'Other'
        end as region,
        
        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at
        
    from deduplicated
)

select * from geography

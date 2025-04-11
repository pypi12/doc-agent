with date_spine as (
    -- Generate a date series from min to max date in our data
    select date from (
        select
            date_sub(min_date, interval rn day) as date
        from (
            select min(order_date) as min_date, max(order_date) as max_date
            from {{ ref('stg_sales') }}
        ) as dates
        cross join (
            select row_number() over() - 1 as rn
            from unnest(generate_array(0, 365)) as numbers
        ) as numbers
        where date_sub(min_date, interval rn day) <= max_date
    )
),

enriched_dates as (
    select
        -- Date key (YYYYMMDD format)
        format_date('%Y%m%d', date) as date_key,
        
        -- Date fields
        date as full_date,
        extract(year from date) as year,
        cast(extract(month from date) as string) as month_number,
        format_date('%B', date) as month_name,
        format_date('%Y-%m', date) as year_month,
        extract(day from date) as day_of_month,
        cast(extract(dayofweek from date) as string) as day_of_week_number,
        format_date('%A', date) as day_of_week_name,
        
        -- Fiscal periods (assuming fiscal year starts April 1)
        case 
            when extract(month from date) >= 4 
            then extract(year from date)
            else extract(year from date) - 1
        end as fiscal_year,
        
        -- Is this a weekend?
        case 
            when extract(dayofweek from date) in (1, 7) then true 
            else false 
        end as is_weekend,
        
        -- Is this a holiday? (simplified example)
        case 
            when format_date('%m-%d', date) in ('12-25', '01-01') then true 
            else false 
        end as is_holiday,
        
        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at

    from date_spine
)

select * from enriched_dates

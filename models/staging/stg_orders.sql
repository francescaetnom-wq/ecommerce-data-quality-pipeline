with cleaned as (

    select
        order_id,
        customer_id,
        cast(order_date as date) as order_date,
        nullif(lower(trim(status)), '') as status
    from {{ ref('raw_orders') }}

)

select * from cleaned
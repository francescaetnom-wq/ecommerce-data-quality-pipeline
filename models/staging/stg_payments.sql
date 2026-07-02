with cleaned as (

    select
        payment_id,
        order_id,
        trim(lower(replace(payment_method, '_', ' '))) as payment_method,
        amount_eur,
        cast(payment_date as date) as payment_date
    from {{ ref('raw_payments') }}

)

select * from cleaned
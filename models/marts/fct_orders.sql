{{ config(materialized='table') }}

with dedup_orders as (
    select *
    from {{ ref('stg_orders') }}
    qualify row_number() over (partition by order_id order by order_date) = 1
),
dedup_customers as (
    select *
    from {{ ref('stg_customers') }}
    qualify row_number() over (partition by customer_id order by customer_id) = 1
),
payments_per_order as (
    select
        order_id,
        sum(amount_eur) as total_amount
    from {{ ref('stg_payments') }}
    group by order_id
),

orders_enriched as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        concat(c.first_name, ' ', c.last_name) as customer_name,
        c.email as customer_email,
        c.customer_id as matched_customer_id,
        p.total_amount
from dedup_orders o
left join dedup_customers c on o.customer_id = c.customer_id
left join payments_per_order p on o.order_id = p.order_id
)

select
    order_id,
    customer_id,
    order_date,
    status,
    customer_name,
    customer_email,
    total_amount,
    case when matched_customer_id is null then 'unknown' else 'known' end as customer_status
from orders_enriched
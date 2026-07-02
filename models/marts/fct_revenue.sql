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

valid_payments as (
    select
        order_id,
        sum(amount_eur) as total_amount,
        max(case when amount_eur < 0 then true else false end) as has_negative_amount
    from {{ ref('stg_payments') }}
    where order_id in (select order_id from dedup_orders)
    group by order_id
),

global_median as (
    select percentile_cont(amount_eur, 0.5) over() as median
    from {{ ref('stg_payments') }}
    where amount_eur is not null
    limit 1
)

select
    o.order_id,
    o.customer_id,
    case when c.customer_id is null then 'unknown' else 'known' end as customer_status,
    p.total_amount,
    coalesce(p.total_amount, gm.median) as total_amount_imputed,
    coalesce(p.has_negative_amount, false) as has_negative_amount
from dedup_orders o
left join dedup_customers c on o.customer_id = c.customer_id
left join valid_payments p on o.order_id = p.order_id
cross join global_median gm
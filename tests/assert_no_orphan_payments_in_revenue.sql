-- deve restituire zero righe: se ne trova, un ordine di fct_revenue
-- sta sommando anche un pagamento che avrebbe dovuto essere escluso
select r.order_id
from {{ ref('fct_revenue') }} r
where r.order_id in (
    select p.order_id
    from {{ ref('stg_payments') }} p
    where p.order_id not in (select order_id from {{ ref('stg_orders') }})
)
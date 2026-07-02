-- deve restituire zero righe: ogni importo imputato negativo
-- deve avere has_negative_amount = true, altrimenti il flag mente
select order_id, total_amount_imputed, has_negative_amount
from {{ ref('fct_revenue') }}
where total_amount_imputed < 0
  and has_negative_amount = false
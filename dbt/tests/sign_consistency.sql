select * from {{ ref('fct_invoice_line') }}
where (is_cancellation and quantity >= 0) or unit_price < 0
   or (quantity < 0 and net_amount > 0) or (quantity > 0 and net_amount < 0)

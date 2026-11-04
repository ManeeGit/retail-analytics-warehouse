-- Grain: one physical source row, not one invoice/product combination.
select s.source_row_id as invoice_line_key, s.source_sha256, s.source_row_number,
       s.invoice_no, s.invoice_at, cast(strftime(s.invoice_at, '%Y%m%d') as integer) as date_key,
       c.customer_key, p.product_key, s.customer_id, s.stock_code,
       s.country, s.signed_quantity as quantity, s.unit_price, s.net_amount,
       s.is_cancellation, s.signed_quantity < 0 as is_return, s.unit_price = 0 as is_free
from {{ ref('stg_lines') }} s
join {{ ref('dim_customer') }} c on s.customer_id = c.customer_id
  and s.invoice_at >= c.valid_from and s.invoice_at < c.valid_to
join {{ ref('dim_product') }} p on s.stock_code = p.stock_code

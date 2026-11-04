select customer_id, count(*) as line_count, min(invoice_at) as first_seen,
       max(invoice_at) as last_seen, sum(net_amount) as lifetime_net_sales,
       count(distinct invoice_no) filter (where not is_return) as sale_invoice_count
from {{ ref('fct_invoice_line') }} group by customer_id

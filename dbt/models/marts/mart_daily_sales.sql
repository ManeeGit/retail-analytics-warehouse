select date_key, country, count(*) as line_count,
       count(distinct invoice_no) filter (where not is_return) as sale_invoice_count,
       sum(case when not is_return then net_amount else 0 end) as gross_sales,
       sum(case when is_return then -net_amount else 0 end) as returns,
       sum(net_amount) as net_sales, sum(quantity) as net_units
from {{ ref('fct_invoice_line') }} group by date_key, country

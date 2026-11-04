-- Type 1: most recently observed description, with stable source ID tie-breaker.
select md5(stock_code) as product_key, stock_code, description
from {{ ref('stg_lines') }}
qualify row_number() over (partition by stock_code order by invoice_at desc, source_row_id desc) = 1

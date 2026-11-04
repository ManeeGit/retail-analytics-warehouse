-- Sparse date dimension: dates actually present in accepted invoice lines.
select distinct cast(strftime(invoice_at, '%Y%m%d') as integer) as date_key,
       cast(invoice_at as date) as calendar_date,
       year(invoice_at) as year, month(invoice_at) as month,
       day(invoice_at) as day, dayofweek(invoice_at) as day_of_week
from {{ ref('stg_lines') }}

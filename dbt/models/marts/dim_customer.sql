-- Synthetic, reproducible SCD2 example. UCI does NOT provide CRM history.
-- The synthetic scenario changes customer 101 at 2024-02-01.
-- All other customer records are static; country stays on the fact as observed.
with customers as (
  select distinct customer_id from {{ ref('stg_lines') }}
  union select 'UNKNOWN'
), versions as (
  select customer_id, 'standard' as segment,
         timestamp '1900-01-01' as valid_from,
         case when customer_id = '101' then timestamp '2024-02-01' else timestamp '9999-12-31' end as valid_to,
         customer_id <> '101' as is_current
  from customers
  union all
  select customer_id, 'loyal', timestamp '2024-02-01', timestamp '9999-12-31', true
  from customers where customer_id = '101'
)
select md5(customer_id || '|' || cast(valid_from as varchar)) as customer_key,
       *, 'synthetic_demo' as history_source
from versions

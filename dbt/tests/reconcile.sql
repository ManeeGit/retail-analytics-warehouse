with totals as (
 select (select count(*) from {{ ref('stg_lines') }}) as staged,
        (select count(*) from {{ ref('fct_invoice_line') }}) as facts,
        (select coalesce(sum(net_amount), 0) from {{ ref('stg_lines') }}) as staged_net,
        (select coalesce(sum(net_sales), 0) from {{ ref('mart_daily_sales') }}) as mart_net
)
select * from totals where staged <> facts or staged_net <> mart_net

-- Run individual SELECT statements with `retail query "..."`.
-- Resolve CURRENT.json once if using your own DuckDB client.

-- 1. Net revenue is gross sales minus returns, not all positive source quantities.
SELECT sum(gross_sales) AS gross_sales,
       sum(returns) AS returns,
       sum(net_sales) AS net_sales
FROM mart_daily_sales;

-- 2. Daily trend by the country observed on the invoice (not a customer domicile).
SELECT d.calendar_date, m.country, m.net_sales, m.net_units
FROM mart_daily_sales m JOIN dim_date d USING (date_key)
ORDER BY d.calendar_date, m.country;

-- 3. Synthetic segment at transaction time, showing the SCD2 join.
SELECT c.segment, count(*) AS lines, sum(f.net_amount) AS net_sales
FROM fct_invoice_line f JOIN dim_customer c USING (customer_key)
GROUP BY c.segment;

-- 4. Quarantine is part of the deliverable, never silently discarded.
SELECT rejection_reason, count(*) AS rejected_rows
FROM quarantine
GROUP BY rejection_reason;

-- 5. Trace a fact to its original input position and preserved raw values.
SELECT f.invoice_line_key, s.source_sha256, s.source_sheet,
       s.source_row_number, s.raw_json
FROM fct_invoice_line f
JOIN source_lines s ON f.invoice_line_key = s.source_row_id
WHERE f.invoice_no = '10009';

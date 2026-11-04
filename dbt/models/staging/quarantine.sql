select * from {{ source('bronze', 'source_lines') }}
where rejection_reason is not null

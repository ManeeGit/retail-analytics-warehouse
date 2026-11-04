-- Rejected records remain in bronze and quarantine for investigation.
select * exclude (rejection_reason, raw_json)
from {{ source('bronze', 'source_lines') }}
where rejection_reason is null

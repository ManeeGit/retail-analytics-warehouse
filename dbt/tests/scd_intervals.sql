with intervals as (
 select *, lag(valid_to) over (partition by customer_id order by valid_from) as prior_end
 from {{ ref('dim_customer') }}
)
select * from intervals where valid_from >= valid_to or prior_end > valid_from

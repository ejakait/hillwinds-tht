
with lagged_plan_dates as (
SELECT
	company_ein,
	plan_type,
	carrier_name,
	start_date,
	end_date,
	lag(end_date, 1, null) over(partition by company_ein, plan_type order by start_date) as previous_plan_end_date,
	lag(carrier_name, 1, null) over (partition by company_ein,
	plan_type
order by
	start_date asc) as previous_carrier
FROM
	plans
order by
	company_ein,
	plan_type,
	start_date asc)
            select
	company_ein,
	end_date as gap_start,
	start_date as gap_end,
	datediff('day', date(previous_plan_end_date), date(start_date)) as gap_length_days,
	carrier_name as next_carrier,
from
	lagged_plan_dates
where
	gap_length_days > 7

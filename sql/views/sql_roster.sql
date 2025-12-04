with employee_counts as (
select
	company_ein,
	count(distinct email) as observed
from
	employees
group by
	1)

select
	*,
	10 as expected,
	Round((observed-expected)/ expected * 100,2) as pct_difference,
	case
		when
pct_difference<20 then 'Low'
when pct_difference between 20 and 50 then 'Medium'
when pct_difference between 50 and 100 then 'High'
when pct_difference>100 then 'Critical'
	end as serverity
from
	employee_counts

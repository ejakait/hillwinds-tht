select
company_ein,
trim(lower(plan_type)) as plan_type,
carrier_name,
cast(start_date as date) as start_date,
cast(end_date as date) as end_date
from plans qualify  ROW_NUMBER() over(partition by company_ein, plan_type order by start_date)=1

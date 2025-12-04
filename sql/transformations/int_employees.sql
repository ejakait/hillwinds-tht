with _employees as (
    select
    person_id,
    full_name,
    trim(title) as title,
    trim(email) as email,
    company_ein,
    cast(start_date as date) as start_date,
    notes
    from employees
    where
	company_ein is not null
	and company_ein not like ''
    )

select
    person_id,
    full_name,
    title,
    email,
    split_part(email, '@', 2) as company_name,
    company_ein,
    start_date,
    notes
from _employees qualify  ROW_NUMBER() over(partition by email,company_ein order by start_date)=1;

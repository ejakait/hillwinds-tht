SELECT
claim_id,
company_ein,
cast(service_date as date) as service_date,
cast(amount as decimal(10,2)) as amount,
trim(lower(claim_type)) as claim_type
from claims qualify  ROW_NUMBER() over(partition by claim_id,company_ein order by service_date)=1;

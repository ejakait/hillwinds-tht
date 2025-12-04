WITH stitched_plans AS (
    SELECT
        p.company_ein,
        p.plan_type,
        MIN(p.start_date) AS start_date,
        MAX(p.end_date) AS end_date
    FROM
        plans p

    GROUP BY
        p.company_ein, p.plan_type
),

plan_gaps AS (
    SELECT
        s.company_ein,
        s.plan_type,
        s.start_date,
        s.end_date,
        LAG(s.end_date, 1) OVER (PARTITION BY s.company_ein, s.plan_type ORDER BY s.start_date) AS previous_plan_end_date,
        LAG(p.carrier_name, 1) OVER (PARTITION BY s.company_ein, s.plan_type ORDER BY s.start_date) AS previous_carrier,
        p.carrier_name AS next_carrier
    FROM
        stitched_plans s
    LEFT JOIN
        plans p ON s.company_ein = p.company_ein AND s.plan_type = p.plan_type AND s.start_date = p.start_date

)

SELECT
    company_ein,
    previous_plan_end_date AS gap_start,
    start_date AS gap_end,
    DATE_DIFF('day', previous_plan_end_date, start_date) AS gap_length_days,
    previous_carrier,
    next_carrier
FROM
    plan_gaps
WHERE
    DATE_DIFF('day', previous_plan_end_date, start_date) > 7
ORDER BY
    company_ein, plan_type, gap_start;


WITH daily_claims AS (
    SELECT
        company_ein,
        CAST(service_date AS DATE) AS service_date,
        SUM(amount) AS daily_amount_total
    FROM
        claims
    GROUP BY
        company_ein, CAST(service_date AS DATE)
),

claims_90_rolling AS (
    SELECT
        service_date,
        company_ein,
        daily_amount_total,
        (service_date - INTERVAL '89 days') AS window_start_date,
        service_date AS window_end_date,

        SUM(daily_amount_total) OVER (
            PARTITION BY company_ein
            ORDER BY service_date
            RANGE BETWEEN INTERVAL '89 days' PRECEDING AND CURRENT ROW
        ) AS current_90_day_sum,

        SUM(daily_amount_total) OVER (
            PARTITION BY company_ein
            ORDER BY service_date
            RANGE BETWEEN INTERVAL '179 days' PRECEDING AND INTERVAL '90 days' PRECEDING
        ) AS previous_90_day_sum
    FROM
        daily_claims
)

SELECT
    cr.company_ein,
    cr.window_start_date AS window_start,
    cr.window_end_date AS window_end,
    cr.previous_90_day_sum AS prev_90d_cost,
    cr.current_90_day_sum AS current_90d_cost,
    ROUND(((cr.current_90_day_sum - cr.previous_90_day_sum) * 100.0 / cr.previous_90_day_sum), 2) AS pct_change
FROM
    claims_90_rolling cr
WHERE
    cr.previous_90_day_sum IS NOT NULL
    AND ROUND(((cr.current_90_day_sum - cr.previous_90_day_sum) * 100.0 / cr.previous_90_day_sum), 2) > 200
ORDER BY
    company_ein, window_end;

WITH daily_claims AS (
    SELECT
        company_ein,
        CAST(service_date AS DATE) AS service_date,
        SUM(amount) AS daily_amount_total
    FROM
    claims
    GROUP BY
        company_ein,
        CAST(service_date AS DATE)
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
    *,
    ROUND(((current_90_day_sum - previous_90_day_sum) * 100.0 / previous_90_day_sum),2) AS percent_increase,
FROM
    claims_90_rolling where percent_increase >200
ORDER BY
    company_ein,
    service_date;

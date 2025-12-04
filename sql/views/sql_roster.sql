WITH expected_counts AS (
    SELECT '11-1111111' AS company_ein, 10 AS expected
    UNION ALL
    SELECT '22-2222222' AS company_ein, 20 AS expected
    UNION ALL
    SELECT '33-3333333' AS company_ein, 50 AS expected
),

employee_counts AS (
    SELECT
        company_ein,
        COUNT(DISTINCT email) AS observed
    FROM
        employees
    GROUP BY
        company_ein
)

SELECT
    e.company_ein as company_ein,
    ec.expected,
    COALESCE(e.observed, 0) AS observed,
    ROUND((COALESCE(e.observed, 0) - ec.expected) * 100.0 / ec.expected, 2) AS pct_diff,
    CASE
        WHEN ROUND((COALESCE(e.observed, 0) - ec.expected) * 100.0 / ec.expected, 2) < 20 THEN 'Low'
        WHEN ROUND((COALESCE(e.observed, 0) - ec.expected) * 100.0 / ec.expected, 2) BETWEEN 20 AND 50 THEN 'Medium'
        WHEN ROUND((COALESCE(e.observed, 0) - ec.expected) * 100.0 / ec.expected, 2) BETWEEN 50 AND 100 THEN 'High'
        ELSE 'Critical'
    END AS severity
FROM
    expected_counts ec
LEFT JOIN
    employee_counts e ON ec.company_ein = e.company_ein
ORDER BY
    company_ein;

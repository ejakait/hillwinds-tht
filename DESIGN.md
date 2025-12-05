# DESIGN.md

## 1) dbt System Design

### Model Layering Strategy: Staging → Intermediate → Fact

- **Staging Layer (`stg_` models):** Ingests raw data with minimal transformations (renaming, type casting, metadata).
- **Intermediate Layer (`int_` models):** Applies light business logic, joins staging models, cleans data, and creates reusable datasets.
- **Marts Layer (`fct_` and `dim_` models):** Builds business-ready fact and dimension tables, optimized for analytical querying.
- `stg_` models focus on preparing raw data from CSVs and JSON into a consistent format.
- `int_` models perform data cleaning, deduplication, normalization, and aggregation.
- `fct_` and `dim_` models deliver final business logic, identifying gaps, mismatches, spikes, and providing company dimensions for BI tools.

### Other dbt Design Considerations:

- **Incremental Merge Logic:**
  - For large, append-only fact tables or staging models, `incremental` materialization would be used (e.g., `stg_claims`, `stg_employees`).
  - Rows would be inserted if they are new (`last_updated > max_event_time_in_table`).
  - For updates, a `merge` strategy would be employed to insert new versions of updated records or overwrite existing ones basedon the `last_updated` column.
- **Schema/Column Tests:**
  - **`not_null` and `unique`:** Applied to primary keys (e.g., `employee_id`, `plan_id`, `claim_id`) and critical foreign keys.
  - **`accepted_values`:** For categorical columns (e.g., `plan_type`, `severity`).
  - **`relationships`:** To ensure referential integrity between fact and dimension tables.
- **Freshness / Anomaly Checks:**
  - **Freshness:** Configured `freshness` tests on source tables (`source.yml`) to ensure data is arriving within expected intervals (e.g., `warn_after: {count: 12, period: hour}`).
- **Metadata Columns:**
  - Each model would include metadata columns like `dbt_inserted_at` (timestamp of insertion), `dbt_updated_at` (timestamp of last update), and possibly `dbt_job_id` for traceability.
  - For Slowly Changing Dimensions (SCD Type 2), `dbt_valid_from` and `dbt_valid_to` would track effective date ranges for record versions.
- **Safe Model Deprecation Strategy:**
  1.  **Communicate:** Inform downstream users of the impending deprecation, providing a clear timeline and alternative models.
  2.  **Soft Deprecation in Code:** Mark the deprecated model with a `deprecated` tag in `schema.yml`, and update any direct `ref()` calls to point to the new model, leaving the old `ref()` for a transition period if possible.
  3.  **Monitor Usage:** Track queries against the deprecated model to ensure usage drops to zero.
  4.  **Hard Deprecation:** Once usage ceases, remove the model definition from the dbt project and drop the corresponding table(s) from the database.

## 2) Deep Debugging & Incident Response

**Scenario:** New feed adds 20M rows/day; costs triple; models slow 4m → 45m; outputs wrong despite passing dbt tests.

### Likely Causes

1.  **Partitioning/Clustering Issues:**
    - The new data volume might have overwhelmed existing partitioning/clustering strategies on target tables, leading to full table scans instead of partition pruning.
2.  **Key Explosion / Cartesian Joins:**
    - The new data introduced duplicates or unexpected joins conditions that result in a massive increase in rows after joins (e.g., `employees` joining with `claims` without proper keys).
3.  **Data Type Mismatches / Implicit Conversions:**
    - New data introduces unexpected data types that force expensive implicit conversions or lead to incorrect join clauses.
4.  **Logic Bug (Beyond dbt Tests):**
    - dbt tests are good for schema and fundamental data quality, but they may not catch complex business logic issues.

### How You'd Isolate the Regression

1.  **Performance Monitoring:** Review database/warehouse performance dashboards (query execution times, resource utilization, concurrency) for the period before and after the incident.
2.  **Query Plan Comparison:** Compare query plans of critical dbt models for runs before and after the slowdown.
3.  **Data Profiling:** Use `dbt-profiler` or custom SQL to quickly profile the incoming new data vs. old data. Look for:
    - Changes in row counts (obvious).
    - Changes in min/max `last_updated` or `service_date`.
    - Changes in cardinality of join keys.
    - Changes in distribution of critical categorical values.
    - Presence of `NULL`s in previously `NOT NULL` columns.
4.  **Incremental Model Inspection:** If incremental models are used, inspect the `dbt_audit` tables to see how many rows were inserted/updated incrementally. A suddenly huge incremental load could point to the problem.
5.  **Review Recent Code Changes:** Manually review recent dbt model changes for any new joins, complex `WHERE` clauses, or changes to window functions.

### Detect Logic Issues Beyond dbt Tests

1.  **Data Quality Checks:** Implement more sophisticated data quality checks not covered by standard dbt tests:
    - **Distribution Checks:** Ensure key metric distributions (e.g., `amount` in claims) haven't drastically shifted.
    - **Referential Integrity:** Verify relationships that go beyond simple foreign key checks (e.g., "for every claim, there must be an active plan for that employee").
    - **Business Rule Assertions:** Custom assertions based on domain knowledge (e.g., "total claims amount for a company should never decrease day-over-day by more than X%").

### Guardrails to Prevent Recurrence

1.  **CI/CD Pipeline Enhancement:**
    - **Performance Testing:** Integrate performance tests into CI/CD for critical models. Run against a representative, scaled dataset.
    - **Data Contracts:** Enforce data contracts with source systems, defining expected schemas, data types, and volume.
    - **Automated Data Profiling:** Run data profiling on incoming source data and alert on significant deviations.
2.  **Rollback Strategy:** Ensure a robust rollback mechanism for dbt deployments.
3.  **Observability:** Implement comprehensive logging and monitoring for all ETL jobs and dbt runs, including custom metrics.

### First 30-Minute Plan

1.  **Confirm Scope & Impact:**
    - Immediately verify that outputs are indeed wrong.
    - Check primary performance dashboards for recent spikes in costs/slowness.
    - Determine which specific dbt models are running slow/generating incorrect data.
2.  **Identify Recent Changes:**
    - Review recent git commits to the dbt project. Was there a recent deployment?
    - Check if any new source data feeds were integrated or changed.
3.  **Isolate & Reproduce:**
    - Attempt to reproduce the issue in a staging environment with the problematic data/code revision.
    - If using incremental models, try running a full refresh of the affected model to see if the issue persists (rules out incremental logic as the sole cause).

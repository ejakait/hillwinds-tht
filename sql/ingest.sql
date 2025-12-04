CREATE OR REPLACE TABLE claims AS SELECT * FROM read_csv('claims_raw.csv', auto_detect=true, header=true);
-- Example with high-water mark:
-- CREATE OR REPLACE TABLE claims AS SELECT * FROM read_csv('claims_raw.csv', auto_detect=true, header=true) WHERE last_updated > '{{ high_water_mark }}';

CREATE OR REPLACE TABLE employees AS SELECT * FROM read_csv('employees_raw.csv', auto_detect=true, header=true);

CREATE OR REPLACE TABLE plans AS SELECT * FROM read_csv('plans_raw.csv', auto_detect=true, header=true);

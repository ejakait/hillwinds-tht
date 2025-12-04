CREATE TABLE IF NOT EXISTS  claims as  SELECT * from read_csv ('claims_raw.csv');
CREATE TABLE IF NOT EXISTS employees as  SELECT * from read_csv ('employees_raw.csv');
CREATE TABLE IF NOT EXISTS plans as  SELECT * from read_csv ('plans_raw.csv');

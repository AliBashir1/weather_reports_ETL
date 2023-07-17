-- create dbs for airflow and applications
CREATE DATABASE weather_db;

-- how to hide this information from github
CREATE ROLE weather_db_user WITH PASSWORD 'weatheruser123!' LOGIN;

-- revoke connecting access from public roles
REVOKE CONNECT ON DATABASE weather_db FROM PUBLIC;
GRANT CONNECT ON DATABASE weather_db to weather_db_user;
-- grant all privileges to respective users
GRANT ALL PRIVILEGES ON DATABASE weather_db to weather_db_user;

-- connect to db
\c weather_db
CREATE TABLE IF NOT EXISTS zipcodes_tbl (
  id SERIAL PRIMARY KEY,
  zipcode INT NOT NULL UNIQUE,
  zipcode_type varchar(50) DEFAULT NULL,
  city varchar(50) DEFAULT NULL,
  county varchar(50) DEFAULT 'NA',
  state varchar(2) DEFAULT 'NA',
  country varchar(2) DEFAULT 'USA',
  population int DEFAULT 9999,
  area_codes varchar(50) DEFAULT 999,
  timezone varchar(50) DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS weather_reports_tbl (
  id SERIAL PRIMARY KEY,
  zipcode INT,
  local_time timestamp DEFAULT NULL,
  last_updated timestamp DEFAULT NULL,
  day_of_week varchar(20) DEFAULT NULL,
  temp_f decimal(3,1) DEFAULT NULL,
  feelslike_f decimal(3,1) DEFAULT NULL,
  condition varchar(20) DEFAULT NULL,
  wind_mph decimal(3,1) DEFAULT NULL,
  wind_degree int DEFAULT NULL,
  wind_dir varchar(5) DEFAULT NULL,
  pressure_mb decimal(5,1) DEFAULT NULL,
  precip_mm decimal(3,1) DEFAULT NULL,
  humidity int DEFAULT NULL,
  cloud int DEFAULT NULL,
  vis_miles decimal(3,1) DEFAULT NULL,
  uv decimal(2,1) DEFAULT NULL,
  gust_mph decimal(3,1) DEFAULT NULL,

  CONSTRAINT FK_zipcode_weather FOREIGN KEY(zipcode)
    REFERENCES zipcodes_tbl(zipcode)
);

-- create table for a csv file
CREATE TEMPORARY TABLE IF NOT EXISTS  zipcodes_csv_data (
zip int,
type varchar(100),
 decommissioned varchar(100),
  primary_city varchar(100),
  acceptable_cities text,
  unacceptable_cities text,
  state varchar(100),
  county varchar(100),
  timezone varchar(100),
  area_codes varchar(100),
  world_region varchar(100),
  country varchar(100),
  latitude varchar(100),
  longitude varchar(100),
  irs_estimated_population int
);

COPY zipcodes_csv_data FROM '/resources/zip_code_database.csv' DELIMITERS ',' CSV header;

-- move data from temp table to actual table
INSERT INTO zipcodes_tbl (zipcode, zipcode_type, city, county, state, country, population, area_codes, timezone)
SELECT zip, type, primary_city, trim(regexp_replace(county, 'County|Municipio|Municipality|Municipality of', '')) as county, state, country, irs_estimated_population, area_codes, timezone from
zipcodes_csv_data;


-- Give access on tables to weather_db_user
GRANT ALL ON TABLE zipcodes_tbl TO weather_db_user;
GRANT ALL ON TABLE weather_reports_tbl TO weather_db_user;

-- CREATE index ON zipcodes_info FOR better query performances
CREATE INDEX county_index ON zipcodes_tbl(county);
CREATE INDEX city_index ON zipcodes_tbl(city);
CREATE INDEX state_index ON zipcodes_tbl(state);


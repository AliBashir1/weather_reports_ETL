-- create dbs for airflow and applications
CREATE DATABASE weather_db;

-- how to hide this information from github
CREATE ROLE weather_db_user WITH PASSWORD 'weatheruser123!' LOGIN;

-- revoke connecting access from public roles
REVOKE CONNECT ON DATABASE weather_db FROM PUBLIC;
GRANT CONNECT ON DATABASE weather_db TO weather_db_user;
-- grant all privileges to respective users
GRANT ALL PRIVILEGES ON DATABASE weather_db TO weather_db_user;

-- connect to db
\c weather_db

CREATE TYPE status AS ENUM('Successful', 'Partial Successful', 'Failed');

CREATE TABLE IF NOT EXISTS zipcodes_tbl (
  id            SERIAL PRIMARY KEY,
  zipcode       INT NOT NULL UNIQUE,
  zipcode_type  VARCHAR(50) DEFAULT NULL,
  city          VARCHAR(50) DEFAULT NULL,
  county        VARCHAR(50) DEFAULT 'NA',
  state         VARCHAR(2) DEFAULT 'NA',
  country       VARCHAR(2) DEFAULT 'USA',
  population    INT DEFAULT 9999,
  area_codes    VARCHAR(50) DEFAULT 999,
  timezone      VARCHAR(50) DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS weather_reports_tbl (
  id            SERIAL PRIMARY KEY,
  zipcode       INT,
  local_time    TIMESTAMPTZ DEFAULT NULL,
  last_updated  TIMESTAMPTZ DEFAULT NULL,
  day_of_week   VARCHAR(50) DEFAULT NULL,
  temp_f        DECIMAL(5,1) DEFAULT NULL,
  feelslike_f   DECIMAL(5,1) DEFAULT NULL,
  condition     VARCHAR(50) DEFAULT NULL,
  wind_mph      DECIMAL(5,1) DEFAULT NULL,
  wind_degree   INT DEFAULT NULL,
  wind_dir      VARCHAR(5) DEFAULT NULL,
  pressure_mb   DECIMAL(5,1) DEFAULT NULL,
  precip_mm     DECIMAL(5,1) DEFAULT NULL,
  humidity      INT DEFAULT NULL,
  cloud         INT DEFAULT NULL,
  vis_miles     DECIMAL(5,1) DEFAULT NULL,
  uv            DECIMAL(5,1) DEFAULT NULL,
  gust_mph      DECIMAL(5,1) DEFAULT NULL,
  created_at    TIMESTAMPTZ default now(),

  CONSTRAINT FK_zipcode_weather FOREIGN KEY(zipcode)
    REFERENCES zipcodes_tbl(zipcode)

);



CREATE TABLE IF NOT EXISTS daily_jobs_info(
    id              SERIAL PRIMARY KEY,
    job_name        VARCHAR(100) NOT NULL,
    daily_load_id   VARCHAR(50) NOT NULL UNIQUE,
    job_run_id      TEXT UNIQUE, -- run_id from dag table
    records_processed   INT NOT NULL,
    file_processed  INT NOT NULL,
    start_date      TIMESTAMPTZ,
    end_date        TIMESTAMPTZ,
    job_status      STATUS,
    created_at      TIMESTAMPTZ default now(),
    job_status_description TEXT

);
CREATE TABLE IF NOT EXISTS hourly_jobs_info(
    id              SERIAL PRIMARY KEY,
    job_name        VARCHAR(100) NOT NULL,
    hourly_load_id  VARCHAR(50) NOT NULL UNIQUE ,
    job_run_id      TEXT UNIQUE , -- run_id from dag table
    records_processed   INT NOT NULL,
    invalid_records     INT NOT NULL,
    total_records       INT NOT NULL,
    start_date      TIMESTAMPTZ,
    end_date        TIMESTAMPTZ,
    job_status      STATUS,
    next_scheduled_job timestamptz,
    created_at      TIMESTAMPTZ default now(),
    daily_load_id   VARCHAR(50),

    CONSTRAINT FK_daily_jobs_info FOREIGN KEY(daily_load_id)
            REFERENCES daily_jobs_info(daily_load_id)

);

-- Staging tables
CREATE TABLE IF NOT EXISTS hourly_jobs_info_staged(
    job_name            VARCHAR(100) NOT NULL,
    hourly_load_id      VARCHAR(50) NOT NULL UNIQUE ,
    job_run_id          TEXT UNIQUE , -- run_id from dag table
    records_processed   INT NOT NULL,
    invalid_records     INT NOT NULL,
    total_records       INT NOT NULL,
    start_date      TIMESTAMPTZ,
    end_date        TIMESTAMPTZ,
    job_status      STATUS,
    next_scheduled_job timestamptz,
    created_at      TIMESTAMPTZ default now()

);

-- create table for a csv file
CREATE TEMPORARY TABLE IF NOT EXISTS  zipcodes_csv_data (
    zip             INT,
    type            VARCHAR(100),
    decommissioned  VARCHAR(100),
    primary_city    VARCHAR(100),
    acceptable_cities   TEXT,
    unacceptable_cities TEXT,
    state           VARCHAR(100),
    county          VARCHAR(100),
    timezone        VARCHAR(100),
    area_codes      VARCHAR(100),
    world_region    VARCHAR(100),
    country         VARCHAR(100),
    latitude        VARCHAR(100),
    longitude       VARCHAR(100),
    irs_estimated_population INT
);
-- copy source file
COPY zipcodes_csv_data FROM '/resources/zip_code_database.csv' DELIMITERS ','  CSV header;

-- move data from temp table to actual table
INSERT INTO zipcodes_tbl (
                    zipcode,
                    zipcode_type,
                    city,
                    county,
                    state,
                    country,
                    population,
                    area_codes,
                    timezone
                    )
            SELECT
                zip,
                type,
                primary_city,
                TRIM(regexp_replace(county, 'County|Municipio|Municipality|Municipality of', '')) AS county,
                state,
                country,
                irs_estimated_population,
                area_codes,
                timezone
            FROM
            zipcodes_csv_data;

-- Give access on tables to weather_db_user
GRANT ALL ON TABLE zipcodes_tbl TO weather_db_user;
GRANT ALL ON TABLE weather_reports_tbl TO weather_db_user;
ALTER TABLE zipcodes_tbl OWNER TO weather_db_user;
ALTER TABLE weather_reports_tbl OWNER TO weather_db_user;
-- CREATE index ON zipcodes_info FOR better query performances
CREATE INDEX IF NOT EXISTS county_index ON zipcodes_tbl(county);
CREATE INDEX IF NOT EXISTS city_index ON zipcodes_tbl(city);
CREATE INDEX IF NOT EXISTS state_index ON zipcodes_tbl(state);


-- PROCEDURES AND FUNCTIONS
-- Creates a load id for daily or hourly jobs
CREATE OR REPLACE PROCEDURE create_load_id(INOUT load_id VARCHAR(50), IN daily_job bool)
LANGUAGE plpgsql
AS $$
DECLARE
    today_date timestamptz = now() at time zone('America/New_York');
    hour_rightnow int = cast(to_char(today_date, 'HH24') as int);
    hour varchar(10);
BEGIN
    raise notice '% , % , %',today_date, hour_rightnow, hour;
    IF hour_rightnow >=0 and hour_rightnow < 6
        then hour = '12AM';
    elsif hour_rightnow >= 6 and  hour_rightnow < 12
        then hour = '6AM';
    elsif  hour_rightnow >= 12 and hour_rightnow < 18
        then hour = '12PM';
    elsif  hour_rightnow >= 18 and hour_rightnow < 24
        then hour = '6PM';
    END IF;
    -- if load is for daily job then it should be load_
    IF daily_job = True THEN
        load_id :=concat('load_',to_char( (today_date - Interval '1 day')::date, 'YYYYMMDD') );
    ELSE
        load_id :=concat('load_',to_char(today_date, 'YYYYMMDD'),'_', hour);
    END if;

END $$;


-- Trigger functions
-- This function moves hourly job info staging data to main table
CREATE OR REPLACE FUNCTION transfer_hourly_job_info_staged_data()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
    BEGIN
        INSERT INTO hourly_jobs_info(
            job_name,
            hourly_load_id,
            job_run_id,
            records_processed,
            invalid_records,
             total_records,
            start_date,
            end_date,
            job_status,
            next_scheduled_job,
            created_at,
            daily_load_id)
        SELECT
            job_name,
            hourly_load_id,
            job_run_id,
            records_processed,
            invalid_records,
            total_records,
            start_date,
            end_date,
            job_status,
            next_scheduled_job,
            created_at,
            new.daily_load_id

        FROM hourly_jobs_info_staged
        WHERE hourly_load_id like concat(new.daily_load_id,'%');
    RETURN NEW;

    END;
$$;
-- This function shall remove staging data from staging table
-- daily_load_id contains yesterday record info
CREATE OR REPLACE FUNCTION remove_hourly_job_staging_records()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    DECLARE
    BEGIN
      DELETE FROM  hourly_jobs_info_staged
             WHERE hourly_load_id like concat(new.daily_load_id, '%');
      RETURN NEW;
    END;
    $$;
-- Trigger
-- It shall trigger the function to move staging data
CREATE TRIGGER trig_remove_hourly_job_staging_records
    AFTER INSERT ON hourly_jobs_info
    FOR EACH ROW EXECUTE PROCEDURE remove_hourly_job_staging_records();
-- It shall trigger the function to remove staging data from staging table.
CREATE TRIGGER hourly_job_transfer_trigger
AFTER INSERT ON daily_jobs_info
FOR EACH ROW EXECUTE PROCEDURE transfer_hourly_job_info();


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
COPY zipcodes_csv_data FROM '/resources/zip_code_database.csv' DELIMITERS ',' CSV header;

-- move data from temp table to actual table
INSERT INTO zipcodes_tbl (zipcode,
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
        area_codes, timezone
    FROM
    zipcodes_csv_data;
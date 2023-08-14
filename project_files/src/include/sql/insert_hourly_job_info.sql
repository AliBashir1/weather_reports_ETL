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


DO $insert_record$
    DECLARE
        load_id VARCHAR(50);
    BEGIN
        CALL create_load_id(load_id:= load_id, daily_job := False);
        INSERT INTO  hourly_jobs_info_staged
        values (
            '{{ params.dag_id }}',
            load_id,
            '{{ params.run_id }}',
            '{{ params.successful_zipcodes_count }}',
            '{{ params.invalid_zipcodes_count }}',
                '{{ params.total_zipcodes }}',
            '{{ ti.xcom_pull(key="start_date") }}',
            '{{ ti.xcom_pull(key="end_date") }}',
            '{{ params.job_status }}',
            '{{ params.next_scheduled_dagrun_datetime }}',
            now()

                );

    END;
$insert_record$;

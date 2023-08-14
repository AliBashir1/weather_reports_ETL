DO $testing$
    DECLARE
        load_id varchar(50);
    begin
        call create_load_id(load_id := load_id, daily_job := True );
        raise notice '%', load_id; -- display result

        INSERT INTO daily_jobs_info("job_name",
                                    "daily_load_id",
                                    "job_run_id",
                                    "records_processed",
                                    "files_processed",
                                    "start_date", "end_date",
                                    "job_status", "created_at",
                                    "job_status_description",
                                    "next_scheduled_job"
                                    )
        VALUES (
                '{{ params.dag_id }}',
                load_id,
                '{{ params.run_id }}',
                '{{ params.records_processed }}',
                '{{ params.files_processed }}',
                '{{ ti.xcom_pull(key="start_date") }}',
                '{{ ti.xcom_pull(key="end_date") }}',
                '{{ params.job_status }}',
                 now(),
                'Expected files are 4 and expected records are 12488.' ||
                'This load has processed {{ params.files_processed }} files and {{ params.records_processed }} records.',
                '{{ params.next_scheduled_dagrun_datetime }}'
              );


    end;

$testing$;


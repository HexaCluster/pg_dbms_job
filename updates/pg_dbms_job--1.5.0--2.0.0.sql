-- Column all_scheduler_job_run_details.run_duration have been change to interval datatype
ALTER TABLE dbms_job.all_scheduler_job_run_details ALTER COLUMN run_duration TYPE interval USING (run_duration||' seconds')::interval;

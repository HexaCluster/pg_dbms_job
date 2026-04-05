-- Column all_scheduler_job_run_details.run_duration have been change to interval datatype
ALTER TABLE dbms_job.all_scheduler_job_run_details ALTER COLUMN run_duration TYPE interval USING (run_duration||' seconds')::interval;
-- NOTIFY is no more necessay with the background worker
DROP TRIGGER dbms_job_scheduled_notify_trg;
DROP FUNCTION dbms_job.job_scheduled_notify();
DROP TRIGGER dbms_job_async_notify_trg;
DROP FUNCTION dbms_job.job_async_notify();


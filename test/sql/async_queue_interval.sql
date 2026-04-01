-- Insert an asynchronous job that must be executed later
TRUNCATE TABLE dbms_job.all_scheduler_job_run_details;
SET ROLE regress_dbms_job_user;
DO $$
DECLARE
jobid bigint;
BEGIN
	-- Asynchronous job, must be executed 3 seconds later
	SELECT dbms_job.submit(
		'BEGIN PERFORM current_timestamp; END;', -- what
		LOCALTIMESTAMP + '5 seconds'::interval, NULL
	) INTO jobid;
END;
$$;

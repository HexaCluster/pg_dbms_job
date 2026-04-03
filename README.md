# pg_dbms_job

PostgreSQL extension to schedules and manages jobs in a job queue similar to Oracle DBMS_JOB package.

* [Description](#description)
* [Installation](#installation)
* [Manage the extension](#manage-the-extension)
* [Running the scheduler](#running-the-scheduler)
* [Configuration](#configuration)
* [Jobs definition](#jobs-definition)
  - [Scheduled jobs](#scheduled-jobs)
  - [Asynchronous jobs](#asynchronous-jobs)
* [View ALL_JOBS](#view-all_jobs)
* [Security](#secutity)
* [Jobs execution history](#jobs-execution-history)
* [Procedures](#procedures)
  - [BROKEN](#broken)
  - [CHANGE](#change)
  - [INTERVAL](#interval)
  - [NEXT_DATE](#next_date)
  - [REMOVE](#remove)
  - [RUN](#run)
  - [SUBMIT](#submit)
  - [WHAT](#what)
* [Limitations](#limitations)
* [Authors](#authors)
* [License](#license)

## [Description](#description)

This PostgreSQL extension provided full compatibility with the DBMS_JOB Oracle module.

It allows to manage scheduled jobs from a job queue or to execute immediately jobs asynchronously. A job definition consist on a code to execute, the next date of execution and how often the job is to be run. A job runs a SQL command, plpgsql code or an existing stored procedure.

If the submit stored procedure is called without the next_date (when) and interval (how often) attributes, the job is executed immediately in an asynchronous process. If interval is NULL and that next_date is lower or equal to current timestamp the job is also executed immediately as an asynchronous process. In all other cases the job is to be started when appropriate but if interval is NULL the job is executed only once and the job is deleted.

If a scheduled job completes successfully, then its new execution date is placed in next_date. The new date is calculated by evaluating the SQL expression defined as interval. The interval parameter must evaluate to a time in the future.

This extension consist in a SQL script to create all the objects related to its operation and a background worker that must be run attached to the database where jobs are defined. The background worker is responsible to execute the queued asynchronous jobs and the scheduled ones. It is run by PostgreSQL at startup throught the setting of `shared_preload_libraries`.

The number of jobs that can be executed at the same time is limited to 1000 by default. If this limit is reached the background worker will wait that a process ends to run a new one.

The scheduler polls every `job_queue_interval` seconds (5 seconds by default) the tables where job definition are stored. This mean that at worst a job will be executed `job_queue_interval` seconds after the next execution date defined.


## [Installation](#installation)

There is no special requirement to run this extension but your PostgreSQL version must support background workers (>= 9.3).

To install the extension execute

    make
    sudo make install

Test of the extension can be done by setting the following parameters in your postgresql.conf

    shared_preload_libraries = 'pg_dbms_job'                # (change requires restart)
    pg_dbms_job.debug = off
    # Poll interval of the job queue
    pg_dbms_job.job_queue_interval = 5
    # Maximum number of job processed at the same time
    pg_dbms_job.job_queue_processes = 10
    pg_dbms_job.database = regress_dbms_job
    pg_dbms_job.username = regress_dbms_job_dba
    pg_dbms_job.naptime = 100ms

restart PostgreSQL and execute the following command as a PostgreSQL superuser:

    make installcheck

The test script `t/01_basic.t` will create the `regress_dbms_job` database with users `regress_dbms_job_dba` and `regress_dbms_job_user` to run the tests.

## [Manage the extension](#manage-the-extension)

Each database that needs to use `pg_dbms_job` must creates the extension:

    psql -d mydb -c "CREATE EXTENSION pg_dbms_job"

To upgrade to a new version execute:

    psql -d mydb -c 'ALTER EXTENSION pg_dbms_job UPDATE TO "2.0.0"'

If you doesn't have the privileges to create an extension you can just import the extension file into the database, for example:

    psql -d mydb -f sql/pg_dbms_job--2.0.0.sql

This is especially useful for database n DBaas cloud services. To upgrade just import the extension upgrade files using psql.

A dedicated scheduler per database using the extension must be started.

## [Running the scheduler](#running-the-scheduler)

Starting with version 2.0.0 the scheduler is now a background worker so you just need to preload it as a library using:

    shared_preload_libraries = 'pg_dbms_job'

and restart PostgreSQL. 

In the postgresql.conf configuration file you must also define the database and superuser role to use to look at jobs and to control naptime.

The user must be the extension tables owner or have the superuser privileges to be able to bypass the Row Level Security rules defined on the pg_dbms_job tables.

## [Configuration](#configuration)

The format of the configuration file is the same as `postgresql.conf`.

### General

- `debug`: debug mode. Default 0, disabled.
- `job_queue_interval`: poll interval of the jobs queue in seconds. Default 5 seconds, max 3600 seconds.
- `job_queue_processes`: Maximum number of job processed at the same time. Default `1000`.
- `naptime`: Time to wait in the main loop before each run. Default `100ms`.
- `database`: name of the database where the extension is created. Default the `postgres` database.
- `username` = name of the role owner of the extension's tables. Default bootstrap superuser.


### Example
```
# Add settings for extensions here
shared_preload_libraries = 'pg_dbms_job'                # (change requires restart)
# Toogle debug mode
#pg_dbms_job.debug = on
# Poll interval of the job queue
pg_dbms_job.job_queue_interval = 5
#Maximum number of job processed at the same time
pg_dbms_job.job_queue_processes = 10
pg_dbms_job.database = regress_dbms_job
pg_dbms_job.username = regress_dbms_job_dba # superuser to work with the extension
pg_dbms_job.naptime = 100ms

```

## [Jobs definition](#jobs-definition)

### [Scheduled jobs](#scheduled-jobs)

Jobs to run are stored in table `dbms_job.all_scheduled_jobs` which is the same structure as the one in Oracle. Some columns are just here for compatibility but are not used. They are executed when current timestamp of the database polled by the scheduler is upper or equal to the date defined in the `next_date` attribute.

Unlike with cron-like scheduler, when the pg_dbms_job scheduler starts it executes all active jobs with a next date in the past. That also mean that the interval of execution will be the same but the first execution date will change.

```
CREATE TABLE dbms_job.all_scheduled_jobs
(
	job bigint DEFAULT nextval('dbms_job.jobseq') PRIMARY KEY, -- identifier of job
	log_user name DEFAULT current_user, -- user that submit the job
	priv_user name DEFAULT current_user, -- user whose default privileges apply to this job (not used)
	schema_user text DEFAULT current_setting('search_path'), -- default schema used to parse the job
	last_date timestamp with time zone, -- date on which this job last successfully executed
	last_sec text, -- same as last_date (not used)
	this_date timestamp with time zone, -- date that this job started executing, null when the job is not running
	this_sec text, -- same as this_date (not used)
	next_date timestamp(0) with time zone NOT NULL, -- date that this job will next be executed
	next_sec timestamp with time zone, -- same as next_date (not used)
	total_time interval, -- total wall clock time spent by the system on this job, in seconds
	broken boolean DEFAULT false, -- true: no attempt is made to run this job, false: an attempt is made to run this job
	interval text, -- a date function, evaluated at the start of execution, becomes next next_date
	failures bigint, -- number of times the job has started and failed since its last success
	what text  NOT NULL, -- body of the anonymous pl/sql block that the job executes
	nls_env text, -- session parameters describing the nls environment of the job (not used)
	misc_env bytea, -- Other session parameters that apply to this job (not used)
	instance integer DEFAULT 0 -- ID of the instance that can execute or is executing the job (not used)
);
```

### [Asynchronous jobs](#asynchronous-jobs)

Job submitted without execution date are jobs that need to be executed asynchronously as soon as possible after being created. They are stored in the queue (FIFO) table `dbms_job.all_async_jobs`.

Same as for scheduled jobs, if jobs exist in the queue at start of the scheduler, they are executed immediately.

```
CREATE TABLE dbms_job.all_async_jobs
(
        job bigint DEFAULT nextval('dbms_job.jobseq') PRIMARY KEY, -- identifier of job
        log_user name DEFAULT current_user, -- user that submit the job
        schema_user text DEFAULT current_setting('search_path'), -- default search_path used to execute the job
        create_date timestamp with time zone DEFAULT current_timestamp, -- date on which this job has been created.
        what text NOT NULL -- body of the anonymous pl/sql block that the job executes
);
```
## [View ALL_JOBS](#view-all_jobs)

All jobs that have to be executed can be listed from the view `dbms_job.all_jobs`, this is the equivalent of the Oracle table DBMS_JOB.ALL_JOBS. This view reports all jobs to be run by execution a union between the two tables described in previous chapters.

## [Security](#secutity)

Jobs are only visible by their own creator. A user can not access to a job defined by an other user unless it has the superuser privileges or it is the owner of the pg_dbms_job tables.

By default a user can not use pg_dbms_job, he must be granted privileges to the pg_dbms_job objects as follow.

```
GRANT USAGE ON SCHEMA dbms_job TO <role>;
GRANT ALL ON ALL TABLES IN SCHEMA dbms_job TO <role>;
GRANT ALL ON ALL SEQUENCES IN SCHEMA dbms_job TO <role>;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_job TO <role>;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA dbms_job TO <role>;
```

A job will be taken in account by the scheduler only when the transaction where it has been created is committed. It is transactional so no risk that it will be executed if the transaction is aborted.

By default the scheduler allow 1000 jobs to be executed at the same time, you may want to control this value to a lower or a upper value. This limit can be changed in the configuration file with directive `job_queue_processes`. Note that if your system doesn't have enough resources to run all the jobs at the same time it could be problematic. You must also take attention to who is authorised to submit jobs because this could affect the performances of the server.

Jobs are executed with as the user that defined the job and with the search path used at the time of the job submission. This information is available in attributes `log_user` and `schema_user` of table `dbms_job.all_scheduled_jobs` and `dbms_job.all_async_jobs`. That mean that the pg_dbms_job user of the scheduler must have the privilege to change the user using `SET ROLE <jobuser>.`. This allow the user that have submitted the job to view its entries in the history table. This user can be defined using `pg_dbms_job.username` with default to `postgres`.


## [Jobs execution history](#jobs-execution-history)

Oracle DBMS_JOB doesn't provide a log history. This feature is available in DBMS_SCHEDULER and the past activity of the scheduler can be seen in table ALL_SCHEDULER_JOB_RUN_DETAILS. This extension stores all PG_DBMS_JOB activity in a similar table named `dbms_job.all_scheduler_job_run_details`.

```
CREATE TABLE dbms_job.all_scheduler_job_run_details
(
        log_id bigserial PRIMARY KEY, -- unique identifier of the log entry
        log_date timestamp with time zone DEFAULT current_timestamp, -- date of the log entry
        owner name, -- owner of the scheduler job
        job_name varchar(261), -- name of the scheduler job
        job_subname varchar(261), -- Subname of the Scheduler job (for a chain step job)
        status varchar(128), -- status of the job run
        error char(5), -- error code in the case of an error
        req_start_date timestamp with time zone, -- requested start date of the job run
        actual_start_date timestamp with time zone, -- actual date on which the job was run
        run_duration interval, -- duration of the job run in seconds, stored as an interval
        instance_id integer, -- identifier of the instance on which the job was run
        session_id integer, -- session identifier of the job run
        slave_pid integer, -- process identifier of the slave on which the job was run
        cpu_used integer, -- amount of cpu used for the job run
        additional_info text -- additional information on the job run, error message, etc.
);
```

## [Procedures](#procedures)

### [BROKEN](#broken)

Disables or suspend job execution. This procedure sets the broken flag. Broken jobs are never run.

Syntax:

	pg_dbms_job.broken ( 
		job       IN  bigint,
		broken    IN  boolean
		next_date IN  timestamp DEFAULT current_timestamp);

Parameters:

- job : ID of the job being run.
- broken : Sets the job as broken or not broken. `true` sets it as broken; `false` sets it as not broken.
- next_date : Next date when the job will be run, default is `current_timestamp`.

If you set job as broken while it is running, unlike Oracle, the scheduler will not reset the job's status to normal after the job completes. Therefore, you can execute this procedure for jobs when they are running they will be disabled.

Example:

	BEGIN;
	CALL pg_dbms_job.broken(12345, true);
	COMMIT;

### [CHANGE](#change)

Alters any of the user-definable parameters associated with a job. Any value you do not want to change can be specified as NULL.

Syntax:

	dbms_job.change ( 
		job       IN  bigint,
		what      IN  text,
		next_date IN  timestamp with time zone,
		interval  IN  text
		[, instance  IN  integer DEFAULT 0,
		   force     IN  boolean DEFAULT false ]);

Parameters:

- job : ID of the job being run.
- what : PL/SQL procedure to run.
- next_date : Next date when the job will be run.
- interval : Date function; evaluated immediately before the job starts running.
- instance : unused
- force : unused

Your job change will not be available for processing by the job queue in the background until it is committed.
If the parameters what, next_date, or interval are NULL, then leave that value as it is.

Example:

Change the interval of execution of job 12345 to run every 3 days

	BEGIN;
	CALL pg_dbms_job.change(12345, null, null, 'current_timestamp + ''3 days''::interval');
	COMMIT;

### [INTERVAL](#interval)

Alters the interval between executions for a specified job

Syntax:

	dbms_job.interval ( 
		job       IN  bigint,
		interval  IN  text);

Parameters:

- job : ID of the job being run.
- interval : Code of the date function, evaluated immediately before the job starts running.

If the job completes successfully, then this new date is placed in next_date. `interval` is evaluated by plugging it into the statement select interval into next_date;

The interval parameter must evaluate to a time in the future.

If interval evaluates to NULL and if a job completes successfully, then the job is automatically deleted from the queue.

With Oracle this is the kind of interval values that we can find:

- Execute daily: `SYSDATE + 1`
- Execute once per week: `SYSDATE + 7`
- Execute hourly: `SYSDATE + 1/24`
- Execute every 2 hour: `SYSDATE + 2/24`
- Execute every 12 hour: `SYSDATE + 12/24`
- Execute every 10 min.: `SYSDATE + 10/1440`
- Execute every 30 sec.: `SYSDATE + 30/86400`

The equivalent to use with pg_dbms_job are the following:

- Execute daily: `date_trunc('second',LOCALTIMESTAMP) + '1 day'::interval`
- Execute once per week: `date_trunc('second',LOCALTIMESTAMP) + '7 days'::interval` or `date_trunc('second',current_timestamp) + '1 week'::interval`
- Execute hourly: `date_trunc('second',LOCALTIMESTAMP) + '1 hour'::interval`
- Execute every 2 hour: `date_trunc('second',LOCALTIMESTAMP) + '2 hours'::interval`
- Execute every 12 hour: `date_trunc('second',LOCALTIMESTAMP) + '12 hours'::interval`
- Execute every 10 min.: `date_trunc('second',LOCALTIMESTAMP) + '10 minutes'::interval`
- Execute every 30 sec.: `date_trunc('second',LOCALTIMESTAMP) + '30 secondes'::interval`

Example:

	BEGIN;
	CALL pg_dbms_job.interval(12345, 'current_timestamp + '10 seconds'::interval);
	COMMIT;

### [NEXT_DATE](#next_date)

Alters the next execution time for a specified job

Syntax:

	dbms_job.next_date ( 
		job       IN  bigint,
		next_date IN  timestamp with time zone);

Parameters:

- job : ID of the job being run.
- next_date : Date of the next refresh: it is when the job will be automatically run, assuming there are background processes attempting to run it.

Example:

	BEGIN;
	CALL pg_dbms_job.next_date(12345, current_timestamp + '1 day'::interval);
	COMMIT;

### [REMOVE](#remove)

Removes specified job from the job queue. You can only remove jobs that you own. If this is run while the job is executing, it will not be interrupted but will not be run again.

Syntax:

	dbms_job.remove ( 
		job       IN  bigint);

Parameters:

- job : ID of the job being run.

Example:

	BEGIN;
	CALL pg_dbms_job.remove(12345);
	COMMIT;

### [RUN](#run)

Forces a specified job to run. This procedure runs the job now. It runs even if it is broken. If it was broken and it runs successfully, the job is updated to indicates that it is no longer broken and goes back to running on its schedule.

Running the job recomputes next_date based on the time you run the procedure.

When runs in foreground there is no logging to the jobs history table but information on the dbms_job.all_scheduled_jobs table are updated in case of error or success. In case of error the exception is raise to the client.

Syntax:

	dbms_job.run ( 
		job       IN  bigint);

Parameters:

- job : ID of the job being run.

Example:

	BEGIN;
	CALL pg_dbms_job.run(12345, false);
	COMMIT;

### [SUBMIT](#submit)

Submits a new job to the job queue. It chooses the job from the sequence dbms_job.jobseq.

Actually this is a function as PostgreSQL < 14 do not support out parameters.

Syntax

	dbms_job.submit ( 
		job       OUT bigint,
		what      IN  text,
		[ next_date IN  timestamp(0) with time zone DEFAULT current_timestamp
		[ , interval  IN  text DEFAULT NULL
		[ , no_parse  IN  boolean DEFAULT false ] ] ] );

Parameters:

- job : ID of the job being run.
- what : text of the code to the job to be run. This must be a valid SQL statement or block of plpgsql code. The SQL code that you submit in the `what` parameter is wrapped in the following plpgsql block:
```
DO $$
DECLARE
    job bigint := $jobid;
    next_date timestamp with time zone := current_timestamp;
    broken boolean := false;
BEGIN
    WHAT
END;
$$;
```

Ensure that you include the ; semi-colon with the statement.

- next_date : Next date when the job will be run.
- interval : Date function that calculates the next time to run the job. The default is NULL. This must evaluate to a either a future point in time or NULL.
- no_parse : Unused.

Example:

This submits a new job to the job queue. The job calls ANALYZE to generate optimizer statistics for the table public.accounts. The job is run every 24 hours:

	BEGIN;
	DO $$
	DECLARE
	    jobno bigint;
	BEGIN
	   SELECT dbms_job.submit(
	      'ANALYZE public.accounts.',
	      LOCALTIMESTAMP, 'LOCALTIMESTAMP + ''1 day''::interval') INTO jobno;
	END;
	COMMIT;

### [WHAT](#what)

Alters the job description for a specified job. This procedure changes what an existing job does, and replaces its environment.

Syntax:

	dbms_job.what ( 
		job       IN  bigint,
		what      IN  text);

Parameters:

- job : ID of the job being run.
- what : PL/SQL procedure to run.

Example:

	BEGIN;
	CALL dbms_job.what('ANALYZE public.accounts.');
	COMMIT;

## [Limitations](#limitations)

Following the job activity a certain amount of bloat can be created in queues tables which can slow down the collect of job to execute by the scheduler. In this case it is recommended to execute a VACUUM FULL on these tables periodically when there is no activity.

```
VACUUM FULL dbms_job.all_scheduled_jobs, dbms_job.all_async_jobs;
```

If you have a very high job execution use that generates thousands of NOTIFY per seconds you should better disable this feature to avoid filling the notify queue. The queue is quite large (8GB in a standard installation) but when it is full the transaction that emit the NOTIFY will fail.  Once the queue is half full you will see warnings in the log file. If you experience this limitation you can disable this feature by dropping the triggers responsible of the notification.
```
DROP TRIGGER dbms_job_scheduled_notify_trg ON dbms_job.all_scheduled_jobs;
DROP TRIGGER dbms_job_async_notify_trg ON dbms_job.all_async_jobs;
```
Once the trigger are dropped the polling of job will only be done every `job_queue_interval` seconds (5 seconds by default).

## [Authors](#authors)

- Gilles Darold

## [License](#license)

This extension is free software distributed under the PostgreSQL
License.

    Copyright (c) 2021-2023 MigOps Inc.
    Copyright (c) 2024-2026 Hexacluster Corp.

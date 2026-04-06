/*-------------------------------------------------------------------------
 * pg_dbms_job.c
 *
 *   PostgreSQL background worker that emulates Oracle DBMS_JOB scheduling.
 *   Rewritten from the original Perl standalone daemon by Gilles Darold.
 *
 * Architecture:
 *   - One "leader" bgworker (pg_dbms_job:main) registered at preload time.
 *     It polls the job tables and dynamically registers a short-lived child
 *     bgworker (pg_dbms_job:worker:<jobid>) for every job that is due.
 *   - All DB access uses the SPI interface (no external libpq needed).
 *   - Configuration lives in postgresql.conf GUC parameters.
 *   - The leader uses WaitLatch() for its sleep so it wakes immediately on
 *     SIGTERM or postmaster death.
 *
 * Installation:
 *   make && make install          (uses PGXS)
 *   -- then in postgresql.conf:
 *   shared_preload_libraries = 'pg_dbms_job'
 *   pg_dbms_job.database = 'mydb'
 *
 * Original Author : Gilles Darold <gilles@darold.net>
 * IA              : Claude have been used to translate the Perl daemon into
 *                   a PG background worker. Reviewed and fixed by the author.
 * Licence         : PostgreSQL
 * Copyright (c) 2021-2023, MigOps Inc.
 * Copyright (c) 2024-2026, Hexacluster Corp.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <time.h>
#include <sys/time.h>

#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

#define PGDJ_VERSION   "1.5"
#define PGDJ_APPNAME   "pg_dbms_job"

/* * GUC variables  to be set in postgresql.conf */
static char *pgdj_database        = NULL;
static char *pgdj_username        = NULL;
static int   pgdj_naptime         = 100;   /* ms between main-loop ticks   */
static int   pgdj_queue_processes = 1000;  /* max parallel job workers     */
static int   pgdj_queue_interval  = 5;     /* s: forced full-poll period   */
static bool  pgdj_debug           = false;

/* Signal flags */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup  = false;

/* -------------------------------------------------------------------------
 * Per-job descriptor – collected by the leader, one entry per due job
 * -------------------------------------------------------------------------
 */
typedef struct JobDesc {
    long long  jobid;
    char      *what;
    char      *log_user;
    char      *schema_user;
    bool       is_scheduled;
} JobDesc;

static JobDesc *pending_jobs     = NULL;
static int      pending_jobs_cnt = 0;
static int      pending_jobs_cap = 0;

/* Counter of dynamic workers the leader has spawned */
static int running_workers = 0;

/* functions declarations */
void _PG_init(void);
PGDLLEXPORT void pgdj_main(Datum main_arg);
PGDLLEXPORT void pgdj_worker_main(Datum main_arg);

static void pgdj_sigterm(SIGNAL_ARGS);
static void pgdj_sighup(SIGNAL_ARGS);

static void jobs_reset(void);
static void jobs_add(long long jobid,
                     const char *what, const char *log_user,
                     const char *schema_user, bool is_scheduled);

static int  fetch_scheduled_jobs(void);
static int  fetch_async_jobs(void);
static void spawn_job_worker(long long jobid, bool is_scheduled);

static void run_job(long long jobid, bool is_scheduled);
static void update_scheduled_success(long long jobid, double duration_secs);
static void update_scheduled_failure(long long jobid);
static void delete_job(long long jobid);
static void store_run_details(const char *owner, long long jobid,
                              const char *start_ts, double duration,
                              const char *status, const char *errmsg,
                              bool success, const char *sqlstate);

/* _PG_init  –  called when the shared library is loaded by the postmaster */
void
_PG_init(void)
{
    BackgroundWorker worker;

    if (!process_shared_preload_libraries_in_progress)
        ereport(ERROR,
                (errmsg("pg_dbms_job must be loaded via shared_preload_libraries")));

    /* ---- GUC definitions ---- */
    DefineCustomStringVariable(
        "pg_dbms_job.database",
        "Database in which pg_dbms_job schema lives.",
        NULL, &pgdj_database, "postgres",
        PGC_POSTMASTER, 0, NULL, NULL, NULL);

    DefineCustomStringVariable(
        "pg_dbms_job.username",
        "Role used by pg_dbms_job workers (NULL = bootstrap superuser).",
        NULL, &pgdj_username, NULL,
        PGC_POSTMASTER, 0, NULL, NULL, NULL);

    DefineCustomIntVariable(
        "pg_dbms_job.naptime",
        "Milliseconds to sleep between main-loop iterations.",
        NULL, &pgdj_naptime, 100, 1, 60000,
        PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable(
        "pg_dbms_job.job_queue_processes",
        "Maximum number of concurrent job worker processes.",
        NULL, &pgdj_queue_processes, 1000, 1, 8192,
        PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable(
        "pg_dbms_job.job_queue_interval",
        "Seconds between forced full job-queue polls.",
        NULL, &pgdj_queue_interval, 5, 1, 3600,
        PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomBoolVariable(
        "pg_dbms_job.debug",
        "Enable verbose debug logging.",
        NULL, &pgdj_debug, false,
        PGC_SIGHUP, 0, NULL, NULL, NULL);

    /* ---- Register the permanent leader background worker ---- */
    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags      = BGWORKER_SHMEM_ACCESS |
                            BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 5;   /* auto-restart 5 s after crash */

    snprintf(worker.bgw_name,          BGW_MAXLEN, "%s:main",  PGDJ_APPNAME);
#if PG_VERSION_NUM >= 110000
    snprintf(worker.bgw_type,          BGW_MAXLEN, "%s",       PGDJ_APPNAME);
#endif
#if PG_VERSION_NUM <= 170000
    snprintf(worker.bgw_library_name,  BGW_MAXLEN,  "%s",       PGDJ_APPNAME);
#else
    snprintf(worker.bgw_library_name,  MAXPGPATH,  "%s",       PGDJ_APPNAME);
#endif
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "pgdj_main");
    worker.bgw_main_arg   = (Datum) 0;
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}

/* Signal handlers */
static void
pgdj_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void
pgdj_sighup(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

/* Job helpers */
static void
jobs_reset(void)
{
    pending_jobs_cnt = 0;
}

static void
jobs_add(long long jobid, const char *what,
         const char *log_user, const char *schema_user,
         bool is_scheduled)
{
    JobDesc *j;

    if (pending_jobs_cnt >= pending_jobs_cap)
    {
        int newcap = pending_jobs_cap == 0 ? 64 : pending_jobs_cap * 2;
        pending_jobs = (JobDesc *) repalloc(pending_jobs,
                                            newcap * sizeof(JobDesc));
        pending_jobs_cap = newcap;
    }
    j = &pending_jobs[pending_jobs_cnt++];
    j->jobid        = jobid;
    j->what         = pstrdup(what        ? what        : "");
    j->log_user     = pstrdup(log_user    ? log_user    : "");
    j->schema_user  = pstrdup(schema_user ? schema_user : "");
    j->is_scheduled = is_scheduled;
}

/* =========================================================================
 * fetch_scheduled_jobs
 *
 * UPDATE ... RETURNING the rows that are due, mark them with this_date so
 * they won't be picked up again by concurrent calls.
 * Returns count of jobs added to pending_jobs.
 * =========================================================================
 */
static int
fetch_scheduled_jobs(void)
{
    int    ret;
    uint64 nrows;
    int    added = 0;

    const char *sql =
        "UPDATE dbms_job.all_scheduled_jobs SET"
        "  this_date = current_timestamp,"
        "  next_date = dbms_job.get_next_date(interval),"
        "  instance  = instance + 1"
        " WHERE interval IS NOT NULL"
        "   AND NOT broken"
        "   AND this_date IS NULL"
        "   AND next_date <= current_timestamp"
        " RETURNING job, what, log_user, schema_user";

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    ret = SPI_execute(sql, false, 0);
    if (ret != SPI_OK_UPDATE_RETURNING)
    {
        elog(WARNING, "%s: fetch_scheduled_jobs SPI error %d",
             PGDJ_APPNAME, ret);
        SPI_finish();
        PopActiveSnapshot();
        AbortCurrentTransaction();
        return 0;
    }

    nrows = SPI_processed;
    for (uint64 i = 0; i < nrows; i++)
    {
        bool  isnull;
        long long jobid = DatumGetInt64(
            SPI_getbinval(SPI_tuptable->vals[i],
                          SPI_tuptable->tupdesc, 1, &isnull));
        char *what        = SPI_getvalue(SPI_tuptable->vals[i],
                                         SPI_tuptable->tupdesc, 2);
        char *log_user    = SPI_getvalue(SPI_tuptable->vals[i],
                                         SPI_tuptable->tupdesc, 3);
        char *schema_user = SPI_getvalue(SPI_tuptable->vals[i],
                                         SPI_tuptable->tupdesc, 4);
        if (pgdj_debug)
            elog(LOG, "%s: found scheduled jobs: %s", PGDJ_APPNAME, what);

        jobs_add(jobid, what, log_user, schema_user, true);
        added++;
    }

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();

    if (pgdj_debug)
        elog(LOG, "%s: found %d scheduled jobs to run", PGDJ_APPNAME, added);
    return added;
}

/* =========================================================================
 * fetch_async_jobs
 *
 * Collects:
 *   1. Rows from all_async_jobs with this_date IS NULL
 *   2. Rows from all_scheduled_jobs where interval IS NULL
 *      and next_date <= now (one-shot jobs)
 * =========================================================================
 */
static int
fetch_async_jobs(void)
{
    int    ret;
    uint64 nrows;
    int    added = 0;

    const char *sql1 =
        "UPDATE dbms_job.all_async_jobs SET"
        "  this_date = current_timestamp"
        " WHERE this_date IS NULL"
        " RETURNING job, what, log_user, schema_user";

    const char *sql2 =
        "UPDATE dbms_job.all_scheduled_jobs SET"
        "  this_date = current_timestamp,"
	"  next_date = 'Infinity'"
        " WHERE this_date IS NULL"
        "   AND interval IS NULL"
        "   AND next_date <= current_timestamp"
        " RETURNING job, what, log_user, schema_user";
    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    /* -- all_async_jobs -- */
    ret = SPI_execute(sql1, false, 0);
    if (ret == SPI_OK_UPDATE_RETURNING)
    {
        nrows = SPI_processed;
        for (uint64 i = 0; i < nrows; i++)
        {
            bool isnull;
            long long jobid = DatumGetInt64(
                SPI_getbinval(SPI_tuptable->vals[i],
                              SPI_tuptable->tupdesc, 1, &isnull));
            char *what        = SPI_getvalue(SPI_tuptable->vals[i],
                                             SPI_tuptable->tupdesc, 2);
            char *log_user    = SPI_getvalue(SPI_tuptable->vals[i],
                                             SPI_tuptable->tupdesc, 3);
            char *schema_user = SPI_getvalue(SPI_tuptable->vals[i],
                                             SPI_tuptable->tupdesc, 4);
            if (pgdj_debug)
                elog(LOG, "%s: found async job: %s", PGDJ_APPNAME, what);

            jobs_add(jobid, what, log_user, schema_user, false);
            added++;
        }
    }
    else
    {
        elog(WARNING, "%s: fetch_async_jobs (all_async_jobs) SPI error %d",
             PGDJ_APPNAME, ret);
    }

    /* -- one-shot scheduled jobs -- */
    ret = SPI_execute(sql2, false, 0);
    if (ret == SPI_OK_UPDATE_RETURNING)
    {
        nrows = SPI_processed;
        for (uint64 i = 0; i < nrows; i++)
        {
            bool isnull;
            long long jobid = DatumGetInt64(
                SPI_getbinval(SPI_tuptable->vals[i],
                              SPI_tuptable->tupdesc, 1, &isnull));
            char *what        = SPI_getvalue(SPI_tuptable->vals[i],
                                             SPI_tuptable->tupdesc, 2);
            char *log_user    = SPI_getvalue(SPI_tuptable->vals[i],
                                             SPI_tuptable->tupdesc, 3);
            char *schema_user = SPI_getvalue(SPI_tuptable->vals[i],
                                             SPI_tuptable->tupdesc, 4);
            if (pgdj_debug)
                elog(LOG, "%s: found delayed job: %s", PGDJ_APPNAME, what);

            jobs_add(jobid, what, log_user, schema_user, true);
            added++;
        }
    }
    else
    {
        elog(WARNING,
             "%s: fetch_async_jobs (one-shot scheduled) SPI error %d",
             PGDJ_APPNAME, ret);
    }

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();

    if (pgdj_debug)
        elog(LOG, "%s: found %d jobs to run", PGDJ_APPNAME, added);

    return added;
}

/* =========================================================================
 * spawn_job_worker
 *
 * Register a dynamic one-shot background worker for job <jobid>.
 * The job type and id are encoded in bgw_extra as  "<s|a>:<jobid>".
 * =========================================================================
 */
static void
spawn_job_worker(long long jobid, bool is_scheduled)
{
    BackgroundWorker        worker;
    BackgroundWorkerHandle *handle;

    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags      = BGWORKER_SHMEM_ACCESS |
                            BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;  /* one-shot, never restart */

    snprintf(worker.bgw_name, BGW_MAXLEN,
             "%s:%s:%lld",
             PGDJ_APPNAME,
             is_scheduled ? "scheduled" : "async",
             jobid);

#if PG_VERSION_NUM >= 110000
    snprintf(worker.bgw_type,          BGW_MAXLEN, "%s:worker", PGDJ_APPNAME);
#endif
#if PG_VERSION_NUM <= 170000
    snprintf(worker.bgw_library_name,  BGW_MAXLEN,  "%s",       PGDJ_APPNAME);
#else
    snprintf(worker.bgw_library_name,  MAXPGPATH,  "%s",       PGDJ_APPNAME);
#endif
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "pgdj_worker_main");

    /* Pass "<s|a>:<jobid>" to the child via bgw_extra */
    snprintf(worker.bgw_extra, BGW_EXTRALEN,
             "%c:%lld",
             is_scheduled ? 's' : 'a',
             jobid);
    if (pgdj_debug)
        elog(LOG, "%s: spawn information passed in bgw_extra: %s", PGDJ_APPNAME, worker.bgw_extra);

    /*
     * Setting bgw_notify_pid causes the postmaster to send SIGUSR1 to us
     * when the worker exits, which wakes our WaitLatch so we can decrement
     * the running_workers counter promptly.
     */
    worker.bgw_notify_pid = MyProcPid;
    worker.bgw_main_arg   = (Datum) 0;

    if (!RegisterDynamicBackgroundWorker(&worker, &handle))
    {
        elog(WARNING, "%s: could not register worker for job %lld",
             PGDJ_APPNAME, jobid);
        return;
    }

    running_workers++;

    if (pgdj_debug)
        elog(LOG, "%s: spawned worker for %s job %lld",
             PGDJ_APPNAME,
             is_scheduled ? "scheduled" : "async",
             jobid);
}

/* =========================================================================
 * pgdj_main  –  leader worker entry point
 * =========================================================================
 */
void
pgdj_main(Datum main_arg)
{
    TimestampTz last_async_poll     = 0;
    TimestampTz last_scheduled_poll = 0;
    bool        startup             = true;

    /* Customise signal handlers before unblocking */
    pqsignal(SIGTERM, pgdj_sigterm);
    pqsignal(SIGHUP,  pgdj_sighup);
    BackgroundWorkerUnblockSignals();

    /* Connect to the target database as configured */
    BackgroundWorkerInitializeConnection(pgdj_database,
                                         pgdj_username
#if PG_VERSION_NUM >= 110000
                                         , 0
#endif
					 );

    pgstat_report_appname(PGDJ_APPNAME ":main");

    if (pgdj_debug)
        elog(LOG, "%s version %s started, managing jobs in database \"%s\"",
             PGDJ_APPNAME, PGDJ_VERSION,
             pgdj_database ? pgdj_database : "postgres");

    /* Allocate job list in TopMemoryContext so it survives per-loop resets */
    pending_jobs = (JobDesc *)
        MemoryContextAlloc(TopMemoryContext, 64 * sizeof(JobDesc));
    pending_jobs_cap = 64;
    pending_jobs_cnt = 0;

    /* Main loop */
    while (!got_sigterm)
    {
        int         rc;
        TimestampTz now;
        bool        do_async;
        bool        do_scheduled;

        /*
         * Sleep for naptime ms, or until woken by:
         *   - SIGTERM  (got_sigterm)
         *   - SIGHUP   (got_sighup)
         *   - SIGUSR1  from postmaster when a dynamic worker exits
         *   - Postmaster death
         */
        rc = WaitLatch(MyLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                       pgdj_naptime,
                       PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);

        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        if (got_sigterm)
            break;

        /* Reload GUCs on SIGHUP */
        if (got_sighup)
        {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
            elog(LOG, "%s: configuration reloaded", PGDJ_APPNAME);
        }

        /*
         * When a dynamic worker exits the postmaster sends us SIGUSR1,
         * which wakes the latch.  Use that as a hint to decrement our
         * counter (conservative: we may decrement more than one per tick
         * if multiple workers exit between ticks, but we never go < 0).
         */
        if (running_workers > 0)
            running_workers--;

        now = GetCurrentTimestamp();

        do_async = startup ||
                   TimestampDifferenceExceeds(last_async_poll, now,
                                              pgdj_queue_interval * 1000);
        do_scheduled = startup ||
                       TimestampDifferenceExceeds(last_scheduled_poll, now,
                                                  pgdj_queue_interval * 1000);

        startup = false;
        jobs_reset();

        /* Collect due jobs from the database */
        if (do_scheduled)
        {
            fetch_scheduled_jobs();
            last_scheduled_poll = GetCurrentTimestamp();
        }
        if (do_async)
        {
            fetch_async_jobs();
            last_async_poll = GetCurrentTimestamp();
        }

        /* Dispatch one worker per pending job */
        for (int i = 0; i < pending_jobs_cnt; i++)
        {
            /* Wait if at the concurrency ceiling */
            while (running_workers >= pgdj_queue_processes)
            {
                elog(WARNING,
                     "%s: job_queue_processes limit (%d) reached, waiting",
                     PGDJ_APPNAME, pgdj_queue_processes);

                rc = WaitLatch(MyLatch,
                               WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                               1000,
                               PG_WAIT_EXTENSION);
                ResetLatch(MyLatch);

                if (rc & WL_POSTMASTER_DEATH)
                    proc_exit(1);
                if (got_sigterm)
		{
		    elog(LOG, "%s: scheduler shutting down", PGDJ_APPNAME);
		    proc_exit(0);
	        }

                if (running_workers > 0)
                    running_workers--;
            }

            if (pgdj_debug)
                elog(LOG, "%s: spawn_job_worker for job %lld scheduled %d",
				PGDJ_APPNAME, pending_jobs[i].jobid,
				pending_jobs[i].is_scheduled);
            spawn_job_worker(pending_jobs[i].jobid,
                             pending_jobs[i].is_scheduled);
        }
    }

    elog(LOG, "%s: scheduler shutting down", PGDJ_APPNAME);
    proc_exit(0);
}

/* =========================================================================
 * run_job  –  execute one job inside the per-job worker process
 *
 * Steps:
 *   1. Read what / log_user / schema_user from the DB.
 *   2. Execute the PL/pgSQL DO block.
 *   3. Update job metadata (success/failure counters).
 *   4. Store run history in all_scheduler_job_run_details.
 * =========================================================================
 */
static void
run_job(long long jobid, bool is_scheduled)
{
    StringInfoData  sql;
    int             ret;
    char           *what        = NULL;
    char           *log_user    = NULL;
    char           *schema_user = NULL;
    const char     *src_table   = is_scheduled
                                  ? "dbms_job.all_scheduled_jobs"
                                  : "dbms_job.all_async_jobs";
    struct timeval tv0, tv1;
    bool  success  = true;
    char  errtext[2048] = "";
    char  sqlstate[16]  = "";
    char  pqstatus[64]  = "Query returning no data success";
    char start_ts[32];
    time_t t;
    double duration;
    char   buf[32];
    struct tm *tms;

    /* ------------------------------------------------------------------
     * Step 1: read job definition
     * ------------------------------------------------------------------
     */
    initStringInfo(&sql);
    appendStringInfo(&sql,
        "SELECT what, log_user, schema_user FROM %s WHERE job = %lld",
        src_table, jobid);

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    ret = SPI_execute(sql.data, true /* read-only */, 0);
    if (ret != SPI_OK_SELECT || SPI_processed == 0)
    {
        elog(WARNING, "%s: job %lld not found in %s – skipping",
             PGDJ_APPNAME, jobid, src_table);
        SPI_finish();
        PopActiveSnapshot();
        AbortCurrentTransaction();
        return;
    }

    what        = SPI_getvalue(SPI_tuptable->vals[0],
                               SPI_tuptable->tupdesc, 1);
    log_user    = SPI_getvalue(SPI_tuptable->vals[0],
                               SPI_tuptable->tupdesc, 2);
    schema_user = SPI_getvalue(SPI_tuptable->vals[0],
                               SPI_tuptable->tupdesc, 3);

    /* Copy strings before SPI_finish invalidates them */
    what        = what        ? pstrdup(what)        : pstrdup("");
    log_user    = log_user    ? pstrdup(log_user)    : pstrdup("");
    schema_user = schema_user ? pstrdup(schema_user) : pstrdup("");

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();

    /* ------------------------------------------------------------------
     * Step 2: execute the job's code
     * ------------------------------------------------------------------
     */
    gettimeofday(&tv0, NULL);

    /* Capture start timestamp string for history table */
    t   = (time_t) tv0.tv_sec;
    tms = localtime(&t);
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tms);
    what = pstrdup(what);           /* already done above; harmless */
    /* store start_ts separately */
    (void) buf;  /* used below via local var */

    t = (time_t) tv0.tv_sec;
    strftime(start_ts, sizeof(start_ts),
                 "%Y-%m-%d %H:%M:%S", localtime(&t));

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    /* SET ROLE */
    if (log_user[0])
    {
        resetStringInfo(&sql);
        appendStringInfo(&sql, "SET ROLE %s", quote_identifier(log_user));
        ret = SPI_execute(sql.data, false, 0);
        if (ret < 0)
        {
            elog(WARNING, "%s: job %lld: SET ROLE %s failed (SPI %d)",
                 PGDJ_APPNAME, jobid, log_user, ret);
            SPI_finish();
            PopActiveSnapshot();
            AbortCurrentTransaction();
            return;
        }
    }

    /* SET LOCAL search_path */
    if (schema_user[0])
    {
        resetStringInfo(&sql);
        appendStringInfo(&sql, "SET LOCAL search_path TO %s", schema_user);
        ret = SPI_execute(sql.data, false, 0);
        if (ret < 0)
            elog(WARNING, "%s: job %lld: SET search_path failed",
                 PGDJ_APPNAME, jobid);
    }

    /* Build the DO block */
    resetStringInfo(&sql);
    appendStringInfo(&sql,
        "DO $pg_dbms_job$\n"
        "DECLARE\n"
        "  job bigint := %lld;\n"
        "  next_date timestamp with time zone := current_timestamp;\n"
        "  broken boolean := false;\n"
        "BEGIN\n"
        "  %s\n"
        "END;\n"
        "$pg_dbms_job$",
        jobid, what);

    if (pgdj_debug)
        elog(LOG, "%s: job %lld executing: %s", PGDJ_APPNAME, jobid, what);

    PG_TRY();
    {
        ret = SPI_execute(sql.data, false, 0);
        if (ret < 0)
        {
            success = false;
            snprintf(pqstatus, sizeof(pqstatus), "The query failed");
            snprintf(errtext,  sizeof(errtext),
                     "SPI error %d: %s", ret, SPI_result_code_string(ret));
        }
    }
    PG_CATCH();
    {
        ErrorData *edata;
        success = false;
        snprintf(pqstatus, sizeof(pqstatus), "The query failed");
        edata = CopyErrorData();
        FlushErrorState();
        snprintf(errtext,  sizeof(errtext),
                 "%s", edata->message ? edata->message : "");
        snprintf(sqlstate, sizeof(sqlstate),
                 "%s", unpack_sql_state(edata->sqlerrcode));
        FreeErrorData(edata);
        elog(WARNING, "%s: job %lld failed: %s", PGDJ_APPNAME, jobid, errtext);
        SPI_finish();
        PopActiveSnapshot();
        AbortCurrentTransaction();
        goto post_exec;
    }
    PG_END_TRY();

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();

post_exec:
    gettimeofday(&tv1, NULL);
    duration = (double)(tv1.tv_sec  - tv0.tv_sec) +
                      (double)(tv1.tv_usec - tv0.tv_usec) / 1e6;

    /* ------------------------------------------------------------------
     * Step 3: update job metadata
     * ------------------------------------------------------------------
     */
    if (is_scheduled)
    {
        if (success)
            update_scheduled_success(jobid, duration);
        else
            update_scheduled_failure(jobid);
    }
    else
    {
        delete_job(jobid);
    }

    /* ------------------------------------------------------------------
     * Step 4: store run history
     * ------------------------------------------------------------------
     */
    store_run_details(log_user, jobid, start_ts, duration,
                      pqstatus, errtext, success, sqlstate);
}

/* =========================================================================
 * Metadata update helpers  –  each executes in its own transaction
 * =========================================================================
 */

/* Oracle 12c behaviour: on success reset failures, record last_date */
static void
update_scheduled_success(long long jobid, double duration_secs)
{
    StringInfoData sql;
    initStringInfo(&sql);
    appendStringInfo(&sql,
        "UPDATE dbms_job.all_scheduled_jobs SET"
        "  this_date  = NULL,"
        "  last_date  = current_timestamp,"
        "  total_time = '%f seconds'::interval,"
        "  failures   = 0,"
        "  instance   = instance + 1"
        " WHERE job = %lld",
        duration_secs, jobid);

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    if (SPI_execute(sql.data, false, 0) != SPI_OK_UPDATE)
        elog(WARNING, "%s: could not update metadata for job %lld after success",
             PGDJ_APPNAME, jobid);
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
}

/* Oracle 12c behaviour: on failure do NOT update last_date, increment failures */
static void
update_scheduled_failure(long long jobid)
{
    StringInfoData sql;
    initStringInfo(&sql);
    appendStringInfo(&sql,
        "UPDATE dbms_job.all_scheduled_jobs SET"
        "  this_date = NULL,"
        "  failures  = failures + 1"
        " WHERE job = %lld",
        jobid);

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    if (SPI_execute(sql.data, false, 0) != SPI_OK_UPDATE)
        elog(WARNING, "%s: could not update metadata for job %lld after failure",
             PGDJ_APPNAME, jobid);
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
}

/* Delete async job (or one-shot scheduled job) after execution */
static void
delete_job(long long jobid)
{
    StringInfoData sql;
    int            ret;

    initStringInfo(&sql);
    appendStringInfo(&sql,
        "DELETE FROM dbms_job.all_async_jobs WHERE job = %lld RETURNING job",
        jobid);

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());

    ret = SPI_execute(sql.data, false, 0);
    if (ret == SPI_OK_DELETE_RETURNING && SPI_processed == 0)
    {
        /* Nothing deleted from async table – must be a one-shot scheduled job */
        resetStringInfo(&sql);
        appendStringInfo(&sql,
            "DELETE FROM dbms_job.all_scheduled_jobs WHERE job = %lld",
            jobid);
        ret = SPI_execute(sql.data, false, 0);
        if (ret != SPI_OK_DELETE)
            elog(WARNING, "%s: could not delete one-shot job %lld",
                 PGDJ_APPNAME, jobid);
    }
    else if (ret != SPI_OK_DELETE_RETURNING)
    {
        elog(WARNING, "%s: could not delete async job %lld (SPI %d)",
             PGDJ_APPNAME, jobid, ret);
    }

    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
}

/* Insert a row into the job run history table */
static void
store_run_details(const char *owner, long long jobid,
                  const char *start_ts, double duration,
                  const char *status, const char *errmsg,
                  bool success, const char *sqlstate)
{
    StringInfoData sql;
    initStringInfo(&sql);

    appendStringInfo(&sql,
        "INSERT INTO dbms_job.all_scheduler_job_run_details"
        " (owner, job_name, status, error,"
        "  req_start_date, actual_start_date, run_duration,"
        "  slave_pid, additional_info)"
        " VALUES (%s, %lld, %s, %s, NULL, %s, '%f seconds'::interval, %d, %s)",
        (owner   && owner[0])   ? quote_literal_cstr(owner)    : "NULL",
        jobid,
        (status  && status[0])  ? quote_literal_cstr(status)   : "NULL",
        (sqlstate && sqlstate[0])? quote_literal_cstr(sqlstate) : "NULL",
        (start_ts && start_ts[0])? quote_literal_cstr(start_ts): "NULL",
        duration,
        (int) MyProcPid,
        (errmsg  && errmsg[0])  ? quote_literal_cstr(errmsg)   : "NULL");

    if (pgdj_debug)
        elog(LOG, "%s: store run details for job %lld: %s", PGDJ_APPNAME, jobid, sql.data);

    SetCurrentStatementStartTimestamp();
    StartTransactionCommand();
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    if (SPI_execute(sql.data, false, 0) != SPI_OK_INSERT)
        elog(WARNING, "%s: could not store run details for job %lld",
             PGDJ_APPNAME, jobid);
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
}

/* =========================================================================
 * pgdj_worker_main  –  per-job worker entry point
 *
 * The type and job id are read from MyBgworkerEntry->bgw_extra:
 *   's' = scheduled,  'a' = async
 *   format: "<s|a>:<jobid>"   e.g. "s:42"
 * =========================================================================
 */
void
pgdj_worker_main(Datum main_arg)
{
    char       extra[BGW_EXTRALEN];
    long long  jobid;
    bool       is_scheduled;

    /* Worker subprocesses ignore SIGHUP and complete their work on SIGTERM */
    pqsignal(SIGTERM, pgdj_sigterm);
    pqsignal(SIGHUP,  SIG_IGN);
    BackgroundWorkerUnblockSignals();

    /* Decode the job descriptor from bgw_extra */
    strncpy(extra, MyBgworkerEntry->bgw_extra, sizeof(extra) - 1);
    extra[sizeof(extra) - 1] = '\0';

    is_scheduled = (extra[0] == 's');
    jobid        = atoll(extra + 2);  /* skip "<s|a>:" */

    /* Connect to the database */
    BackgroundWorkerInitializeConnection(pgdj_database,
                                         pgdj_username
#if PG_VERSION_NUM >= 110000
                                         , 0
#endif
					 );

    pgstat_report_appname(MyBgworkerEntry->bgw_name);

    elog(LOG, "%s: worker started for %s job %lld",
         PGDJ_APPNAME,
         is_scheduled ? "scheduled" : "async",
         jobid);

    run_job(jobid, is_scheduled);

    elog(LOG, "%s: worker finished for job %lld", PGDJ_APPNAME, jobid);
    proc_exit(0);
}

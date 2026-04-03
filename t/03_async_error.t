use Test::Simple tests => 4;

# Submit an asynchronous job with a failure

# Verify that the process is running
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "1", "Daemon pg_dbms_job is running");

# Cleanup history and job tables
$ret = `psql -X -d regress_dbms_job -c "TRUNCATE dbms_job.all_scheduler_job_run_details, dbms_job.all_scheduled_jobs" > /dev/null 2>&1`;
ok( $? == 0, "truncate history and job tables");

# Create an asynchronous job
$ret = `psql -X -d regress_dbms_job -f test/sql/async_error.sql > /dev/null 2>&1`;
ok( $? == 0, "Submit job");
sleep(5);

# Look if the job have been registered in the history table
my $ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Found $ret async job in the history");


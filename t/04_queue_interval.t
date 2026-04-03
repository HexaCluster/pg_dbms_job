use Test::Simple tests => 5;

# Submit an asynchronous job and validate that queue_job_interval is respected

# Verify that the process is running
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "1", "Background worker pg_dbms_job is running");

# Cleanup history and job tables
$ret = `psql -X -d regress_dbms_job -c "TRUNCATE dbms_job.all_scheduler_job_run_details, dbms_job.all_scheduled_jobs" > /dev/null 2>&1`;
ok( $? == 0, "truncate history and job tables");

# Create an asynchronous job that must be executed later in 3 seconds
$ret = `psql -X -d regress_dbms_job -f test/sql/async_queue_interval.sql > /dev/null 2>&1`;
ok( $? == 0, "Submit job");
sleep(3);

# Look if the job have been registered in the history table, it should not
my $ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "0", "No delayed job found in the history: $ret");

# Wait to reach job_queue_interval
sleep(10);

# Now verify that the job have been run
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Found $ret delayed job in the history");


use Test::Simple tests => 6;

# Test asynchronous jobs

# Cleanup history and job tables
$ret = `psql -X -d regress_dbms_job -c "TRUNCATE dbms_job.all_scheduler_job_run_details, dbms_job.all_scheduled_jobs" > /dev/null 2>&1`;
ok( $? == 0, "truncate history and job tables");

# Create an asynchronous job
$ret = `psql -X -d regress_dbms_job -f test/sql/async.sql > /dev/null 2>&1`;
ok( $? == 0, "Submit job");
sleep(5);

# We should have the bgw and the child still running,
# the current running jobs must not be stopped
$ret = `ps auwx | grep pg_dbms_job | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "2", "Background worker pg_dbms_job and subprocess are running: $ret");

# Be sure that the job have been processed
sleep(6);

# Verify that the job have been removed from the queue
my $ret = `psql -X -d regress_dbms_job -Atc "SELECT count(*) FROM dbms_job.all_async_jobs;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "0", "Asynchronous job have been removed");

# Look if the job have been registered in the history table
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Found $ret async job in the history");

sleep(1);

# Now all process must be terminated
$ret = `ps auwx | grep pg_dbms_job | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "1", "Asynchronous job is stopped");

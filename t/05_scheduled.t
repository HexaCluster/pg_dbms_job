use Test::Simple tests => 11;

# Test scheduled job with an interval of 6 seconds

# Cleanup garbage from previous regression test runs
`rm -f /tmp/regress_dbms_job.*`;

# Verify that the process is running
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "1", "Background worker pg_dbms_job is running");

# Cleanup history and job tables
$ret = `psql -X -d regress_dbms_job -c "TRUNCATE dbms_job.all_scheduler_job_run_details, dbms_job.all_scheduled_jobs" > /dev/null 2>&1`;
ok( $? == 0, "truncate history and job tables");

# Create an scheduled job that must be executed later in 3 seconds and each 6 seconds after
$ret = `psql -X -d regress_dbms_job -f test/sql/scheduled.sql > /dev/null 2>&1`;
ok( $? == 0, "Submit job");
sleep(1);

# Look if the job have been registered in the history table, it should not
my $ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "0", "No async job found in the history: $ret");

# Wait to reach job_queue_interval
sleep(7);

# Now verify that the job have been run
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Found $ret async job in the history");

# Get id of the job
my $jobid = `psql -X -d regress_dbms_job -Atc "SELECT job FROM dbms_job.all_scheduled_jobs;"`;
chomp($jobid);
ok( $? == 0, "Get id of the job");

# Wait that at least 2 more job execution was done (12 seconds)
sleep(15);

# Now verify that we have 2 jobs that have been run
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "2", "Found $ret async job in the history");

sleep(6);

# Mark the job as broken to stop its execution, we should have a third trace in the history
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user; CALL dbms_job.broken($jobid, true);" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "CALL", "Call to broken procedure");

sleep(15);

# Now verify that we still have 3 jobs that have been run
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "3", "Found $ret async job in the history");

# Mark the job as not broken to restart its execution
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user; CALL dbms_job.broken($jobid, false);" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "CALL", "Call to broken procedure");

sleep(15);

# Now verify that we have 5 jobs that have been run
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduler_job_run_details;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "5", "Found $ret job in the history");


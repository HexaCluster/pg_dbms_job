use Test::Simple tests => 20;

# Test pg_dbms_job procedures

# Verify that the process is running
$ret = `ps auwx | grep pg_dbms_job: | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "1", "Background worker pg_dbms_job is running");

# Cleanup history and job tables
$ret = `psql -X -d regress_dbms_job -c "TRUNCATE dbms_job.all_scheduler_job_run_details, dbms_job.all_scheduled_jobs" > /dev/null 2>&1`;
ok( $? == 0, "truncate history and job tables");

# Submit a job that must be executed each days
$ret = `psql -X -d regress_dbms_job -f test/sql/submit.sql > /dev/null 2>&1`;
ok( $? == 0, "Submit job");
sleep(1);

# Get the id of the job that have been registered
my $job = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT job FROM dbms_job.all_scheduled_jobs;" | grep -v SET`;
chomp($job);
ok( $? == 0 && $job ne "" , "Job $job have been created");

# Remove the job
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.remove($job);" | grep -v SET`;
ok( $? == 0, "Removing job $job");
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduled_jobs;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "0", "Job $job have been removed");

# Submit the job again
$ret = `psql -X -d regress_dbms_job -f test/sql/submit.sql > /dev/null 2>&1`;
ok( $? == 0, "Submit job");
sleep(1);

# Get the id of the new job that have been registered
$job = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT job FROM dbms_job.all_scheduled_jobs;" | grep -v SET`;
chomp($job);
ok( $? == 0 && $job ne "", "New job $job have been created");

# Change the next execution date to NULL
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.next_date($job, NULL);" > /dev/null 2>&1 | grep -v SET`;
ok( $? != 0, "Can not set next_date to NULL for job $job");

# Change the next execution date to today + 1 year
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.next_date($job, date_trunc('day', current_timestamp) + '1 year'::interval);" | grep -v SET`;
ok( $? == 0, "Change next_date for job $job");

# Verify that the new next_date that have been registered
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT job FROM dbms_job.all_scheduled_jobs WHERE next_date = date_trunc('day', current_timestamp) + '1 year'::interval;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq $job, "New next_date for job $job have been modified");

# Change the interval to once a month
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.interval($job, 'date_trunc(''day'', current_timestamp) + ''1 month''::interval');" | grep -v SET`;
ok( $? == 0, "Change interval for job $job");

# Verify that the new interval that have been registered
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT job FROM dbms_job.all_scheduled_jobs WHERE md5(interval) = 'fb9412d079a32a090003d1c080619d72';" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq $job, "New interval for job $ret have been modified");

# Change the action to NULL, should be an error
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.what($job, NULL);" > /dev/null 2>&1 | grep -v SET`;
ok( $? != 0, "Change what to NULL for job $job");

# Change the action
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.what($job, 'BEGIN PERFORM version(); END;');" | grep -v SET`;
ok( $? == 0, "Change what for job $job");

# Verify that the new what value that have been registered
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT job FROM dbms_job.all_scheduled_jobs WHERE what = 'BEGIN PERFORM version(); END;';" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq $job, "New what for job $job have been modified");

# Change the modifiable columns to NULL, nothing must be changed
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.change($job, NULL, NULL, NULL);" | grep -v SET`;
ok( $? == 0, "Change all for job $job to NULL");

# Verify that nothing have changed
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduled_jobs WHERE md5(what) = '9e5f15c5784fb71b23e3d6419475d6de' AND md5(interval) = 'fb9412d079a32a090003d1c080619d72' AND next_date = date_trunc('day', current_timestamp) + '1 year'::interval;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Job $job is the same, nothing changed");

# Change the modifiable columns
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;CALL dbms_job.change($job, 'VACUUM ANALYZE;', date_trunc('day', current_timestamp) + '1 day'::interval, 'current_timestamp + ''1 day''::interval');" | grep -v SET`;
ok( $? == 0, "Change all for job $job");

# Verify that all values have changed
$ret = `psql -X -d regress_dbms_job -Atc "SET ROLE regress_dbms_job_user;SELECT count(*) FROM dbms_job.all_scheduled_jobs WHERE md5(what) = '4819535deca0cb7a637474c659d1b4e5' AND md5(interval) = 'b508a5fc92a976eac08a9c017b049f92' AND next_date = date_trunc('day', current_timestamp) + '1 day'::interval;" | grep -v SET`;
chomp($ret);
ok( $? == 0 && $ret eq "1", "Job $job have been executed and removed");


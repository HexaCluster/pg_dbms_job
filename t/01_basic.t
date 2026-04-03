use Test::Simple tests => 12;

# Test that the ademon can be started and stopped
# as well as default privileges on objects

# First drop the test database and users
`psql -c "DROP ROLE regress_dbms_job_user" 2>/dev/null`;
`psql -c "DROP ROLE regress_dbms_job_dba" 2>/dev/null`;

# Create the test scheduler dameon connection user, need to be superuser
my $ret = `psql -c "CREATE ROLE regress_dbms_job_dba LOGIN SUPERUSER PASSWORD 'regress_dbms_job_dba'"`;
ok( $? == 0, "Create regression test supuser: regress_dbms_job_dba");

# Create the test user
$ret = `psql -c "CREATE ROLE regress_dbms_job_user LOGIN PASSWORD 'regress_dbms_job_user'"`;
ok( $? == 0, "Create regression test user: regress_dbms_job_user");


# Create the test database
$ret = `psql -c "CREATE DATABASE regress_dbms_job OWNER regress_dbms_job_dba"`;
ok( $? == 0, "Create test regression database: regress_dbms_job");

# Create the schema and object of the pg_dbms_job extension
my $ver = `grep default_version pg_dbms_job.control | sed -E "s/.*'(.*)'/\\1/"`;
chomp($ver);
$ret = `psql -d regress_dbms_job -c "CREATE SCHEMA dbms_job;" > /dev/null 2>&1`;
ok( $? == 0, "Create dbms_job schema");

$ret = `psql -d regress_dbms_job -c "GRANT ALL ON SCHEMA PUBLIC TO regress_dbms_job_user;" > /dev/null 2>&1`;
ok( $? == 0, "Create dbms_job schema");

$ret = `psql -d regress_dbms_job -f sql/pg_dbms_job--$ver.sql > /dev/null 2>&1`;
ok( $? == 0, "Import manually pg_dbms_job extension file");

# Set privilege to allow user regress_dbms_job_user to work with the extension
$ret = `psql -d regress_dbms_job -c "GRANT USAGE ON SCHEMA dbms_job TO regress_dbms_job_user"`;
ok( $? == 0, "Add privileges to test user on pg_dbms_job schema");
$ret = `psql -d regress_dbms_job -c "GRANT ALL ON ALL TABLES IN SCHEMA dbms_job TO regress_dbms_job_user"`;
ok( $? == 0, "Add privileges to test user on pg_dbms_job tables");
$ret = `psql -d regress_dbms_job -c "GRANT ALL ON ALL SEQUENCES IN SCHEMA dbms_job TO regress_dbms_job_user"`;
ok( $? == 0, "Add privileges to test user on pg_dbms_job sequences");
$ret = `psql -d regress_dbms_job -c "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA dbms_job TO regress_dbms_job_user"`;
ok( $? == 0, "Add privileges to test user on pg_dbms_job functions");
$ret = `psql -d regress_dbms_job -c "GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA dbms_job TO regress_dbms_job_user"`;
ok( $? == 0, "Add privileges to test user on pg_dbms_job procedures");

sleep(5);

# Verify that the process is running
$ret = `ps auwx | grep pg_dbms_job | grep -v grep | wc -l`;
chomp($ret);
ok( $ret eq "1", "pg_dbms_job background worker is running");


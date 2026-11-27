use strict;
use warnings;

sub worker_count {
    my $sql = <<'SQL';
SELECT count(*)
FROM pg_stat_activity
WHERE CASE WHEN current_setting('server_version_num')::integer >= 110000
           THEN backend_type IN ('pg_dbms_job', 'pg_dbms_job:worker')
           ELSE application_name = 'pg_dbms_job:main'
                OR application_name LIKE 'pg_dbms_job:async:%'
                OR application_name LIKE 'pg_dbms_job:scheduled:%'
      END
SQL
    open my $psql, '-|', 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1',
        '-d', 'postgres', '-c', $sql or die "cannot start psql: $!";
    my $count = <$psql>;
    close $psql or die "unable to count pg_dbms_job workers";
    chomp $count;
    return $count;
}

1;

#!/usr/bin/perl -w

use strict;
use warnings;
use Nagios::Plugin;
use DBI;

my $np = Nagios::Plugin->new(
    usage => 'Usage: %s --master --slave',
);

$np->add_arg (
    spec => 'master|M=s',
    help => 'Specify master server DSN',
    required => 1,
    );
$np->add_arg (
    spec => 'master_user|U=s',
    help => 'Spacify master server user',
    );
$np->add_arg (
    spec => 'master_password|P=s',
    help => 'Spacify master server password',
    default => '',
    );
$np->add_arg (
    spec => 'slave|S=s',
    help => 'Specify slave server DSN',
    required => 1,
    );
$np->add_arg (
    spec => 'slave_user|u=s',
    help => 'Spacify slave server user',
    );
$np->add_arg (
    spec => 'slave_password|p=s',
    help => 'Spacify slave server password',
    default => '',
    );
$np->add_arg (
    spec => 'warning|W=s',
    help => 'Warning lag, in kb (default: 1024)',
    default => '1024',
    );
$np->add_arg (
    spec => 'critical|C=s',
    help => 'Critical lag, in kb (default: 4096)',
    default => '4096',
    );
$np->getopts();

sub verbose {
    my $message = join('', @_) || return 1;
    if ( $np->opts->verbose ) {
        $|++;
        print "$message";
        $|--;
    }    
}

sub xlog_to_bytes {
    my $data = shift || return 1;
    my ($id, $offset) = split(/\//, $data);
    return (hex('ffffffff') * hex($id)) + hex($offset);
}

my $master = DBI->connect (
    'dbi:Pg:'.$np->opts->master,
    $np->opts->master_user,
    $np->opts->master_password,
    );
if (!$master) {
    $np->nagios_exit( CRITICAL, 'Master: '.$DBI::errstr);
}

my $slave = DBI->connect (
    'dbi:Pg:'.$np->opts->slave,
    $np->opts->slave_user,
    $np->opts->slave_password,
    );
if (!$slave) {
    $np->nagios_exit( CRITICAL, 'Slave: '.$DBI::errstr);
}

# queries
my ($master_current, $slave_receive, $slave_replay);
my $master_query = 'SELECT pg_current_xlog_location()';
my $slave_receive_query = 'SELECT pg_last_xlog_receive_location()';
my $slave_replay_query = 'SELECT pg_last_xlog_replay_location()';

verbose "running '$slave_receive_query' on slave\n";
$slave_receive = $slave->selectall_arrayref( $slave_receive_query );

verbose "running '$slave_replay_query' on slave\n";
$slave_replay = $slave->selectall_arrayref( $slave_replay_query );

verbose "running '$master_query' on master\n";
$master_current = $master->selectall_arrayref( $master_query );

my ($master_xlog, $slave_xlog_receive, $slave_xlog_replay);
$master_xlog = $master_current->[0][0];
$slave_xlog_receive = $slave_receive->[0][0];
$slave_xlog_replay = $slave_replay->[0][0];

my ($master_bytes, $slave_bytes_receive, $slave_bytes_replay);
my ($diff_receive, $code_receive) = (0,0);
my ($diff_replay, $code_replay) = (0,0);

# process result from master server
if ( defined $master_xlog ) {
    # convert to bytes
    $master_bytes = xlog_to_bytes($master_xlog);
    verbose "master: $master_xlog, $master_bytes;\n";
} else {
    $np->nagios_exit( CRITICAL, 'Master server has no replication or wrong version;' );
}

# process receive status from slave server
if ( defined $slave_xlog_receive ) {
    # convert to bytes
    $slave_bytes_receive = xlog_to_bytes($slave_xlog_receive);
    verbose "slave receive: $slave_xlog_receive $slave_bytes_receive;\n";

    # and diff to kilobytes
    $diff_receive = int(($master_bytes - $slave_bytes_receive)/1024);
    verbose "diff receive: $diff_receive;\n";

    # check for receive lag
    $code_receive = $np->check_threshold (
        check => $diff_receive,
        warning => $np->opts->warning,
        critical => $np->opts->critical,
        );
    $np->add_message ( $code_receive, "Receive lag: ${diff_receive}kb;" );
} else {
    if ( defined $slave_xlog_replay ) {
        # print on all statuses
        for (OK, WARNING, CRITICAL) {
            $np->add_message( $_, 'Replay from WAL;' );
        }
    } else {
        $np->nagios_exit( CRITICAL, 'Slave server has no replication or wrong version;' );
    }
}

# process replay status from slave server
if ( defined $slave_xlog_replay ) {
    # convert to bytes
    $slave_bytes_replay = xlog_to_bytes($slave_xlog_replay);
    verbose "slave replay: $slave_xlog_replay $slave_bytes_replay;\n";

    # and diff to kilobytes
    $diff_replay = int(($master_bytes - $slave_bytes_replay)/1024);
    verbose "diff replay: $diff_replay;\n";

    # check for replay lag
    $code_replay = $np->check_threshold (
        check => $diff_replay,
        warning => $np->opts->warning,
        critical => $np->opts->critical,
        );
    $np->add_message ( $code_replay, "Replay lag: ${diff_replay}kb;" );
} else {
    for (OK, WARNING, CRITICAL) {
        $np->add_message ( $_, 'Wrong replay and receive statuses;' );
    }
}

my ( $code, $message ) = $np->check_messages();
$np->nagios_exit ( $code, $message );

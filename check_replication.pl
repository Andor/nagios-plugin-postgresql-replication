#!/usr/bin/perl -w

use strict;
use warnings;
use Nagios::Plugin;
use DBI;
use Data::Dumper;

my $np = Nagios::Plugin->new(
    usage => 'Usage: %s --master --slave',
);

$np->add_arg (
    spec => 'master|M=s',
    help => 'Specify master server DSN',
    required => 1,
    );
$np->add_arg (
    spec => 'slave|S=s',
    help => 'Specify slave server DSN',
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
    help => 'Specify HTTP username',
    default => '1024',
    );
$np->add_arg (
    spec => 'critical|C=s',
    help => 'Specify HTTP username',
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
    { RaiseError => 1 }
    );
my $slave = DBI->connect (
    'dbi:Pg:'.$np->opts->slave,
    $np->opts->slave_user,
    $np->opts->slave_password,
    { RaiseError => 1 }
    );

# queries
my ($master_current, $slave_receive, $slave_replay);
my $master_query = 'SELECT pg_current_xlog_location()';
my $slave_receive_query = 'SELECT pg_last_xlog_receive_location()';
my $slave_replay_query = 'SELECT pg_last_xlog_replay_location()';

verbose "running '$master_query' on master\n";
$master_current = $master->selectall_arrayref( $master_query );

verbose "running '$slave_receive_query' on slave\n";
$slave_receive = $slave->selectall_arrayref( $slave_receive_query );

verbose "running '$slave_replay_query' on slave\n";
$slave_replay = $slave->selectall_arrayref( $slave_replay_query );

my ($master_xlog, $slave_xlog_receive, $slave_xlog_replay);
$master_xlog = $master_current->[0][0];
$slave_xlog_receive = $slave_receive->[0][0];
$slave_xlog_replay = $slave_replay->[0][0];

if ( !defined $master_xlog ) {
    $np->nagios_exit( CRITICAL, 'Master server has no replication or wrong version;' );
}
if ( !defined $slave_xlog_receive || !defined $slave_xlog_replay ) {
    $np->nagios_exit( CRITICAL, 'Slave server has no replication or wrong version;' );
}

# convert values to bytes
my ($master_bytes, $slave_bytes_receive, $slave_bytes_replay);
$master_bytes = xlog_to_bytes($master_xlog);
$slave_bytes_receive = xlog_to_bytes($slave_xlog_receive);
$slave_bytes_replay = xlog_to_bytes($slave_xlog_replay);

verbose "master: $master_xlog, $master_bytes;\n";
verbose "slave receive: $slave_xlog_receive $slave_bytes_receive;\n";
verbose "slave replay: $slave_xlog_replay $slave_bytes_replay;\n";

my $diff_receive = int($master_bytes - $slave_bytes_receive);
my $diff_replay = int($master_bytes - $slave_bytes_replay);

verbose "diff receive: $diff_receive; diff replay: $diff_replay;\n";

my $code = $np->check_threshold (
    check => $diff_receive,
    warning => $np->opts->warning,
    critical => $np->opts->critical,
    );

$np->nagios_exit( $code, "Receive lag: $diff_receive, replay lag: $diff_replay" );

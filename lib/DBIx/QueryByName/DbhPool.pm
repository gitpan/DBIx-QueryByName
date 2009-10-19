package DBIx::QueryByName::DbhPool;
use utf8;
use strict;
use warnings;
use Data::Dumper;
use DBI;
use DBIx::QueryByName::Logger qw(get_logger);

sub new {
    return bless( { connections => {}, config => {} }, $_[0] );
}

sub add_credentials {
    my ($self, $session, @params) = @_;
	my $log = get_logger();
    $log->logcroak("undefined session name") if (!defined $session);
    $log->logcroak("no session parameters provided") if (scalar @params == 0);
    $log->logcroak("credentials for session [$session] are already declared") if ($self->knows_session($session));
    $self->{config}->{$session} = \@params;
    return $self;
}

sub knows_session {
    my ($self, $session) = @_;
	my $log = get_logger();
    $log->logcroak("undefined session name") if (!defined $session);
    return (exists $self->{config}->{$session}) ? 1 : 0;
}

# open database connection for the given session and return a database
# handler
sub connect {
    my ($self, $session) = @_;
	my $log = get_logger();
    $log->logcroak("undefined session name") if (!defined $session);

    return $self->{connections}->{$$}->{$session} if (defined $self->{connections}->{$$}->{$session});

    my $error_reported = 0;
    while (1) {
        $log->logcroak("don't know how to open connection [$session]")
            if (!$self->knows_session($session));

        my $dbh = DBI->connect( @{$self->{config}->{$session}} );

        if (!defined $dbh) {
            # TODO: croak after a number of attempts?
            $log->error("Unable to connect to database [$session]: ".$DBI::errstr) if ($error_reported == 0);
            $error_reported = 1;
            sleep(1);
            next;
        }

        $self->{connections}->{$$}->{$session} = $dbh;
        $log->info( "Database is back online [$session]") if ($error_reported == 1);
        return $dbh;
    }
}

sub disconnect {
    my ($self, $session) = @_;
	my $log = get_logger();
    $log->logcroak("undefined session name")   if (!defined $session);
    $log->logcroak("not a known session name") if (!$self->knows_session($session));

    if (defined $self->{connections}->{$$}->{$session}) {
        $self->{connections}->{$$}->{$session}->disconnect();
        undef $self->{connections}->{$$}->{$session};
    }
    return $self;
}

sub disconnect_all {
    my $self = shift;
    my $log = get_logger();

    foreach my $pid ( keys %{$self->{connections}} ) {
        foreach my $session ( keys %{$self->{connections}->{$pid}} ) {
            if ( $$ == $pid ) {
                $self->disconnect($session);
            } elsif (defined $self->{connections}->{$pid}->{$session}) {
                # the connection belongs to an other process than self.
                # Prevent forked child (this pid) from disconnecting the database connection
                my $dbh = $self->{connections}->{$pid}->{$session}->{InactiveDestroy} = 1;
                undef $self->{connections}->{$pid}->{$session};
            }
        }
    }
}

1;

__END__

=head1 NAME

DBIx::QueryByName::DbhPool - A pool of database handles

=head1 DESCRIPTION

An instance of DBIx::QueryByName::DbhPool stores the all opened
database handles used by the corresponding instances of
DBIx::QueryByName, as well as information on how to open database
connections.

DO NOT USE DIRECTLY!

=head1 INTERFACE

This API is subject to change!

=over 4

=item C<< my $pool = DBIx::QueryByName::DbhPool->new(); >>

Instanciate DBIx::QueryByName::DbhPool.

=item C<< $pool->add_credentials($session, @params); >>

Store credentials for opening the database connection named
C<$session>. C<@params> is a standard DBI connection string or list.
Return the pool.

=item C<< $pool->knows_session($session); >>

Return true if the pool knows connection credentials for a database
connection named C<$session>. False otherwise.

=item C<< my $dbh = $pool->connect($session); >>

Tries to open the database connection associated with the session name
C<$session>. Will retry every second indefinitely until success.
Return the database handle for the new connection.

=item C<< my $dbh = $pool->disconnect($session); >>

Disconnects the database connection associated with the session name
C<$session>. Return the pool.

=item C<< my $dbh = $pool->disconnect_all(); >>

Disconnects all the database connections in the pool that belong to the running process.
Doesn't affect any parent/child process's connections.

=back

=cut


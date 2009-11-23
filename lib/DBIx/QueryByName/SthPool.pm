package DBIx::QueryByName::SthPool;
use utf8;
use strict;
use warnings;
use Data::Dumper;
use DBI;
use DBIx::QueryByName::Logger qw(get_logger debug);

sub _add_sth {
    my ($self,$query,$sth) = @_;
    die "undefined query or statement handler" if (!defined $query || !defined $sth);
    $self->{sths}->{$$}->{$query} = $sth;
}

sub _get_sth {
    my ($self,$query) = @_;
    die "undefined query" if (!defined $query);
    return $self->{sths}->{$$}->{$query};
}

sub new {
    return bless( { sths => {} }, $_[0] );
}

sub parent {
    my ($self, $parent) = @_;
    $self->{dbhpool} = $parent->_dbh_pool;
    $self->{querypool} = $parent->_query_pool;
}

sub finish_all_sths {
    my $self = shift;
    debug "Closing all sths for pid $$";
    foreach my $query ( keys %{$self->{sths}->{$$}} ) {
        if (defined $self->{sths}->{$$}->{$query}) {
            $self->{sths}->{$$}->{$query}->finish;
        }
    }
    undef($self->{sths}->{$$});
}

sub _prepare {
    my ($self,$query) = @_;
    my $log = get_logger();

    my ($session,$sql) = $self->{querypool}->get_query($query);

    if (defined $self->_get_sth($query)) {
        debug "Query is already prepared. Using cached value.";
        return $self->_get_sth($query);
    }

    my $dbh = $self->{dbhpool}->connect($session);

    debug "Preparing query $query";
    my $sth = $dbh->prepare($sql);

    # TODO: add more verbose error description?
    # TODO: retry in some smart way?
    if (!defined $sth) {
        $log->logcroak("failed to prepare query [$query]");
    }

    $self->_add_sth($query,$sth);
    return $sth;
}

sub prepare_and_execute {
    # TODO: support multiple args
    my ($self,$query,@args) = @_;
    my $log = get_logger();
    my ($session,undef) = $self->{querypool}->get_query($query);

    my $sth = $self->_prepare($query);

    my $rv;
    my $error_reported = 0;
    while (1) {

        # Normally, if traffic between the client and the database
        # server is interupted (cable cut, whatever), the client will
        # timeout after 1min (observed on osx). But it has been
        # observed on some setups (client on linux, server blocked by
        # drop rule in firewall) that the client hang forever in
        # execute(). The following code is a workaround:
        #
        #         my $did_timeout = 0;
        #         eval {
        #             local $SIG{ALRM} = sub { $did_timeout = 1; die 'TIMEOUT' };
        #             alarm($self->{execute_timeout});
        #             # call execute
        #             alarm(0);
        #         };
        #         alarm(0);
        #
        #         if ($did_timeout) {
        #         } elsif ($@) {}
        #
        # But this code works only if perl is running with 'unsafe'
        # signal handling (env PERL_SIGNALS=unsafe). And we don't want
        # to compromise DBD::Pg by interupting impromptuously.

        # WARNING: execute* might hang forever
        if (scalar @args && ref $args[0] eq 'ARRAY') {
            debug "Calling execute_array for query $query";
            $rv = $sth->execute_array({},@args);
        } else {
            debug "Calling execute for query $query";
            $rv = $sth->execute(@args);
        }

        if (!defined $rv) {
            my $err = $sth->err || 99999999999999;
            my $errstr = $sth->errstr || '';

            debug "An error occured while executing query [$query] [$err] [$errstr]";

            # if connection error while executing, retry
            # TODO: support error messages per database type
            # NOTE: if execute times-out properly, it raises an error with code 7 and text 'could not receive data from server: Operation timed out'
            if ( $err == 7 && $errstr =~ m/could not connect to server|no connection to the server|terminating connection due to administrator command/ ) {

                $log->error("Query $query failed, will try again, Error code [$err], Error message [$errstr]" )
                    if ($error_reported == 0);

                # try to reconnect to database
                my $dbh = $self->{dbhpool}->connect($session);
                unless ($dbh->ping()) {
                    debug "Can ping database. Trying to disconnect, re-connect and re-prepare";
                    $self->{dbhpool}->disconnect($session);
                    $self->finish_all_sths();  # TODO: do we really want to finish ALL queries or only those in this session?
                    # TODO: shouldn't we finish first, disconnect then?
                    $sth = $self->_prepare($query);

                } else {
                    debug "Cannot ping database. Waiting 1sec and retrying to execute.";
                }

            } else {
                $log->logcroak("Query $query failed, won't try again, Error code [$err], Error message [$errstr]" );
                # TODO: use sth's pg_cmd_status to warn extra if it was a delete/update/insert query
                return undef; # Will never reach this line, logcroak will die, but just in case
            }

            $error_reported = 1;
            sleep(1);
            next;
        }

        if ($error_reported == 1) {
            $log->info("Successfully retried executing query $query");
        }
        last;
    }

    return $sth;
}

sub DESTROY {
    my $self = shift;
    debug "DESTROY SthPool -> calling finish_all_sths and disconnect_all";
    # either this DESTROY is called first, or DbhPool's DESTROY is
    $self->finish_all_sths();
    $self->{dbhpool}->disconnect_all() if (defined $self->{dbhpool});
}

1;

__END__

=head1 NAME

DBIx::QueryByName::SthPool - A pool of statement handles

=head1 DESCRIPTION

An instance of DBIx::QueryByName::SthPool stores all the statement
handles obtained by calling queries by name. It contains the logic for
retrying failed queries.

DO NOT USE DIRECTLY!

=head1 INTERFACE

This API is subject to change!

=over 4

=item C<< my $pool = DBIx::QueryByName::SthPool->new(); >>

Instanciate DBIx::QueryByName::SthPool.

=item C<< $pool->parent($dbixquerybyname) >>

Called after new() to tell the sth pool of which instance of
DBIx::QueryByName it is related to.

=item C<< $pool->finish_all_sths(); >>

Call finish on all the statement handles opened by the current process
via calls to named queries.

=item C<< $pool->prepare($queryname) >>

Prepare the sql query associated to the query with name C<$queryname>.

=item C<< $pool->prepare_and_execute($queryname,$value1,$value2,$value3...) >>

or

=item C<< $pool->prepare_and_execute($queryname,\@values1,\@values2,\@values3...) >>

Call execute respectively execute_array for the statement handle associated
with C<$queryname>


=back

=cut


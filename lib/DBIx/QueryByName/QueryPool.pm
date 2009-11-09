package DBIx::QueryByName::QueryPool;
use utf8;
use strict;
use warnings;
use DBIx::QueryByName::Logger qw(get_logger);

sub new {
    return bless( {}, $_[0] );
}

sub add_query {
    my ($self, %args) = @_;
    my $log     = get_logger();
    my $name    = $args{name}    || $log->logcroak("BUG: undefined query name");
    my $sql     = $args{sql}     || $log->logcroak("BUG: undefined query sql");
    my $session = $args{session} || $log->logcroak("BUG: undefined query session");
    my $params  = $args{params}  || $log->logcroak("BUG: undefined query parameters");

    $log->logcroak("invalid query name: contain non alfanumeric characters ($name)")
        if ($name !~ /^[a-zA-Z0-9_]+$/);

    $log->logcroak("invalid query parameters: expecting an array reference: ".Dumper($params))
        if (ref $params ne 'ARRAY');

    foreach my $p (@{$params}) {
        $log->logcroak("invalid query parameter: contain undefined parameter: ".Dumper($params))
            if (!defined $p);

        $log->logcroak("invalid query parameter: contain non alfanumeric characters [$p]")
            if ($p !~ /^[a-zA-Z0-9\,]+$/);
    }

    # TODO: validate the query's sql code
    # TODO: validate session
    #    my $session = $args{session} || $log->logcroak("BUG: undefined query session");

    $self->{$name} = {
        sql     => $sql,
        session => $session,
        params  => $params,
    };

    return $self;
}

sub knows_query {
    my ($self, $name) = @_;
    get_logger()->logcroak("BUG: undefined query name") if (!defined $name);
    return (exists $self->{$name}) ? 1 : 0;
}

sub get_query {
    my ($self, $name) = @_;
    get_logger()->logcroak("BUG: undefined query name") if (!defined $name);
    get_logger()->logcroak("BUG: undefined query name") if (!$self->knows_query($name));
    return ($self->{$name}->{session}, $self->{$name}->{sql}, @{$self->{$name}->{params}});
}

1;

__END__

=head1 NAME

DBIx::QueryByName::QueryPool - Manages a pool of sql query descriptions

=head1 DESCRIPTION

An instance of DBIx::QueryByName::QueryPool stores the descriptions of
all the queries that can be executed with corresponding instances of
DBIx::QueryByName.

DO NOT USE DIRECTLY!

=head1 INTERFACE

This API is subject to change!

=over 4

=item C<< my $pool = DBIx::QueryByName::QueryPool->new(); >>

Instanciate DBIx::QueryByName::QueryPool.

=item C<< $pool->add_query(name => $name, sql => $sql, session => $session, params => \@params); >>

Add a query to this pool.
Example:

    $pool->add_query(name => 'get_user_adress',
                     sql => 'SELECT adress FROM adresses WHERE firstname=? AND lastname=?',
                     params => [ 'firstname', 'lastname' ],
                     session => 'name_of_db_connection',
                    );

=item C<< $pool->knows_query($name); >>

True if the pool already contains a query with that name. False otherwise.

=item C<< my ($session,$sql,@params) = $pool->get_query($name); >>

Return the name of the database session, the sql code and the named parameters
of the query named C<$name>. Croak if no query with that name.

=back

=cut


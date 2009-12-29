package DBIx::QueryByName::Result::ScalarIterator;
use utf8;
use strict;
use warnings;
use Data::Dumper;
use DBIx::QueryByName::Logger qw(get_logger);
use base qw( DBIx::QueryByName::Result::Iterator );

sub next {
    my $self = shift;

    return undef
        if (!defined $self->{sth});

    if (my @values = $self->{sth}->fetchrow_array()) {

        get_logger->logcroak("query ".$self->{query}." returns more than 1 column per row. Got: ".Dumper(\@values))
            if (scalar @values > 1);

        return $values[0];
    }

    # no more rows to fetch.
    # TODO: handle specially if it was an error?
    $self->{sth}->finish();
    $self->{sth} = undef;
    return undef;
}

1;

__END__

=head1 NAME

DBIx::QueryByName::Result::ScalarIterator - A scalar iterator around a statement handle

=head1 DESCRIPTION

Provides an iterator-like api to a DBI statement handle that is expected
to return only a single column upon each call to fetchrow_array().

DO NOT USE DIRECTLY!

=head1 INTERFACE

=over 4

=item C<< my $i = new($sth); >>

Return a scalar iterator wrapped around this statement handle.

=item C<< my $v = $i->next(); >>

C<$v> is the value of the single column in the entry returned by the
next call to fetch_row() on the iterator's statement handle. Return
undef if no entries could be fetched.

=back

=cut


package DBIx::QueryByName::Result::HashIterator;
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

    if (my $hash = $self->{sth}->fetchrow_hashref()) {
        return $hash;
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

DBIx::QueryByName::Result::HashIterator - A hash iterator around a statement handle

=head1 DESCRIPTION

Provides an iterator-like api to a DBI statement handle that is expected
to return one or more columns upon each call to fetchrow_array().

DO NOT USE DIRECTLY!

=head1 INTERFACE

=over 4

=item C<< my $i = new($query,$sth); >>

Return a hash iterator wrapped around this statement handle.

=item C<< my $result = $i->next(); >>

C<%result> is the hashref returned by fetchrow_hash() called upon this
iterator's statement handle. Return undef if no more rows to fetch.

=back

=cut


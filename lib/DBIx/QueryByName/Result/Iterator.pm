package DBIx::QueryByName::Result::Iterator;
use utf8;
use strict;
use warnings;
use Data::Dumper;
use DBIx::QueryByName::Logger qw(get_logger);

sub new {
    my ($class,$query,$sth) = @_;
    get_logger->logcroak("undefined query name")
        if (!defined $query);
    get_logger->logcroak("undefined sth or not a sth")
        if (!defined $sth || ref $sth ne 'DBI::st');
    return bless( { query => $query, sth => $sth }, $class);
}

sub next { die "BUG: not implemented" }

1;

__END__

=head1 NAME

DBIx::QueryByName::Result::Iterator - Parent class to all iteratorsA scalar iterator around a statement handle

=head1 DESCRIPTION

Provides an iterator-like api to a DBI statement handle. DO NOT USE DIRECTLY!

=head1 INTERFACE

=over 4

=item C<< new($query,$sth) >>

=item C<< $i->next() >>

=back

=cut


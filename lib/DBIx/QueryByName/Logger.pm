package DBIx::QueryByName::Logger;
use utf8;
use strict;
use warnings;
use Carp qw(croak);
use base qw(Exporter);

our @EXPORT_OK = qw( get_logger );

my $SELF = bless({},__PACKAGE__);

# check whether Log::Log4perl is available...
my $LOG4PERLEXISTS = 1;
eval 'use Log::Log4perl';
$LOG4PERLEXISTS = 0 if (defined $@ && $@ ne '');

sub get_logger {
    return ($LOG4PERLEXISTS) ? Log::Log4perl::get_logger() : $SELF;
}

# default logger methods
sub logcroak {
    my $msg = shift || '';
    croak $msg;
}

1;

__END__

=head1 NAME

DBIx::QueryByName::Logger - Take care of all logging

=head1 SYNOPSIS

    use DBIx::QueryByName::Logger qw(get_logger);
    my $log = get_logger();

    $log->logcroak('something went bad');

=head1 INTERFACE

=over 4

=item C<< $log = get_logger(); >>

If Log::Log4perl is available, return its logger. Otherwise return an
instance of self that offers a default implementation of the following
Log4perl methods:

=item C<< $log->logcroak($msg); >>

Log C<$msg> and croak.

=back

=cut


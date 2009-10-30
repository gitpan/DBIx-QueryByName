package DBIx::QueryByName::Logger;
use utf8;
use strict;
use warnings;
use Carp qw(croak);
use base qw(Exporter);

our @EXPORT_OK = qw( get_logger );
our $AUTOLOAD;

my $SELF = bless({},__PACKAGE__);

# check whether Log::Log4perl is available...
my $LOG4PERLEXISTS = 1;
eval 'use Log::Log4perl';
$LOG4PERLEXISTS = 0 if (defined $@ && $@ ne '');

sub get_logger {
    return ($LOG4PERLEXISTS) ? Log::Log4perl::get_logger() : $SELF;
}

# default logger methods
sub logcroak { my $msg = $_[1] || ''; croak "$msg\n" }
sub error    { my $msg = $_[1] || ''; print STDERR "ERROR: $msg\n" }
sub warn     { my $msg = $_[1] || ''; print STDERR "WARN: $msg\n" }
sub info     { my $msg = $_[1] || ''; print STDERR "INFO: $msg\n" }
sub log      { my $msg = $_[1] || ''; print STDERR "$msg\n" }

# and all the others
sub AUTOLOAD {
    my $msg = $_[1] || '';
    my $method = $AUTOLOAD;
    return if ($method =~ /::DESTROY$/);
    print STDERR "unhandled call to [$method]: $msg\n";
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

=item C<< $log->logcroak($msg); >> Log C<$msg> and croak.

=item C<< $log->error($msg); >> Log C<$msg> as an error.

=item C<< $log->warn($msg); >> Log C<$msg> as a warning.

=item C<< $log->info($msg); >> Log C<$msg>.

=item C<< $log->log($msg); >> Log C<$msg>.

=back

=cut


package DBIx::QueryByName::FromXML;
use utf8;
use strict;
use warnings;
use XML::Parser;
use XML::SimpleObject;
use Data::Dumper;
use DBIx::QueryByName::Logger qw(get_logger);

my $PARSER = XML::Parser->new( ErrorContext => 2, Style => 'Tree' );

sub load {
    my $qpool   = shift;
    my $session = shift;
    my $xml     = shift;
    my $log     = get_logger();

    $log->logcroak("undefined query pool")  if (!defined $qpool);
    $log->logcroak("undefined session")     if (!defined $session);
    $log->logcroak("undefined xml string")    if (!defined $xml);

    my @queries;
    eval {
        @queries = XML::SimpleObject->new( $PARSER->parse($xml) )->child('queries')->children('query');
    };

    $log->logcroak("failed to parse xml: $@")
        if (defined $@ && $@ ne '');

    $log->logcroak("invalid xml: no <query> nodes (session_name => $session)")
        if (scalar @queries == 1 and ref $queries[0] ne 'XML::SimpleObject');

    foreach my $query ( @queries ) {

        my $name = $query->attribute('name');
        $log->logcroak("invalid xml: no name attribute in query node (session_name => $session)")
            if (!defined $name);

        my $params = $query->attribute('params');
        $log->logcroak("invalid xml: no params attribute in query node (query_name => $name, session_name => $session)")
            if (!defined $params);

        my @params = map { $_ =~ s/^\s*//; $_ =~ s/\s*$//; $_ } split(q{,}, $params);
#        my @params = split(q{,}, $params);

        if ( $qpool->knows_query($name) ) {
            # query is already imported, possibly from other XML file
            $log->logcroak("query already imported (query_name => $name, session_name => $session)");
        }

        # TODO: we might want to perform minimum sanity check on query string
        $qpool->add_query(
            name    => $name,
            sql     => $query->value,
            session => $session,
            params  => \@params,
            );
    }
}

1;

__END__

=head1 NAME

DBIx::QueryByName::FromXML - Import named queries from an xml text

=head1 DESCRIPTION

DBIx::QueryByName::FromXML is a backend for the load() method from
DBIx::QueryByName and is in charge of loading named queries from an
xml file/string that describes each query.

DO NOT USE DIRECTLY!

=head1 XML SYNTAX

See DBIx::QueryByName.

=head1 INTERFACE

=over 4

=item C<< load($querypool,$sessionname,$xmlstring); >>

Fill this C<$querypool> with all the queries described in C<$xmlstring>.
Those queries will only run over database connections opened with the credential
associated with C<$sessionname>.

=back

=cut


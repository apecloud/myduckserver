use strict;
use warnings;
use DBI;

package Tests;

sub new {
    my $class = shift;
    my $self = {
        conn => undef,
        tests => [],
    };
    bless $self, $class;
    return $self;
}

sub connect {
    my ($self, $ip, $port, $user, $password) = @_;
    my $dsn = "dbi:Pg:dbname=main;host=$ip;port=$port";
    $self->{conn} = DBI->connect($dsn, $user, $password, { RaiseError => 1, AutoCommit => 1 });
}

sub disconnect {
    my $self = shift;
    $self->{conn}->disconnect();
}

sub add_test {
    my ($self, $query, $expected_results) = @_;
    push @{$self->{tests}}, { query => $query, expected_results => $expected_results };
}

sub run_tests {
    my $self = shift;
    foreach my $test (@{$self->{tests}}) {
        return 0 unless $self->run_test($test);
    }
    return 1;
}

sub trim {
    my $s = shift;
    $s =~ s/^\s+|\s+$//g;
    return $s;
}

sub read_tests_from_file {
    my ($self, $filename) = @_;
    open my $fh, '<', $filename or die "Could not open file '$filename': $!";
    my $query;
    my @results;
    while (my $line = <$fh>) {
        $line = trim($line);
        if ($line eq '') {
            if ($query) {
                $self->add_test($query, [@results]);
                $query = undef;
                @results = ();
            }
        } elsif (!$query) {
            $query = $line;
        } else {
            push @results, [split /,/, $line];
        }
    }
    $self->add_test($query, [@results]) if $query;
    close $fh;
}

sub run_test {
    my ($self, $test) = @_;
    my $query = $test->{query};
    my $expected_results = $test->{expected_results};
    print "Running test: $query\n";

    my $st = $self->{conn}->prepare($query);
    $st->execute();
    my $rows = $st->rows;
    if ($rows == 0 || $rows == -1) {
        print "No rows returned\n";
        return defined $expected_results && @$expected_results == 0;
    }
    my $cols = $st->{NUM_OF_FIELDS};
    if (!defined $cols) {
        print "No columns returned\n";
        return defined $expected_results && @$expected_results == 0;
    }
    if ($cols != @{$expected_results->[0]}) {
        print "Expected " . @{$expected_results->[0]} . " columns\n";
        return 0;
    }
    my $row_num = 0;
    while (my @row = $st->fetchrow_array) {
        for my $col_num (0..$#row) {
            if ($row[$col_num] ne $expected_results->[$row_num][$col_num]) {
                print "Expected:\n'$expected_results->[$row_num][$col_num]'\n";
                print "Result:\n'$row[$col_num]'\nRest of the results:\n";
                while (my @rest = $st->fetchrow_array) {
                    print join(',', @rest) . "\n";
                }
                return 0;
            }
        }
        $row_num++;
    }
    print "Returns $row_num rows\n";
    if ($row_num != @$expected_results) {
        print "Expected " . @$expected_results . " rows\n";
        return 0;
    }
    return 1;
}

package main;

if (@ARGV < 5) {
    die "Usage: perl PGTest.pl <ip> <port> <user> <password> <testFile>\n";
}

my $tests = Tests->new();
$tests->connect($ARGV[0], $ARGV[1], $ARGV[2], $ARGV[3]);
$tests->read_tests_from_file($ARGV[4]);

if (!$tests->run_tests()) {
    $tests->disconnect();
    exit 1;
}
$tests->disconnect();
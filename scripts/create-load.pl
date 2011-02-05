#!/usr/bin/perl -w

# Create a load generarator specification.

# Usage:
#    create-load.pl start_id num_parts total_load load_jitter rate rate_jitter
#
# Produces "num_parts" lines of text with the following columns
#    PART_ID    RATE    LENGTH
#
# PART_ID are sequential starting at start_id
# RATE is a number in the range [rate - rate_jitter, rate + rate_jitter]
# RATE*LENGTH summed over all columns adds up to total_load
# RATE*LENGTH vary from the total_load/num_parts by a relative amount of load_jitter
#

use strict;
our $, = "\t";
our $\ = "\n";

my ($start_id, $parts, $load_total, $load_jitter, $rate, $rate_jitter) = @ARGV;

my @RATE;
my @LOAD;

my $L = 0;

## Generate Random LOADs and RATEs
for (1..$parts) {
    my $dl = (rand(2*$load_jitter)-$load_jitter);
    my $dr = (rand(2*$rate_jitter)-$rate_jitter);

    push @LOAD, (1+$dl);
    push @RATE, $rate*(1+$dr);

    $L += (1+$dl);
}

# now we have to scale @LOAD to add up to $load_total
my $s = $load_total/$L;
@LOAD = map {$_*$s} @LOAD;


for (my $i = 0; $i < $parts; ++$i) {
    my $length = $LOAD[$i]/$RATE[$i];

    print ($i+$start_id, $RATE[$i], $length);
}

printf STDERR <<_END;
parts:          $parts
load_total:     $load_total
load_jitter:    $load_jitter
rate:           $rate
rate_jitter:    $rate_jitter
_END

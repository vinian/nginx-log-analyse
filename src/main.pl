#!/usr/bin/perl

use strict;
use warnings;

use Smart::Comments;
use FindBin;
use lib $FindBin::Bin;
use Worker;
use POSIX qw( strftime );

my $w = Worker->new();

$w->open_file;

while ( my $line = $w->read_line ) {
    chomp $line;
    if ($line =~ /$w->{log_format}/) {
        my ($ip, $date, $time, $r, $status, $send, $cook) = ($1, $2, $3, $4, $5, $6, $7);
        $r = (split(/\s+/, $r))[1];
        my ($protocol, $rest) = split(/\?/, $r);
        ($protocol) = $protocol =~ /(\d+)/;
        next if $protocol < 51 or $protocol > 58;
        my %h;
        foreach ( split(/\&/, $rest) ) {
            my ($k, $v) = split /=/;
            $h{$k} = $v;
        }

        $h{client_ip} = $ip;
        $h{time} =~ s/(\d\d\d)$/".$1"/e;
        $h{time} = strftime("%Y:%m:%d %H:%M:%S", localtime( $h{time} ));
        ### %h
        $w->insert_log(1000, \%h);
        $w->insert_log($protocol, \%h);
    }
}

$w->close_file;


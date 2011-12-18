
use strict;
use warnings;

use Test::More;
use File::Temp qw/tempfile/;

use Text::CSV;
my $parser = Text::CSV->new();

use_ok( 'Tie::Array::CSV' );

my $test_data = <<END_DATA;
name,rank,serial number
joel berger,plebe,1010101
larry wall,general,1
damian conway,colonel,1001
END_DATA

{ 
  my ($fh, $file) = tempfile();
  print $fh $test_data;

  my @csv;
  ok( tie(@csv, 'Tie::Array::CSV', $fh), "Tied CSV" );

  {

    my $row_1a = $csv[1];
    my $row_1b = $csv[1];
  
    is( $row_1a . "", $row_1b . "", "repeated requests for same row return same object" );

  }

  # DANGER: non-api test
  ok( ! defined tied(@csv)->{active_rows}{1}, "on destruction of row object, active_row entry is undef");
}

done_testing();

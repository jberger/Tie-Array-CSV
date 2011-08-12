package Tie::Array::CSV;

use strict;
use warnings;

use Carp;

use Tie::File;
use Text::CSV;

use Tie::Array;
our @ISA = ('Tie::Array');

sub TIEARRAY {
  my $class = shift;
  my ($file, $opts) = @_;

  my @tiefile;
  tie @tiefile, 'Tie::File', $file, %{ $opts->{tie_file} || {} }
    or croak "Cannot tie file $file";

  my $csv = Text::CSV->new($opts->{text_csv} || {}) 
    or croak "CSV (new) error: " . Text::CSV->error_diag();

  my $self = {
    file => \@tiefile,
    csv => $csv,
  };

  bless $self, $class;

  return $self;
}

sub FETCH {
  my $self = shift;
  my $index = shift;

  my $line = $self->{file}[$index];

  $self->{csv}->parse($line)
    or croak "CSV parse error: " . $self->{csv}->error_diag();
  my @fields = $self->{csv}->fields;

  tie my @line, 'Tie::Array::CSV::Row', { 
    file => $self->{file},
    line_num => $index,
    fields => \@fields, 
    csv => $self->{csv},
  };

  return \@line;
}

sub STORE {
  my $self = shift;
  my ($index, $value) = @_;

  $self->{csv}->combine(
    ref $value ? @$value : ($value)
  ) 
    or croak "CSV combine error: " . $self->{csv}->error_diag();
  $self->{file}[$index] = $self->{csv}->string;
}

sub FETCHSIZE {
  my $self = shift;

  return scalar @{ $self->{file} };
}

sub STORESIZE {
  my $self = shift;
  my $new_size = shift;

  $#{ $self->{file} } = $new_size - 1;
  
}

package Tie::Array::CSV::Row;

use Carp;

use Tie::Array;
our @ISA = ('Tie::Array');

use overload 
  '@{}' => sub{ return @{ $_[0]{fields} } };

sub TIEARRAY {
  my $class = shift;
  my $self = shift;

  bless $self, $class;

  return $self;
}

sub FETCH {
  my $self = shift;
  my $index = shift;

  return $self->{fields}[$index];
}

sub STORE {
  my $self = shift;
  my ($index, $value) = @_;

  $self->{fields}[$index] = $value;

  $self->_update;

}

sub FETCHSIZE {
  my $self = shift;

  return scalar @{ $self->{fields} };
}

sub STORESIZE {
  my $self = shift;
  my $new_size = shift;

  $#{ $self->{fields} } = $new_size - 1;

  $self->_update;
}

sub SHIFT {
  my $self = shift;

  my $value = shift @{ $self->{fields} };

  $self->_update;

  return $value;
}

sub UNSHIFT {
  my $self = shift;
  my $value = shift;

  unshift @{ $self->{fields} }, $value;

  $self->_update;

}

sub _update {
  my $self = shift;

  $self->{csv}->combine(@{ $self->{fields} })
    or croak "CSV combine error: " . $self->{csv}->error_diag();
  $self->{file}[$self->{line_num}] = $self->{csv}->string;
}

__END__
__POD__

=head1 NAME

Tie::Array::CSV - A tied array which combines the power of Tie::File and Text::CSV

=head1 SYNOPSIS

 use strict; use warnings;
 use Tie::Array::CSV;
 tie my @file, 'Tie::Array::CSV', 'filename';

 print $file[0][2];
 $file[3][5] = "Camel";

=head1 DESCRIPTION

This module allows an array to be tied to a CSV file for reading and writing. The array is a standard Perl 2D array (i.e. an array of array references) which gives access to the row and column of the user's choosing. This is done using the well established modules:

=over

=item * 
L<Tie::File>

=over

=item *

arbitrary line access

=item *

low memory use even for large files

=back

=item *

L<Text::CSV> 

=over

=item *

row parsing

=item *

row updating

=item *

uses the speedy L<Text::CSV_XS> if installed

=back

=back

This module was inspired by L<Tie::CSV_File> which (sadly) hasn't been maintained. It also doesn't attempt to do any of the parsing (as that module did), but rather passes all of the heavy lifting to other modules.

=head1 OPTIONS

As with any tied array, the construction uses the C<tie> function. 

 tie my @file, 'Tie::Array::CSV', 'filename'

would tie the lexically scoped array C<@file> to the file C<filename> using this module. Following these three arguements to C<tie>, one may optionally pass a hashref containing additional configuration. Currently the only options are "pass-through" options, sent to the constructors of the different modules used internally, read more about them in those module's documentation.

=over

=item *

tie_file - hashref of options which are passed to the L<Tie::File> constructor

=item *

text_csv - hashref of options which are passed to the L<Text::CSV> constructor

=back

=head1 ERRORS

For simplicity this module C<croak>s on all errors, which are trappable using a C<$SIG{__DIE__}> handler.

=head1 CAVEATS

Much of the functionality of normal arrays is mimicked using L<Tie::Array>. The interaction of this with L<Tie::File> should be mentioned in that certain actions may be very inefficient. For example, C<(un)shift>-ing the first row of data will probably involve L<Tie::Array> asking L<Tie::File> to move each row up one line, one-by-one. As a note, the intra-row C<(un)shift> does not suffer this problem.

=head1 SEE ALSO

=over

=item *

L<Tie::CSV_File> - inspiration for this module, but problematic

=item *

L<Tie::Array::DBD> - tie database connection to array

=item *

L<Tie::DBI> - similar but hash based

=back

=head1 SOURCE REPOSITORY

L<http://github.com/jberger/Tie-Array-CSV>

=head1 AUTHOR

Joel Berger, E<lt>joel.a.berger@gmail.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by Joel Berger

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

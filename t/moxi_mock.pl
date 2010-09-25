#!/usr/bin/perl

# This program starts moxi and then runs the python mock server tests.
#
#  ./t/moxi_mock.pl [upstream_protocol] [downstream_protocol] [test_name]
#
# An upstream_protocol/downstream_protocol is ascii or binary.
#
# Parameters are optional, so these examples work, and default to ascii...
#
#  ./t/moxi_mock.pl
#  ./t/moxi_mock.pl ascii
#  ./t/moxi_mock.pl ascii ascii TestProxyAscii.testBasicQuit
#
# Alternative command-line...
#
#  ./t/moxi_mock.pl [mock_test] [downstream_protocol] [test_name] \
#     [moxi-z-param] [moxi-Z-param] [rest/http-server-params]
#
#  ./t/moxi_mock.pl moxi_mock_auth binary "" \
#                   url=http://127.0.0.1:4567/pools/default/buckets/default \
#                   usr=TheUser,pwd=ThePassword,port_listen=11333, \
#                   ./t/rest_mock.rb
#
my $upstream_protocol   = $ARGV[0] || 'ascii';
my $downstream_protocol = $ARGV[1] || 'ascii';
my $test_name           = $ARGV[2] || '';
my $little_z            = $ARGV[3] || './t/moxi_mock.cfg';
my $big_Z               = $ARGV[4] || '';
my $restargs            = $ARGV[5] || '...NONE...';

print "moxi_mock.pl: " . $upstream_protocol . " " . $downstream_protocol . " " . $test_name + "\n";

my $exe = "./moxi";

croak("moxi binary doesn't exist.  Haven't run 'make' ?\n") unless -e $exe;
croak("moxi binary not executable\n") unless -x _;

# Fork rest/http server if necessary.
#
my $restpid = -1;
if ($restargs ne '...NONE...') {
  print($restargs . "\n");
  $restpid = fork();
  unless ($restpid) {
    setpgrp();
    exec "ruby $restargs";
    exit;
  }
  setpgrp($childpid, $childpid);
  sleep(1);
}

# Fork moxi for moxi-specific testing.
#
my $childargs =
      " -z " . $little_z .
      " -p 0 -U 0 -v -t 1" .
      " -Z \"" . $big_Z .
            "downstream_max=1,downstream_conn_max=0," .
            "downstream_protocol=" . $downstream_protocol . "\"";
if ($< == 0) {
   $childargs .= " -u root";
}

print($childargs . "\n");

my $childpid = fork();

unless ($childpid) {
    setpgrp();
    exec "$exe $childargs";
    exit; # never gets here.
}
setpgrp($childpid, $childpid);
sleep(1);

my $result = -1;

if ($upstream_protocol ne 'ascii' &&
    $upstream_protocol ne 'binary') {
  $result = system("python ./t/" . $ARGV[0] . ".py " . $test_name);
} else {
  my $u = substr($upstream_protocol, 0, 1); # This is 'a' or 'b'.
  my $d = substr($downstream_protocol, 0, 1); # This is 'a' or 'b'.

  $result = system("python ./t/moxi_mock_" . $u . "2" . $d . ".py " . $test_name);
}

kill 2, -$childpid;

if ($restpid >= 0) {
  kill 2, -$restpid;
  sleep(1);
}

exit $result;

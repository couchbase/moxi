#!/usr/bin/perl

# Run this main test driver program from the project's
# top directory, which has t as a subdirectory.
#
my $exe = "./moxi";

use Carp qw(croak);

croak("moxi binary doesn't exist.  Haven't run 'make' ?\n") unless -e $exe;
croak("moxi binary not executable\n") unless -x _;

sub go {
  if ($ARGV[0] eq 'mock_only') {
    return;
  }
  my ($topology, $protocol) = @_;
  print "------------------------------------\n";
  print "testing $topology $protocol\n";
  my $result = system("./t/moxi.pl $topology $protocol");
  if ($result != 0) {
    print("fail moxi.pl $topology $protocol test\n");
    exit $result;
  }
}

# Ascii protocol compatibility tests.
#
go('simple',   'ascii');
go('chain',    'ascii');
go('fanout',   'ascii');
go('fanoutin', 'ascii');

# Binary protocol compatibility tests.
#
go('simple', 'binary');
go('fanout', 'binary');

print "------------------------------------ ascii\n";

my $res = system("./t/moxi_mock.pl ascii");
if ($res != 0) {
  print "exit: $res\n";
  exit($res);
}

print "------------------------------------ auth\n";

my $cmd = "./t/moxi_mock.pl moxi_mock_auth binary \"\"" .
                 " url=http://127.0.0.1:4567/pools/default/buckets/default" .
                 " usr=TheUser,pwd=ThePassword,port_listen=11333," .
                   "downstream_timeout=0," .
                   "downstream_conn_queue_timeout=0,wait_queue_timeout=0," .
                   "connect_timeout=5000,auth_timeout=0," .
                   "connect_max_errors=0,connect_retry_interval=0," .
                 " ./t/rest_mock.rb";
print($cmd . "\n");
my $res = system($cmd);
if ($res != 0) {
  print "exit: $res\n";
  exit($res);
}

sleep(1);

print "------------------------------------ multitenancy\n";

my $cmd = "./t/moxi_mock.pl moxi_multitenancy binary \"\"" .
                 " url=http://127.0.0.1:4567/pools/default/buckets/default" .
                 " default_bucket_name=default,port_listen=11333," .
                   "downstream_timeout=0," .
                   "downstream_conn_queue_timeout=0,wait_queue_timeout=0," .
                   "connect_timeout=5000,auth_timeout=0," .
                   "connect_max_errors=0,connect_retry_interval=0," .
                 " \"./t/rest_mock.rb ./t/moxi_multitenancy_rest.cfg\"";
print($cmd . "\n");
my $res = system($cmd);
if ($res != 0) {
  print "exit: $res\n";
  exit($res);
}

sleep(1);

print "------------------------------------ multitenancy_default\n";

my $cmd = "./t/moxi_mock.pl moxi_multitenancy_default binary \"\"" .
                 " url=http://127.0.0.1:4567/pools/default/buckets/default" .
                 " default_bucket_name=default,port_listen=11333," .
                   "downstream_timeout=0," .
                   "downstream_conn_queue_timeout=0,wait_queue_timeout=0," .
                   "connect_timeout=5000,auth_timeout=0," .
                   "connect_max_errors=0,connect_retry_interval=0," .
                 " \"./t/rest_mock.rb ./t/moxi_multitenancy_rest_default.cfg\"";
print($cmd . "\n");
my $res = system($cmd);
if ($res != 0) {
  print "exit: $res\n";
  exit($res);
}



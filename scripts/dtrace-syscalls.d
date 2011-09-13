/* Trace all the syscalls invoked during a given C function.
 *
 * usage:
 *   sudo dtrace -q -s dtrace-syscalls.d PID PROBEFUNC
 *
 * example:
 *   sudo dtrace -q -s dtrace-syscalls.d 6666 try_read_command
 */

pid$1::$2:entry
{
  printf("starting %s\n", probefunc);
  self->in_function = 1;
  ustack();
}

pid$1::$2:return
{
  printf("ending %s\n", probefunc);
  self->in_function = 0;
}

syscall:::
/ self->in_function != 0 /
{
  printf("%s %s %s\n", execname, probefunc, probename);
}



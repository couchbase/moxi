/* Count function invocations.
 *
 * usage:
 *   sudo dtrace -q -s dtrace-profile.d PID
 */

pid$1:::entry
{
  self->t[probefunc] = timestamp;
}

pid$1:::return
/ self->t[probefunc] != 0 /
{
  @counts[probemod, probefunc] = sum(timestamp - self->t[probefunc]);
}

tick-5s
{
  printa(@counts);
}

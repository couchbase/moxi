#ifndef TEST_COMMON_H
#define TEST_COMMON_H 1

#include <libconflate/conflate.h>

void fail_if_impl(bool val, const char *msg, const char *file, int line);


#define fail_if(a, b) fail_if_impl(a, b, __FILE__, __LINE__)
#define fail_unless(a, b) fail_if(!(a), b)

void check_pair_equality(kvpair_t *one, kvpair_t *two);

#endif /* TEST_COMMMON_H */

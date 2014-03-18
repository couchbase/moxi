#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <libconflate/conflate.h>

#include "test_common.h"

static kvpair_t *pair = NULL;

static void setup(void) {
    pair = NULL;
}

static void teardown(void) {
    free_kvpair(pair);
}

static void test_mk_pair_with_arg(void)
{
    char* args[] = {"arg1", "arg2", NULL};
    pair = mk_kvpair("some_key", args);

    fail_if(pair == NULL, "Didn't create a pair.");
    fail_unless(strcmp(pair->key, "some_key") == 0, "Key is broken.");
    fail_unless(strcmp(pair->values[0], "arg1") == 0, "First value is broken.");
    fail_unless(strcmp(pair->values[1], "arg2") == 0, "Second value is broken.");
    fail_unless(pair->used_values == 2, "Wrong number of used values.");
    fail_unless(pair->allocated_values >= pair->used_values,
                "Allocated values can't be smaller than used values.");
    fail_unless(pair->next == NULL, "Next pointer is non-null.");
}

static void test_mk_pair_without_arg(void)
{
    pair = mk_kvpair("some_key", NULL);

    fail_if(pair == NULL, "Didn't create a pair.");
    fail_unless(strcmp(pair->key, "some_key") == 0, "Key is broken.");
    fail_unless(pair->used_values == 0, "Has values?");
    fail_unless(pair->allocated_values >= pair->used_values,
                "Allocated values can't be smaller than used values.");
    fail_unless(pair->values[0] == NULL, "First value isn't null.");
    fail_unless(pair->next == NULL, "Next pointer is non-null.");
}

static void test_add_value_to_empty_values(void)
{
    pair = mk_kvpair("some_key", NULL);

    fail_if(pair == NULL, "Didn't create a pair.");
    fail_unless(strcmp(pair->key, "some_key") == 0, "Key is broken.");
    fail_unless(pair->used_values == 0, "Has values?");
    fail_unless(pair->allocated_values >= pair->used_values,
                "Allocated values can't be smaller than used values.");
    fail_unless(pair->values[0] == NULL, "First value isn't null.");
    fail_unless(pair->next == NULL, "Next pointer is non-null.");

    add_kvpair_value(pair, "newvalue1");
    fail_unless(pair->used_values == 1, "Value at 1");
    fail_unless(strcmp(pair->values[0], "newvalue1") == 0, "Unexpected value at 0");

    add_kvpair_value(pair, "newvalue2");
    fail_unless(pair->used_values == 2, "Value at 2");
    fail_unless(strcmp(pair->values[1], "newvalue2") == 0, "Unexpected value at 1");
}

static void test_add_value_to_existing_values(void)
{
    char* args[] = {"arg1", "arg2", NULL};
    pair = mk_kvpair("some_key", args);

    fail_if(pair == NULL, "Didn't create a pair.");
    fail_unless(strcmp(pair->key, "some_key") == 0, "Key is broken.");
    fail_unless(pair->used_values == 2, "Has values?");
    fail_unless(pair->allocated_values >= pair->used_values,
                "Allocated values can't be smaller than used values.");
    fail_unless(pair->next == NULL, "Next pointer is non-null.");

    add_kvpair_value(pair, "newvalue1");
    fail_unless(pair->used_values == 3, "Value at 3");
    add_kvpair_value(pair, "newvalue2");
    fail_unless(pair->used_values == 4, "Value at 4");

    fail_unless(strcmp(pair->values[0], "arg1") == 0, "Unexpected value at 0");
    fail_unless(strcmp(pair->values[1], "arg2") == 0, "Unexpected value at 1");
    fail_unless(strcmp(pair->values[2], "newvalue1") == 0, "Unexpected value at 2");
    fail_unless(strcmp(pair->values[3], "newvalue2") == 0, "Unexpected value at 3");
}

static void test_find_from_null(void)
{
    fail_unless(find_kvpair(NULL, "some_key") == NULL, "Couldn't find from NULL.");
}

static void test_find_first_item(void)
{
    pair = mk_kvpair("some_key", NULL);
    fail_unless(find_kvpair(pair, "some_key") == pair, "Identity search failed.");
}

static void test_find_second_item(void)
{
    kvpair_t *pair1 = mk_kvpair("some_key", NULL);
    pair = mk_kvpair("some_other_key", NULL);
    pair->next = pair1;

    fail_unless(find_kvpair(pair, "some_key") == pair1, "Depth search failed.");
}

static void test_find_missing_item(void)
{
    kvpair_t *pair1 = mk_kvpair("some_key", NULL);
    pair = mk_kvpair("some_other_key", NULL);
    pair->next = pair1;

    fail_unless(find_kvpair(pair, "missing_key") == NULL, "Negative search failed.");
}

static void test_simple_find_from_null(void)
{
    fail_unless(get_simple_kvpair_val(NULL, "some_key") == NULL,
                "Couldn't find from NULL.");
}

static void test_simple_find_first_item(void)
{
    char *val[2] = { "someval", NULL };
    pair = mk_kvpair("some_key", val);
    fail_unless(strcmp(get_simple_kvpair_val(pair, "some_key"),
                       "someval") == 0, "Identity search failed.");
}

static void test_simple_find_second_item(void)
{
    char *val[2] = { "someval", NULL };
    kvpair_t *pair1 = mk_kvpair("some_key", val );
    pair = mk_kvpair("some_other_key", NULL);
    pair->next = pair1;

    fail_unless(strcmp(get_simple_kvpair_val(pair, "some_key"),
                       "someval") == 0, "Depth search failed.");
}

static void test_simple_find_missing_item(void)
{
    kvpair_t *pair1 = mk_kvpair("some_key", NULL);
    pair = mk_kvpair("some_other_key", NULL);
    pair->next = pair1;

    fail_unless(get_simple_kvpair_val(pair, "missing_key") == NULL,
                "Negative search failed.");
}

static void test_copy_pair(void)
{
    char *args1[] = {"arg1", "arg2", NULL};
    char *args2[] = {"other", NULL};
    kvpair_t *pair1 = mk_kvpair("some_key", args1);
    kvpair_t *copy;
    pair = mk_kvpair("some_other_key", args2);
    pair->next = pair1;

    copy = dup_kvpair(pair);
    fail_if(copy == NULL, "Copy failed.");
    fail_if(copy == pair, "Copy something not an identity.");

    fail_unless(strcmp(copy->key, pair->key) == 0, "Keys don't match.");
    fail_if(copy->key == pair->key, "Keys were identical.");
    check_pair_equality(pair, copy);

    free_kvpair(copy);
}

static bool walk_incr_count_true(void *opaque,
                                 const char *key,
                                 const char **values)
{
    (*(int*)opaque)++;
    (void)key;
    (void)values;
    return true;
}

static bool walk_incr_count_false(void *opaque,
                                  const char *key,
                                  const char **values)
{
    (*(int*)opaque)++;
    (void)key;
    (void)values;
    return false;
}

static void test_walk_true(void)
{
    char *args1[] = {"arg1", "arg2", NULL};
    char *args2[] = {"other", NULL};
    kvpair_t *pair1 = mk_kvpair("some_key", args1);
    int count = 0;

    pair = mk_kvpair("some_other_key", args2);
    pair->next = pair1;

    walk_kvpair(pair, &count, walk_incr_count_true);

    fail_unless(count == 2, "Count was not two");
}

static void test_walk_false(void)
{
    int count = 0;
    char *args1[] = {"arg1", "arg2", NULL};
    char *args2[] = {"other", NULL};
    kvpair_t *pair1 = mk_kvpair("some_key", args1);
    pair = mk_kvpair("some_other_key", args2);
    pair->next = pair1;

    walk_kvpair(pair, &count, walk_incr_count_false);

    printf("Count was %d\n", count);
    fail_unless(count == 1, "Count was not one");
}

int main(void)
{
    typedef void (*testcase)(void);
    testcase tc[] = {
        test_mk_pair_with_arg,
        test_mk_pair_without_arg,
        test_add_value_to_existing_values,
        test_add_value_to_empty_values,
        test_find_from_null,
        test_find_first_item,
        test_find_second_item,
        test_find_missing_item,
        test_simple_find_from_null,
        test_simple_find_first_item,
        test_simple_find_second_item,
        test_simple_find_missing_item,
        test_copy_pair,
        test_walk_true,
        test_walk_false,
        NULL
    };
    int ii = 0;

    while (tc[ii] != 0) {
        setup();
        tc[ii++]();
        teardown();
    }

    return EXIT_SUCCESS;
}

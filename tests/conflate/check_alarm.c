#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <pthread.h>

#include <alarm.h>
#include "test_common.h"

static alarm_queue_t *alarmqueue = NULL;

static void handle_setup(void) {
    alarmqueue = init_alarmqueue();
    fail_if(alarmqueue == NULL, "Failed to set up alarm queue");
}

static void handle_teardown(void) {
    destroy_alarmqueue(alarmqueue);
    alarmqueue = NULL;
}

static void test_simple_alarm(void)
{
    alarm_t in_alarm;

    fail_unless(add_alarm(alarmqueue, "test", "This is a test message 1"),
                "Failed to alarm.");
    in_alarm = get_alarm(alarmqueue);
    fail_unless(in_alarm.open == 1, "Didn't receive alarm message 1.");
    fail_unless(strcmp(in_alarm.name, "test") == 0, "Alarm name didn't match");
    fail_unless(strcmp(in_alarm.msg, "This is a test message 1") == 0,
                "Didn't get the right message for message 1.");

    fail_unless(add_alarm(alarmqueue, "test2", "This is a test message 2"),
                "Failed to alarm.");
    fail_unless(add_alarm(alarmqueue, "test3", "This is a test message 3"),
                "Failed to alarm.");
    in_alarm = get_alarm(alarmqueue);
    fail_unless(in_alarm.open == 1, "Didn't receive alarm message 2.");
    fail_unless(strcmp(in_alarm.name, "test2") == 0, "test2 didn't match");
    fail_unless(strcmp(in_alarm.msg, "This is a test message 2") == 0,
                "Didn't get the right message for message 2.");

    in_alarm = get_alarm(alarmqueue);
    fail_unless(in_alarm.open == 1, "Didn't receive alarm message 1.");
    fail_unless(strcmp(in_alarm.name, "test3") == 0, "test3 didn't match");
    fail_unless(strcmp(in_alarm.msg, "This is a test message 3") == 0,
                "Didn't get the right message for message 3.");
    in_alarm = get_alarm(alarmqueue);
    fail_unless(in_alarm.open == 0, "Shouldn't have recieved open alarm.");
}

static void test_giant_alarm(void)
{
    char *msg="This is a really large message that exceeds the 256 or "
        "so bytes allocated for messages to see what happens when a "
        "message is too big.  Hopefully it's fine.  "
        "It turns out that 256 bytes or whatever the current value is ends "
        "up being quite a bit of bytes.  I suppose that makes sense if "
        "I do a bit of math, but I'm too lazy to do anything other than assert.";

    fail_unless(strlen(msg) > ALARM_MSG_MAXLEN,
                "Message string was too short to blow up.");
    fail_unless(add_alarm(alarmqueue, "bigass", msg), "Failed to alarm.");

    alarm_t in_alarm = get_alarm(alarmqueue);
    fail_unless(in_alarm.open, "Didn't receive a large alarm.");

    fail_unless(strlen(in_alarm.msg) == ALARM_MSG_MAXLEN,
            "Alarm message is too long.");

    fail_unless(strcmp("bigass", in_alarm.name) == 0,
                "Alarm name didn't match.");
    fail_unless(strncmp(msg, in_alarm.msg, ALARM_MSG_MAXLEN) == 0,
                "Alarm message didn't match.");
}

static void test_giant_name(void)
{
    const char *name = "this name should exceed the max length";
    fail_unless(strlen(name) > ALARM_NAME_MAXLEN,
                "Name wasn't too big enough.");

    fail_unless(add_alarm(alarmqueue, name, "some message"),
                "Failed to alarm.");

    alarm_t in_alarm = get_alarm(alarmqueue);
    fail_unless(in_alarm.open, "Didn't receive an alarm.");

    fail_unless(strlen(in_alarm.name) == ALARM_NAME_MAXLEN,
                "Alarm name is too long.");
    fail_unless(strncmp(name, in_alarm.name, ALARM_NAME_MAXLEN) == 0,
                "Alarm name didn't match.");
    fail_unless(strcmp("some message", in_alarm.msg) == 0,
                "Alarm message didn't match.");
}

static void test_full_queue(void)
{
    for (int i = 0; i < ALARM_QUEUE_SIZE; i++) {
        fail_unless(add_alarm(alarmqueue, "add", "Test alarm message."),
                    "Failed to add alarm.");
    }
    fail_if(add_alarm(alarmqueue, "fail", "Test failing alarm."),
            "Should have failed to add another alarm.");
}

int main(void)
{
    typedef void (*testcase)(void);
    testcase tc[] = {
        test_simple_alarm,
        test_giant_alarm,
        test_giant_name,
        test_full_queue,
        NULL
    };

    int ii = 0;

    while (tc[ii] != 0) {
        handle_setup();
        tc[ii++]();
        handle_teardown();
    }

    return EXIT_SUCCESS;
}

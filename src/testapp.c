/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#undef NDEBUG
#include "src/config.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <platform/cbassert.h>
#include <string.h>
#include <inttypes.h>
#include <stdbool.h>
#include <unistd.h>
#include <netinet/in.h>

#include "protocol_binary.h"
#include "cache.h"
#include "util.h"

#define TMP_TEMPLATE "/tmp/test_file.XXXXXXX"

enum test_return { TEST_SKIP, TEST_PASS, TEST_FAIL };

static enum test_return cache_create_test(void)
{
    cache_t *cache = cache_create("test", sizeof(uint32_t), sizeof(char*),
                                  NULL, NULL);
    cb_assert(cache != NULL);
    cache_destroy(cache);
    return TEST_PASS;
}

const uint64_t constructor_pattern = 0xdeadcafebabebeef;

static int cache_constructor(void *buffer, void *notused1, int notused2) {
    uint64_t *ptr = buffer;
    *ptr = constructor_pattern;

    (void)notused1;
    (void)notused2;

    return 0;
}

static enum test_return cache_constructor_test(void)
{
    uint64_t *ptr;
    uint64_t pattern;
    cache_t *cache = cache_create("test", sizeof(uint64_t), sizeof(uint64_t),
                                  cache_constructor, NULL);
    cb_assert(cache != NULL);
    ptr = cache_alloc(cache);
    pattern = *ptr;
    cache_free(cache, ptr);
    cache_destroy(cache);
    return (pattern == constructor_pattern) ? TEST_PASS : TEST_FAIL;
}

static int cache_fail_constructor(void *buffer, void *notused1, int notused2) {
    (void)buffer;
    (void)notused1;
    (void)notused2;
    return 1;
}

static enum test_return cache_fail_constructor_test(void)
{
    enum test_return ret = TEST_PASS;

    cache_t *cache = cache_create("test", sizeof(uint64_t), sizeof(uint64_t),
                                  cache_fail_constructor, NULL);
    uint64_t *ptr;

    cb_assert(cache != NULL);
    ptr = cache_alloc(cache);
    if (ptr != NULL) {
        ret = TEST_FAIL;
    }
    cache_destroy(cache);
    return ret;
}

static void *destruct_data = 0;

static void cache_destructor(void *buffer, void *notused) {
    (void)notused;
    destruct_data = buffer;
}

static enum test_return cache_destructor_test(void)
{
    cache_t *cache = cache_create("test", sizeof(uint32_t), sizeof(char*),
                                  NULL, cache_destructor);
    char *ptr;

    cb_assert(cache != NULL);
    ptr = cache_alloc(cache);
    cache_free(cache, ptr);
    cache_destroy(cache);

    return (ptr == destruct_data) ? TEST_PASS : TEST_FAIL;
}

static enum test_return cache_reuse_test(void)
{
    int ii;
    cache_t *cache = cache_create("test", sizeof(uint32_t), sizeof(char*),
                                  NULL, NULL);
    char *ptr = cache_alloc(cache);
    cache_free(cache, ptr);
    for (ii = 0; ii < 100; ++ii) {
        char *p = cache_alloc(cache);
        cb_assert(p == ptr);
        cache_free(cache, ptr);
    }
    cache_destroy(cache);
    return TEST_PASS;
}

static enum test_return cache_redzone_test(void)
{
#ifndef HAVE_UMEM_H
    cache_t *cache = cache_create("test", sizeof(uint32_t), sizeof(char*),
                                  NULL, NULL);

    char *p;
    char old;
    /* Ignore SIGABORT */
    struct sigaction old_action;
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = SIG_IGN;

    sigemptyset(&action.sa_mask);
    sigaction(SIGABRT, &action, &old_action);

    /* check memory debug.. */
    p = cache_alloc(cache);
    old = *(p - 1);
    *(p - 1) = 0;
    cache_free(cache, p);
    cb_assert(cache_error == -1);
    *(p - 1) = old;

    p[sizeof(uint32_t)] = 0;
    cache_free(cache, p);
    cb_assert(cache_error == 1);

    /* restore signal handler */
    sigaction(SIGABRT, &old_action, NULL);

    cache_destroy(cache);

    return TEST_PASS;
#else
    return TEST_SKIP;
#endif
}

static enum test_return test_safe_strtoul(void) {
    uint32_t val;
    cb_assert(safe_strtoul("123", &val));
    cb_assert(val == 123);
    cb_assert(safe_strtoul("+123", &val));
    cb_assert(val == 123);
    cb_assert(!safe_strtoul("", &val));  /* empty */
    cb_assert(!safe_strtoul("123BOGUS", &val));  /* non-numeric */
    /* Not sure what it does, but this works with ICC :/
       cb_assert(!safe_strtoul("92837498237498237498029383", &val)); // out of range
    */

    /* extremes: */
    cb_assert(safe_strtoul("4294967295", &val)); /* 2**32 - 1 */
    cb_assert(val == 4294967295L);
    /* This actually works on 64-bit ubuntu
       cb_assert(!safe_strtoul("4294967296", &val)); // 2**32
    */
    cb_assert(!safe_strtoul("-1", &val));  /* negative */
    return TEST_PASS;
}


static enum test_return test_safe_strtoull(void) {
    uint64_t val;
    cb_assert(safe_strtoull("123", &val));
    cb_assert(val == 123);
    cb_assert(safe_strtoull("+123", &val));
    cb_assert(val == 123);
    cb_assert(!safe_strtoull("", &val));  /* empty */
    cb_assert(!safe_strtoull("123BOGUS", &val));  /* non-numeric */
    cb_assert(!safe_strtoull("92837498237498237498029383", &val)); /* out of range */

    /* extremes: */
    cb_assert(safe_strtoull("18446744073709551615", &val)); /* 2**64 - 1 */
    cb_assert(val == 18446744073709551615ULL);
    cb_assert(!safe_strtoull("18446744073709551616", &val)); /* 2**64 */
    cb_assert(!safe_strtoull("-1", &val));  /* negative */
    return TEST_PASS;
}

static enum test_return test_safe_strtoll(void) {
    int64_t val;
    cb_assert(safe_strtoll("123", &val));
    cb_assert(val == 123);
    cb_assert(safe_strtoll("+123", &val));
    cb_assert(val == 123);
    cb_assert(safe_strtoll("-123", &val));
    cb_assert(val == -123);
    cb_assert(!safe_strtoll("", &val));  /* empty */
    cb_assert(!safe_strtoll("123BOGUS", &val));  /* non-numeric */
    cb_assert(!safe_strtoll("92837498237498237498029383", &val)); /* out of range */

    /* extremes: */
    cb_assert(!safe_strtoll("18446744073709551615", &val)); /* 2**64 - 1 */
    cb_assert(safe_strtoll("9223372036854775807", &val)); /* 2**63 - 1 */
    cb_assert(val == 9223372036854775807LL);
    /*
      cb_assert(safe_strtoll("-9223372036854775808", &val)); // -2**63
      cb_assert(val == -9223372036854775808LL);
    */
    cb_assert(!safe_strtoll("-9223372036854775809", &val)); /* -2**63 - 1 */

    /* We'll allow space to terminate the string.  And leading space. */
    cb_assert(safe_strtoll(" 123 foo", &val));
    cb_assert(val == 123);
    return TEST_PASS;
}

static enum test_return test_safe_strtol(void) {
    int32_t val;
    cb_assert(safe_strtol("123", &val));
    cb_assert(val == 123);
    cb_assert(safe_strtol("+123", &val));
    cb_assert(val == 123);
    cb_assert(safe_strtol("-123", &val));
    cb_assert(val == -123);
    cb_assert(!safe_strtol("", &val));  /* empty */
    cb_assert(!safe_strtol("123BOGUS", &val));  /* non-numeric */
    cb_assert(!safe_strtol("92837498237498237498029383", &val)); /* out of range */

    /* extremes: */
    /* This actually works on 64-bit ubuntu
       cb_assert(!safe_strtol("2147483648", &val)); // (expt 2.0 31.0)
    */
    cb_assert(safe_strtol("2147483647", &val)); /* (- (expt 2.0 31) 1) */
    cb_assert(val == 2147483647L);
    /* This actually works on 64-bit ubuntu
       cb_assert(!safe_strtol("-2147483649", &val)); // (- (expt -2.0 31) 1)
    */

    /* We'll allow space to terminate the string.  And leading space. */
    cb_assert(safe_strtol(" 123 foo", &val));
    cb_assert(val == 123);
    return TEST_PASS;
}

/**
 * Function to start the server and let it listen on a random port
 *
 * @param port_out where to store the TCP port number the server is
 *                 listening on
 * @param is_daemon set to true if you want to run the memcached server
 *               as a daemon process
 * @return the pid of the memcached server
 */
static pid_t start_server(in_port_t *port_out, bool is_daemon) {
    pid_t pid;
    FILE *fp;
    char buffer[80];
    char environment[80];
    char pid_file[80];
    char *filename;

    snprintf(environment, sizeof(environment),
             "MEMCACHED_PORT_FILENAME=/tmp/ports.%lu", (long)getpid());
    filename = environment + strlen("MEMCACHED_PORT_FILENAME=");
    snprintf(pid_file, sizeof(pid_file), "/tmp/pid.%lu", (long)getpid());

    remove(filename);
    remove(pid_file);

    pid = fork();
    cb_assert(pid != -1);

    if (pid == 0) {
        /* Child */
        char *argv[20];
        int arg = 0;
        putenv(environment);
#ifdef __sun
        putenv("LD_PRELOAD=watchmalloc.so.1");
        putenv("MALLOC_DEBUG=WATCH");
#endif

        if (!is_daemon) {
            argv[arg++] = "./timedrun";
            argv[arg++] = "15";
        }
        argv[arg++] = "./memcached-debug";
        argv[arg++] = "-p";
        argv[arg++] = "-1";
        argv[arg++] = "-U";
        argv[arg++] = "0";
        /* Handle rpmbuild and the like doing this as root */
        if (getuid() == 0) {
            argv[arg++] = "-u";
            argv[arg++] = "root";
        }
        if (is_daemon) {
            argv[arg++] = "-d";
            argv[arg++] = "-P";
            argv[arg++] = pid_file;
        }
        argv[arg++] = NULL;
        cb_assert(execv(argv[0], argv) != -1);
    }

    /* Yeah just let us "busy-wait" for the file to be created ;-) */
    while (access(filename, F_OK) == -1) {
        usleep(10);
    }

    fp = fopen(filename, "r");
    if (fp == NULL) {
        fprintf(stderr, "Failed to open the file containing port numbers: %s\n",
                strerror(errno));
        cb_assert(false);
    }

    *port_out = (in_port_t)-1;
    while ((fgets(buffer, sizeof(buffer), fp)) != NULL) {
        if (strncmp(buffer, "TCP INET: ", 10) == 0) {
            int32_t val;
            cb_assert(safe_strtol(buffer + 10, &val));
            *port_out = (in_port_t)val;
        }
    }

    fclose(fp);
    cb_assert(remove(filename) == 0);

    if (is_daemon) {
        int32_t val;
        /* loop and wait for the pid file.. There is a potential race
         * condition that the server just created the file but isn't
         * finished writing the content, but I'll take the chance....
         */
        while (access(pid_file, F_OK) == -1) {
            usleep(10);
        }

        fp = fopen(pid_file, "r");
        if (fp == NULL) {
            fprintf(stderr, "Failed to open pid file: %s\n",
                    strerror(errno));
            cb_assert(false);
        }
        cb_assert(fgets(buffer, sizeof(buffer), fp) != NULL);
        fclose(fp);

        cb_assert(safe_strtol(buffer, &val));
        pid = (pid_t)val;
    }

    return pid;
}

static enum test_return test_issue_44(void) {
    in_port_t port;
    pid_t pid = start_server(&port, true);
    cb_assert(kill(pid, SIGHUP) == 0);
    sleep(1);
    cb_assert(kill(pid, SIGTERM) == 0);

    return TEST_PASS;
}

/*
 * static struct addrinfo *lookuphost(const char *hostname, in_port_t port)
 * {
 *     struct addrinfo *ai = 0;
 *     struct addrinfo hints = { .ai_family = AF_UNSPEC,
 *                               .ai_protocol = IPPROTO_TCP,
 *                               .ai_socktype = SOCK_STREAM };
 *     char service[NI_MAXSERV];
 *     int error;
 *
 *     (void)snprintf(service, NI_MAXSERV, "%d", port);
 *     if ((error = getaddrinfo(hostname, service, &hints, &ai)) != 0) {
 *        if (error != EAI_SYSTEM) {
 *           fprintf(stderr, "getaddrinfo(): %s\n", gai_strerror(error));
 *        } else {
 *           perror("getaddrinfo()");
 *        }
 *     }
 *
 *     return ai;
 * }
 */

/*
 * static int connect_server(const char *hostname, in_port_t port)
 * {
 *     struct addrinfo *ai = lookuphost(hostname, port);
 *     int sock = -1;
 *     if (ai != NULL) {
 *        if ((sock = socket(ai->ai_family, ai->ai_socktype,
 *                           ai->ai_protocol)) != -1) {
 *           if (connect(sock, ai->ai_addr, ai->ai_addrlen) == -1) {
 *              fprintf(stderr, "Failed to connect socket: %s\n",
 *                      strerror(errno));
 *              close(sock);
 *              sock = -1;
 *           }
 *        } else {
 *           fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
 *        }
 *
 *        freeaddrinfo(ai);
 *     }
 *     return sock;
 * }
 */

static enum test_return test_vperror(void) {
    int rv = 0;
    int oldstderr = dup(STDERR_FILENO);
    char tmpl[sizeof(TMP_TEMPLATE)+1];
    int newfile;
    char buf[80] = { 0 };
    FILE *efile;
    char *prv;
    char expected[80] = { 0 };

    strncpy(tmpl, TMP_TEMPLATE, sizeof(TMP_TEMPLATE)+1);

    newfile = mkstemp(tmpl);
    cb_assert(newfile > 0);
    rv = dup2(newfile, STDERR_FILENO);
    cb_assert(rv == STDERR_FILENO);
    rv = close(newfile);
    cb_assert(rv == 0);

    errno = EIO;
    vperror("Old McDonald had a farm.  %s", "EI EIO");

    /* Restore stderr */
    rv = dup2(oldstderr, STDERR_FILENO);
    cb_assert(rv == STDERR_FILENO);


    /* Go read the file */
    efile = fopen(tmpl, "r");
    cb_assert(efile);
    prv = fgets(buf, sizeof(buf), efile);
    cb_assert(prv);
    fclose(efile);

    unlink(tmpl);

    snprintf(expected, sizeof(expected),
             "Old McDonald had a farm.  EI EIO: %s\n", strerror(EIO));

    /*
    fprintf(stderr,
            "\nExpected:  ``%s''"
            "\nGot:       ``%s''\n", expected, buf);
    */

    return strcmp(expected, buf) == 0 ? TEST_PASS : TEST_FAIL;
}


static enum test_return test_issue_72(void) {
    /* SKIP: moxi doesn't handle MEMCACHED_PORT_FILENAME now. */
    return TEST_SKIP;
}

typedef enum test_return (*TEST_FUNC)(void);
struct testcase {
    const char *description;
    TEST_FUNC function;
};

struct testcase testcases[] = {
    { "cache_create", cache_create_test },
    { "cache_constructor", cache_constructor_test },
    { "cache_constructor_fail", cache_fail_constructor_test },
    { "cache_destructor", cache_destructor_test },
    { "cache_reuse", cache_reuse_test },
    { "cache_redzone", cache_redzone_test },
    { "issue_44", test_issue_44 },
    { "issue_72", test_issue_72 },
    { "vperror", test_vperror },
    { "safestrtoul", test_safe_strtoul },
    { "safestrtoull", test_safe_strtoull },
    { "safestrtoll", test_safe_strtoll },
    { "safestrtol", test_safe_strtol },
    { NULL, NULL }
};

int main(void)
{
    int exitcode = 0;
    int ii = 0, num_cases = 0;

    for (num_cases = 0; testcases[num_cases].description; num_cases++) {
        /* Just counting */
    }

    printf("1..%d\n", num_cases);

    for (ii = 0; testcases[ii].description != NULL; ++ii) {
        enum test_return ret = testcases[ii].function();
        fflush(stdout);
        alarm(60);
        if (ret == TEST_SKIP) {
            fprintf(stdout, "ok # SKIP %d - %s\n", ii + 1, testcases[ii].description);
        } else if (ret == TEST_PASS) {
            fprintf(stdout, "ok %d - %s\n", ii + 1, testcases[ii].description);
        } else {
            fprintf(stdout, "not ok %d - %s\n", ii + 1, testcases[ii].description);
            exitcode = 1;
        }
        fflush(stdout);
    }

    return exitcode;
}

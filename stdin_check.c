/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include "stdin_check.h"
#include "log.h"

static void* check_stdin_thread(void* arg)
{
    int ch;

    (void)arg;
    pthread_detach(pthread_self());

    do {
        ch = getc(stdin);
    } while (ch != EOF && ch != '\n' && ch != '\r');

    fprintf(stderr, "%s on stdin.  Exiting\n", (ch == EOF) ? "EOF" : "EOL");
    exit(0);
    /* NOTREACHED */
    return NULL;
}

int stdin_check(void) {
    pthread_t t;
    if (pthread_create(&t, NULL, check_stdin_thread, NULL) != 0) {
        perror("couldn't create stdin checking thread.");
        return -1;
    }

    return 0;
}

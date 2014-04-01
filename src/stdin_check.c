/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "src/config.h"
#include <stdio.h>
#include <stdlib.h>

#include "stdin_check.h"
#include "log.h"

static void check_stdin_thread(void* arg)
{
    int ch;

    (void)arg;

    do {
        ch = getc(stdin);
    } while (ch != EOF && ch != '\n' && ch != '\r');

    fprintf(stderr, "%s on stdin.  Exiting\n", (ch == EOF) ? "EOF" : "EOL");
    exit(0);
}

int stdin_check(void) {
    cb_thread_t t;
    if (cb_create_thread(&t, check_stdin_thread, NULL, 1) != 0) {
        perror("couldn't create stdin checking thread.");
        return -1;
    }

    return 0;
}

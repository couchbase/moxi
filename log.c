/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * moxi logging API
 * Based on log.[ch] from lighttpd source
 * mtaneja@zynga.com
 */

#include "config.h"
#undef write
#include <sys/types.h>

#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>

#include <stdarg.h>
#include <stdio.h>
#include <assert.h>

#include "log.h"
#ifdef HAVE_VALGRIND_VALGRIND_H
#include <valgrind/valgrind.h>
#endif

#ifndef O_LARGEFILE
# define O_LARGEFILE 0
#endif

#define MAX_LOGBUF_LEN 1000

extern volatile uint64_t msec_current_time;

/**
 * open the errorlog
 *
 * we have 3 possibilities:
 * - stderr (default)
 * - syslog
 * - logfile
 *
 * if the open failed, report to the user and die
 */
int log_error_open(moxi_log *mlog) {
    assert(mlog);

    if (mlog->log_mode == ERRORLOG_FILE) {
        const char *logfile = mlog->log_file;

        if (-1 == (mlog->fd = open(logfile, O_APPEND | O_WRONLY | O_CREAT | O_LARGEFILE, 0644))) {
#ifdef HAVE_SYSLOG_H
            fprintf(stderr, "ERROR: opening errorlog '%s' failed. error: %s, Switching to syslog.\n",
                    logfile, strerror(errno));

            mlog->log_mode = ERRORLOG_SYSLOG;
#else
            fprintf(stderr, "ERROR: opening errorlog '%s' failed. error: %s, Switching to stderr.\n",
                    logfile, strerror(errno));
            mlog->log_mode = ERRORLOG_STDERR;
#endif
        }
    }

#ifdef HAVE_SYSLOG_H
    if (mlog->log_mode == ERRORLOG_SYSLOG) {
        openlog(mlog->log_ident, LOG_CONS | LOG_PID, LOG_DAEMON);
    }
#endif

    return 0;
}

/**
 * open the errorlog
 *
 * if the open failed, report to the user and die
 * if no filename is given, use syslog instead
 */
int log_error_cycle(moxi_log *mlog) {
    /* only cycle if we are not in syslog-mode */

    if (mlog->log_mode == ERRORLOG_FILE) {
        const char *logfile = mlog->log_file;
        /* already check of opening time */

        int new_fd;

        log_error_write(mlog, __FILE__, __LINE__, "About to cycle log \n");

        if (-1 == (new_fd = open(logfile, O_APPEND | O_WRONLY | O_CREAT | O_LARGEFILE, 0644))) {
#ifdef HAVE_SYSLOG_H
            /* write to old log */
            log_error_write(mlog, __FILE__, __LINE__,
                            "cycling errorlog '%s' failed: %s. failing back to syslog()",
                            logfile, strerror(errno));

            mlog->log_mode = ERRORLOG_SYSLOG;
#else
            log_error_write(mlog, __FILE__, __LINE__,
                            "cycling errorlog '%s' failed: %s. failing back to stderr",
                            logfile, strerror(errno));

            mlog->log_mode = ERRORLOG_STDERR;
#endif
            close(mlog->fd);
            mlog->fd = -1;

        } else {
            /* ok, new log is open, close the old one */
            close(mlog->fd);
            mlog->fd = new_fd;
            log_error_write(mlog, __FILE__, __LINE__, "Log Cycled \n");
        }
    }

    return 0;
}

int log_error_close(moxi_log *mlog) {
    switch(mlog->log_mode) {
        case ERRORLOG_FILE:
            close(mlog->fd);
            break;
#ifdef HAVE_SYSLOG_H
        case ERRORLOG_SYSLOG:
            closelog();
            break;
#endif
        case ERRORLOG_STDERR:
            break;
    }

    return 0;
}

static inline
void mappend_log(char *logbuf, int *logbuf_used, const char *str) {
    int str_len = strlen(str);
    int used = *logbuf_used;
    if (used + str_len >= MAX_LOGBUF_LEN - 1) {
        str_len = MAX_LOGBUF_LEN - 2 - used;
    }
    if (str_len <= 0) {
        return;
    }
    memcpy(logbuf + used, str, str_len);
    *logbuf_used = used + str_len;
    assert(*logbuf_used < MAX_LOGBUF_LEN);
}

static inline
void mappend_log_int(char *logbuf, int *logbuf_used, int num) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%d", num);
    mappend_log(logbuf, logbuf_used, buf);
}

int log_error_write(moxi_log *mlog, const char *filename, unsigned int line,
                    const char *fmt, ...) {
    va_list ap;
    static char ts_debug_str[255];
    int written = 0;

    char logbuf[MAX_LOGBUF_LEN + 10]; /* scratch buffer */
    int  logbuf_used = 0;             /* length of scratch buffer */
    time_t cur_ts = 0;

    logbuf_used = 0;

    switch(mlog->log_mode) {
        case ERRORLOG_FILE:
        case ERRORLOG_STDERR:
            /* cache the generated timestamp */
            if (!mlog->base_ts) {
                mlog->base_ts = time(NULL);
            }
            cur_ts = mlog->base_ts + (msec_current_time/1000);

            if (cur_ts != mlog->last_generated_debug_ts) {
                memset(ts_debug_str, 0, sizeof(ts_debug_str));
                strftime(ts_debug_str, 254, "%Y-%m-%d %H:%M:%S", localtime(&(cur_ts)));
                mlog->last_generated_debug_ts = cur_ts;
            }

            mappend_log(logbuf, &logbuf_used, ts_debug_str);
            mappend_log(logbuf, &logbuf_used, ": (");
            break;
#ifdef HAVE_SYSLOG_H
        case ERRORLOG_SYSLOG:
            memset(logbuf, 0, MAX_LOGBUF_LEN);
            /* syslog is generating its own timestamps */
            mappend_log(logbuf, &logbuf_used, "(");
            break;
#endif
    }

    mappend_log(logbuf, &logbuf_used, filename);
    mappend_log(logbuf, &logbuf_used, ".");
    mappend_log_int(logbuf, &logbuf_used, line);
    mappend_log(logbuf, &logbuf_used, ") ");

    assert(logbuf_used < MAX_LOGBUF_LEN);

    va_start(ap, fmt);
    logbuf_used +=
        vsnprintf((logbuf + logbuf_used), (MAX_LOGBUF_LEN - logbuf_used - 1), fmt, ap);
    va_end(ap);

    /* vsprintf returns total string length, so no buffer overflow is
     * possible, but we can shoot logbuf_used past MAX_LOGBUF_LEN */
    if (logbuf_used >= MAX_LOGBUF_LEN) {
        logbuf_used = MAX_LOGBUF_LEN - 1;
    }

    if (logbuf_used > 1) {
        logbuf[logbuf_used - 1] = '\n';
    }

    assert(logbuf_used < MAX_LOGBUF_LEN);
    logbuf[logbuf_used] = '\0';

    switch(mlog->log_mode) {
        case ERRORLOG_FILE:
            written = write(mlog->fd, logbuf, logbuf_used);
            break;
        case ERRORLOG_STDERR:
            written = write(STDERR_FILENO, logbuf, logbuf_used);
            break;
#ifdef HAVE_SYSLOG_H
        case ERRORLOG_SYSLOG:
            syslog(LOG_ERR, "%s", logbuf);
            break;
#endif
    }

    return 0;
}




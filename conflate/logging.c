#include <string.h>
#include <stdarg.h>
#include <stdio.h>

#include <libconflate/conflate.h>
#include "conflate_internal.h"

static char* lvl_name(enum conflate_log_level lvl)
{
    char *rv = NULL;

    switch(lvl) {
    case LOG_LVL_FATAL: rv = "FATAL"; break;
    case LOG_LVL_ERROR: rv = "ERROR"; break;
    case LOG_LVL_WARN: rv = "WARN"; break;
    case LOG_LVL_INFO: rv = "INFO"; break;
    case LOG_LVL_DEBUG: rv = "DEBUG"; break;
    }

    return rv;
}

void conflate_stderr_logger(void *userdata, enum conflate_log_level lvl,
                            const char *msg, ...)
{
    char fmt[512];
    va_list ap;

    snprintf(fmt, sizeof(fmt), "%s: %s\n", lvl_name(lvl), msg);
    va_start(ap, msg);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    (void)userdata;
}

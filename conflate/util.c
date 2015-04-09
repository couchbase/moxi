#include <stdio.h>
#include <stdlib.h>
#include <platform/cbassert.h>
#include <string.h>

#include <libconflate/conflate.h>

char* safe_strdup(const char* in) {
    int len = strlen(in);
    char *rv = calloc(len + 1, sizeof(char));
    cb_assert(rv);
    memcpy(rv, in, len);
    return rv;
}

void free_string_list(char **vals)
{
    int i = 0;
    for (i = 0; vals[i]; i++) {
        free(vals[i]);
    }
    free(vals);
}

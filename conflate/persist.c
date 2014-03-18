#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <libconflate/conflate.h>
#include "conflate_internal.h"

kvpair_t* load_kvpairs(conflate_handle_t *handle, const char *filename)
{
    (void)handle;
    (void)filename;
    return NULL;
}

bool save_kvpairs(conflate_handle_t *handle, kvpair_t* kvpair,
                  const char *filename)
{
    (void)handle;
    (void)kvpair;
    (void)filename;
    return true;
}

bool conflate_delete_private(conflate_handle_t *handle,
                             const char *k, const char *filename)
{
    (void)handle;
    (void)k;
    (void)filename;
    return true;
}

bool conflate_save_private(conflate_handle_t *handle,
                           const char *k, const char *v, const char *filename)
{
    (void)handle;
    (void)k;
    (void)v;
    (void)filename;
    return true;
}

char *conflate_get_private(conflate_handle_t *handle,
                           const char *k, const char *filename)
{
    (void)handle;
    (void)k;
    (void)filename;
    return NULL;
}

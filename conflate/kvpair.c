#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <libconflate/conflate.h>

kvpair_t* mk_kvpair(const char* k, char** v)
{
    kvpair_t* rv = calloc(1, sizeof(kvpair_t));
    assert(rv);

    rv->key = safe_strdup(k);
    if (v) {
        int i = 0;
        for (i = 0; v[i]; i++) {
            add_kvpair_value(rv, v[i]);
        }
    } else {
        rv->allocated_values = 4;
        rv->values = calloc(4, sizeof(char*));
        assert(rv->values);
    }

    return rv;
}

void add_kvpair_value(kvpair_t* pair, const char* value)
{
    assert(pair);
    assert(value);

    /* The last item in the values list must be null as it acts a sentinal */
    if (pair->allocated_values == 0 ||
            (pair->used_values + 1) == pair->allocated_values) {
        pair->allocated_values = pair->allocated_values << 1;
        if (pair->allocated_values == 0) {
            pair->allocated_values = 4;
        }

        pair->values = realloc(pair->values,
                               sizeof(char*) * pair->allocated_values);
        assert(pair->values);
    }

    pair->values[pair->used_values++] = safe_strdup(value);
    pair->values[pair->used_values] = 0;
}

void free_kvpair(kvpair_t* pair)
{
    if (pair) {
        free_kvpair(pair->next);
        free(pair->key);
        free_string_list(pair->values);
        free(pair);
    }
}

void walk_kvpair(kvpair_t *pair, void *opaque, kvpair_visitor_t visitor)
{
    bool keep_going = true;
    while (keep_going && pair) {
        keep_going = visitor(opaque,
                             (const char*)pair->key,
                             (const char **)pair->values);
        pair = pair->next;
    }
}

kvpair_t* find_kvpair(kvpair_t* pair, const char* key)
{
    assert(key);

    while (pair && strcmp(pair->key, key) != 0) {
        pair = pair->next;
    }

    return pair;
}

char *get_simple_kvpair_val(kvpair_t *pair, const char *key)
{
    char *rv = NULL;
    kvpair_t *found;
    assert(key);
    found = find_kvpair(pair, key);

    if (found) {
        rv = found->values[0];
    }

    return rv;
}

kvpair_t *dup_kvpair(kvpair_t *pair)
{
    kvpair_t *copy;
    assert(pair);
    copy = mk_kvpair(pair->key, pair->values);
    if (pair->next) {
        copy->next = dup_kvpair(pair->next);
    }
    return copy;
}

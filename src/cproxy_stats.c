/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "src/config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <platform/cbassert.h>
#include "memcached.h"
#include "cproxy.h"
#include "work.h"
#include "log.h"

/* Protocol STATS command handling. */

/* Special STATS value merging rules, instead of the */
/* default to just sum the values.  Note the trailing space. */

char *protocol_stats_keys_first =
    "pid version libevent "
    "ep_dbname ep_storage_type ep_flusher_state ep_warmup_thread ";

char *protocol_stats_keys_smallest =
    "uptime "
    "time "
    "pointer_size "
    "limit_maxbytes "
    "accepting_conns "
    ":chunk_size "
    ":chunk_per_page "
    ":age "; /* TODO: Should age merge be largest, not smallest? */

bool protocol_stats_merge_sum(char *v1, int v1len,
                              char *v2, int v2len,
                              char *out, int outlen);
bool protocol_stats_merge_smallest(char *v1, int v1len,
                                   char *v2, int v2len,
                                   char *out, int outlen);

int count_dot_pair(char *x, int xlen, char *y, int ylen);
int count_dot(char *x, int len);

/* Per-key stats. */

static char *key_stats_key(void *it);
static int key_stats_key_len(void *it);
static int key_stats_len(void *it);
static void *key_stats_get_next(void *it);
static void key_stats_set_next(void *it, void *next);
static void *key_stats_get_prev(void *it);
static void key_stats_set_prev(void *it, void *prev);
static uint64_t key_stats_get_exptime(void *it);
static void key_stats_set_exptime(void *it, uint64_t exptime);

mcache_funcs mcache_key_stats_funcs = {
    .item_key         = key_stats_key,
    .item_key_len     = key_stats_key_len,
    .item_len         = key_stats_len,
    .item_add_ref     = key_stats_add_ref,
    .item_dec_ref     = key_stats_dec_ref,
    .item_get_next    = key_stats_get_next,
    .item_set_next    = key_stats_set_next,
    .item_get_prev    = key_stats_get_prev,
    .item_set_prev    = key_stats_set_prev,
    .item_get_exptime = key_stats_get_exptime,
    .item_set_exptime = key_stats_set_exptime
};

#define MAX_TOKENS     5
#define PREFIX_TOKEN   0
#define NAME_TOKEN     1
#define VALUE_TOKEN    2
#define MERGE_BUF_SIZE 300

bool protocol_stats_merge_line(genhash_t *merger, char *line) {
    int nline;
    token_t tokens[MAX_TOKENS];
    int ntokens;
    char *name;
    int name_len;


    cb_assert(merger != NULL);
    cb_assert(line != NULL);

    nline = strlen(line); /* Ex: "STATS uptime 123455" */
    if (nline <= 0 ||
        nline >= MERGE_BUF_SIZE) {
        return false;
    }

    ntokens = scan_tokens(line, tokens, MAX_TOKENS, NULL);

    if (ntokens != 4) { /* 3 + 1 for the terminal token. */
        return false;
    }

    name = tokens[NAME_TOKEN].value;
    name_len = tokens[NAME_TOKEN].length;
    if (name == NULL ||
        name_len <= 0 ||
        tokens[VALUE_TOKEN].value == NULL ||
        tokens[VALUE_TOKEN].length <= 0 ||
        tokens[VALUE_TOKEN].length >= MERGE_BUF_SIZE) {
        return false;
    }

    return protocol_stats_merge_name_val(merger,
                                         tokens[PREFIX_TOKEN].value,
                                         tokens[PREFIX_TOKEN].length,
                                         name, name_len,
                                         tokens[VALUE_TOKEN].value,
                                         tokens[VALUE_TOKEN].length);
}

/* TODO: The stats merge assumes an ascii upstream. */

bool protocol_stats_merge_name_val(genhash_t *merger,
                                   char *prefix,
                                   int   prefix_len,
                                   char *name,
                                   int   name_len,
                                   char *val,
                                   int   val_len) {

    char *key;
    int key_len;

    cb_assert(merger);
    cb_assert(name);
    cb_assert(val);

    key = name + name_len - 1;     /* Key part for merge rule lookup. */
    while (key >= name && *key != ':') { /* Scan for last colon. */
        key--;
    }
    if (key < name) {
        key = name;
    }
    key_len = name_len - (key - name);
    if (key_len > 0 && key_len < MERGE_BUF_SIZE) {
        char buf_name[MERGE_BUF_SIZE];
        char buf_key[MERGE_BUF_SIZE];
        char buf_val[MERGE_BUF_SIZE];
        char *prev;
        token_t prev_tokens[MAX_TOKENS];
        size_t prev_ntokens;
        bool ok;

        strncpy(buf_name, name, name_len);
        buf_name[name_len] = '\0';

        prev = (char *) genhash_find(merger, buf_name);
        if (prev == NULL) {
            char *hval = malloc(prefix_len + 1 +
                                name_len + 1 +
                                val_len + 1);
            if (hval != NULL) {
                memcpy(hval, prefix, prefix_len);
                hval[prefix_len] = ' ';

                memcpy(hval + prefix_len + 1, name, name_len);
                hval[prefix_len + 1 + name_len] = ' ';

                memcpy(hval + prefix_len + 1 + name_len + 1, val, val_len);
                hval[prefix_len + 1 + name_len + 1 + val_len] = '\0';

                genhash_update(merger, hval + prefix_len + 1, hval);
            }

            return true;
        }

        strncpy(buf_key, key, key_len);
        buf_key[key_len] = '\0';

        if (strstr(protocol_stats_keys_first, buf_key) != NULL) {
            return true;
        }

        prev_ntokens = scan_tokens(prev, prev_tokens, MAX_TOKENS, NULL);
        if (prev_ntokens != 4) {
            return true;
        }

        strncpy(buf_val, val, val_len);
        buf_val[val_len] = '\0';

        if (strstr(protocol_stats_keys_smallest, buf_key) != NULL) {
            ok = protocol_stats_merge_smallest(prev_tokens[VALUE_TOKEN].value,
                                               prev_tokens[VALUE_TOKEN].length,
                                               buf_val, val_len,
                                               buf_val, MERGE_BUF_SIZE);
        } else {
            ok = protocol_stats_merge_sum(prev_tokens[VALUE_TOKEN].value,
                                          prev_tokens[VALUE_TOKEN].length,
                                          buf_val, val_len,
                                          buf_val, MERGE_BUF_SIZE);
        }

        if (ok) {
            int   vlen = strlen(buf_val);
            char *hval = malloc(prefix_len + 1 +
                                name_len + 1 +
                                vlen + 1);
            if (hval != NULL) {
                memcpy(hval, prefix, prefix_len);
                hval[prefix_len] = ' ';

                memcpy(hval + prefix_len + 1, name, name_len);
                hval[prefix_len + 1 + name_len] = ' ';

                strcpy(hval + prefix_len + 1 + name_len + 1, buf_val);
                hval[prefix_len + 1 + name_len + 1 + vlen] = '\0';

                genhash_update(merger, hval + prefix_len + 1, hval);

                free(prev);
            }
        }

        /* Note, if we couldn't merge, then just keep */
        /* the previous value. */

        return true;
    }

    return false;
}

bool protocol_stats_merge_sum(char *v1, int v1len,
                              char *v2, int v2len,
                              char *out, int outlen) {
    int dot = count_dot_pair(v1, v1len, v2, v2len);
    if (dot > 0) {
        float v1f = strtof(v1, NULL);
        float v2f = strtof(v2, NULL);
        sprintf(out, "%f", v1f + v2f);
        return true;
    } else {
        uint64_t v1i = 0;
        uint64_t v2i = 0;

        if (safe_strtoull(v1, &v1i) &&
            safe_strtoull(v2, &v2i)) {
            sprintf(out, "%"PRIu64"", (v1i + v2i));
            return true;
        }
    }

    (void)outlen;
    return false;
}

bool protocol_stats_merge_smallest(char *v1, int v1len,
                                   char *v2, int v2len,
                                   char *out, int outlen) {
    int dot = count_dot_pair(v1, v1len, v2, v2len);
    if (dot > 0) {
        float v1f = strtof(v1, NULL);
        float v2f = strtof(v2, NULL);
        sprintf(out, "%f", (v1f > v2f ? v1f : v2f));
        return true;
    } else {
        uint64_t v1i = 0;
        uint64_t v2i = 0;

        if (safe_strtoull(v1, &v1i) &&
            safe_strtoull(v2, &v2i)) {
            sprintf(out, "%"PRIu64"", (v1i > v2i ? v1i : v2i));
            return true;
        }
    }

    (void)outlen;
    return false;
}

/* Callback to hash table iteration that frees the multiget_entry list.
 */
void protocol_stats_foreach_free(const void *key,
                                 const void *value,
                                 void *user_data) {
    (void)key;
    (void)user_data;
    cb_assert(value != NULL);
    free((void*)value);
}

void protocol_stats_foreach_write(const void *key,
                                  const void *value,
                                  void *user_data) {

    char *line = (char *) value;
    conn *uc = (conn *) user_data;
    int nline;
    cb_assert(line != NULL);
    cb_assert(uc != NULL);
    (void)key;

    nline = strlen(line);
    if (nline > 0) {
        item *it;
        if (settings.verbose > 2) {
            moxi_log_write("%d: cproxy_stats writing: %s\n", uc->sfd, line);
        }

        if (IS_BINARY(uc->protocol)) {
            token_t line_tokens[MAX_TOKENS];
            size_t  line_ntokens = scan_tokens(line, line_tokens, MAX_TOKENS, NULL);

            if (line_ntokens == 4) {
                uint16_t key_len  = line_tokens[NAME_TOKEN].length;
                uint32_t data_len = line_tokens[VALUE_TOKEN].length;

                it = item_alloc("s", 1, 0, 0,
                                sizeof(protocol_binary_response_stats) + key_len + data_len);
                if (it != NULL) {
                    protocol_binary_response_stats *header =
                        (protocol_binary_response_stats *) ITEM_data(it);

                    memset(ITEM_data(it), 0, it->nbytes);

                    header->message.header.response.magic = (uint8_t) PROTOCOL_BINARY_RES;
                    header->message.header.response.opcode = uc->binary_header.request.opcode;
                    header->message.header.response.keylen  = (uint16_t) htons(key_len);
                    header->message.header.response.bodylen = htonl(key_len + data_len);
                    header->message.header.response.opaque  = uc->opaque;

                    memcpy((ITEM_data(it)) + sizeof(protocol_binary_response_stats),
                           line_tokens[NAME_TOKEN].value, key_len);
                    memcpy((ITEM_data(it)) + sizeof(protocol_binary_response_stats) + key_len,
                           line_tokens[VALUE_TOKEN].value, data_len);

                    if (add_conn_item(uc, it)) {
                        add_iov(uc, ITEM_data(it), it->nbytes);

                        if (settings.verbose > 2) {
                            moxi_log_write("%d: cproxy_stats writing binary", uc->sfd);
                            cproxy_dump_header(uc->sfd, ITEM_data(it));
                        }

                        return;
                    }

                    item_remove(it);
                }
            }

            return;
        }

        it = item_alloc("s", 1, 0, 0, nline + 2);
        if (it != NULL) {
            strncpy(ITEM_data(it), line, nline);
            strncpy(ITEM_data(it) + nline, "\r\n", 2);

            if (add_conn_item(uc, it)) {
                add_iov(uc, ITEM_data(it), nline + 2);
                return;
            }

            item_remove(it);
        }
    }
}

int count_dot_pair(char *x, int xlen, char *y, int ylen) {
    int xdot = count_dot(x, xlen);
    int ydot = count_dot(y, ylen);

    return (xdot > ydot ? xdot : ydot);
}

int count_dot(char *x, int len) { /* Number of '.' chars in a string. */
    int dot = 0;
    char *end;
    for (end = x + len; x < end; x++) {
        if (*x == '.')
            dot++;
    }

    return dot;
}

/* ---------------------------------------- */

void cproxy_reset_stats_td(proxy_stats_td *pstd) {
    int j;

    cb_assert(pstd);

    cproxy_reset_stats(&pstd->stats);

    for (j = 0; j < STATS_CMD_TYPE_last; j++) {
        int k;
        for (k = 0; k < STATS_CMD_last; k++) {
            cproxy_reset_stats_cmd(&pstd->stats_cmd[j][k]);
        }
    }
}

void cproxy_reset_stats(proxy_stats *ps) {
    cb_assert(ps);

    /* Only clear the tot_xxx stats, not the num_xxx ones. */

    ps->tot_upstream = 0;
    ps->tot_downstream_conn = 0;
    ps->tot_downstream_conn_acquired = 0;
    ps->tot_downstream_conn_released = 0;
    ps->tot_downstream_released = 0;
    ps->tot_downstream_reserved = 0;
    ps->tot_downstream_reserved_time = 0;
    ps->max_downstream_reserved_time = 0;
    ps->tot_downstream_freed = 0;
    ps->tot_downstream_quit_server = 0;
    ps->tot_downstream_max_reached = 0;
    ps->tot_downstream_create_failed = 0;
    ps->tot_downstream_connect_started = 0;
    ps->tot_downstream_connect_wait = 0;
    ps->tot_downstream_connect = 0;
    ps->tot_downstream_connect_failed = 0;
    ps->tot_downstream_connect_timeout = 0;
    ps->tot_downstream_connect_interval = 0;
    ps->tot_downstream_connect_max_reached = 0;
    ps->tot_downstream_waiting_errors = 0;
    ps->tot_downstream_auth = 0;
    ps->tot_downstream_auth_failed = 0;
    ps->tot_downstream_bucket = 0;
    ps->tot_downstream_bucket_failed = 0;
    ps->tot_downstream_propagate_failed = 0;
    ps->tot_downstream_close_on_upstream_close = 0;
    ps->tot_downstream_conn_queue_timeout = 0;
    ps->tot_downstream_conn_queue_add = 0;
    ps->tot_downstream_conn_queue_remove = 0;
    ps->tot_downstream_timeout = 0;
    ps->tot_wait_queue_timeout = 0;
    ps->tot_assign_downstream = 0;
    ps->tot_assign_upstream = 0;
    ps->tot_assign_recursion = 0;
    ps->tot_reset_upstream_avail = 0;
    ps->tot_retry = 0;
    ps->tot_retry_time = 0;
    ps->max_retry_time = 0;
    ps->tot_retry_vbucket = 0;
    ps->tot_upstream_paused = 0;
    ps->tot_upstream_unpaused = 0;
    ps->tot_multiget_keys = 0;
    ps->tot_multiget_keys_dedupe = 0;
    ps->tot_multiget_bytes_dedupe = 0;
    ps->tot_optimize_sets = 0;
    ps->err_oom = 0;
    ps->err_upstream_write_prep = 0;
    ps->err_downstream_write_prep = 0;
    ps->tot_cmd_time = 0;
    ps->tot_cmd_count = 0;
}

void cproxy_reset_stats_cmd(proxy_stats_cmd *sc) {
    cb_assert(sc);
    memset(sc, 0, sizeof(proxy_stats_cmd));
}

/* ------------------------------------------------- */

key_stats *find_key_stats(proxy_td *ptd, char *key, int key_len,
                          uint64_t msec_time) {
    key_stats *ks;
    cb_assert(ptd);
    cb_assert(key);
    cb_assert(key_len > 0);

    ks = mcache_get(&ptd->key_stats, key, key_len, msec_time);
    if (ks == NULL) {
        ks = calloc(1, sizeof(key_stats));
        if (ks != NULL) {
            memcpy(ks->key, key, key_len);
            ks->key[key_len] = '\0';
            ks->refcount = 1;
            ks->added_at = msec_time;

            mcache_set(&ptd->key_stats, ks,
                       msec_time +
                       ptd->behavior_pool.base.key_stats_lifespan,
                       true, false);
        }
    }

    return ks;
}

void touch_key_stats(proxy_td *ptd, char *key, int key_len,
                     uint64_t msec_time,
                     enum_stats_cmd_type cmd_type,
                     enum_stats_cmd cmd,
                     int delta_seen,
                     int delta_hits,
                     int delta_misses,
                     int delta_read_bytes,
                     int delta_write_bytes) {
    key_stats *ks = find_key_stats(ptd, key, key_len, msec_time);
    if (ks != NULL) {
        proxy_stats_cmd *psc = &ks->stats_cmd[cmd_type][cmd];
        if (psc != NULL) {
            psc->seen        += delta_seen;
            psc->hits        += delta_hits;
            psc->misses      += delta_misses;
            psc->read_bytes  += delta_read_bytes;
            psc->write_bytes += delta_write_bytes;
        }

        key_stats_dec_ref(ks);
    }
}

/* ------------------------------------------------- */

static char *key_stats_key(void *it) {
    key_stats *i = it;
    cb_assert(i);
    return i->key;
}

static int key_stats_key_len(void *it) {
    key_stats *i = it;
    cb_assert(i);
    return strlen(i->key);
}

static int key_stats_len(void *it) {
    (void)it;
    return sizeof(key_stats);
}

void key_stats_add_ref(void *it) {
    key_stats *i = it;
    if (i != NULL) {
        i->refcount++;
    }
}

void key_stats_dec_ref(void *it) {
    key_stats *i = it;
    if (i != NULL) {
        i->refcount--;
        if (i->refcount <= 0) {
            free(it);
        }
    }
}

static void *key_stats_get_next(void *it) {
    key_stats *i = it;
    cb_assert(i);
    return i->next;
}

static void key_stats_set_next(void *it, void *next) {
    key_stats *i = it;
    cb_assert(i);
    i->next = (key_stats *) next;
}

static void *key_stats_get_prev(void *it) {
    key_stats *i = it;
    cb_assert(i);
    return i->prev;
}

static void key_stats_set_prev(void *it, void *prev) {
    key_stats *i = it;
    cb_assert(i);
    i->prev = (key_stats *) prev;
}

static uint64_t key_stats_get_exptime(void *it) {
    key_stats *i = it;
    cb_assert(i);
    return i->exptime;
}

static void key_stats_set_exptime(void *it, uint64_t exptime) {
    key_stats *i = it;
    cb_assert(i);
    i->exptime = exptime;
}


/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "src/config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <platform/cbassert.h>
#include <math.h>
#include "memcached.h"
#include "cproxy.h"
#include "work.h"
#include "log.h"

/* Internal declarations. */

static protocol_binary_request_noop req_noop = {
    .bytes = {0}
};

#define CMD_TOKEN  0
#define KEY_TOKEN  1
#define MAX_TOKENS 9

/* A2B means ascii-to-binary (or, ascii upstream and binary downstream). */

struct A2BSpec {
    char *line;

    protocol_binary_command cmd;
    protocol_binary_command cmdq;

    int     size;         /* Number of bytes in request header. */
    token_t tokens[MAX_TOKENS];
    int     ntokens;
    bool    noreply_allowed;
    int     num_optional; /* Number of optional arguments in cmd. */
    bool    broadcast;    /* True if cmd does scatter/gather. */
};

/* The a2b_specs are immutable after init. */

/* The arguments are carefully named with unique first characters. */

struct A2BSpec a2b_specs[] = {
    { .line = "set <key> <flags> <exptime> <bytes> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_SET,
      .cmdq = PROTOCOL_BINARY_CMD_SETQ,
      /* The size should be... */
      /*   sizeof(protocol_binary_request_header) [24 bytes] + */
      /*   sizeof(protocol_binary_request_set.message.body) [8 bytes] */
      .size = sizeof(protocol_binary_request_header) + 8
    },
    { .line = "add <key> <flags> <exptime> <bytes> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_ADD,
      .cmdq = PROTOCOL_BINARY_CMD_ADDQ,
      /* The size should be... */
      /*   sizeof(protocol_binary_request_header) [24 bytes] + */
      /*   sizeof(protocol_binary_request_add.message.body) [8 bytes] */
      .size = sizeof(protocol_binary_request_header) + 8
    },
    { .line = "replace <key> <flags> <exptime> <bytes> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_REPLACE,
      .cmdq = PROTOCOL_BINARY_CMD_REPLACEQ,
      /* The size should be... */
      /*   sizeof(protocol_binary_request_header) [24 bytes] + */
      /*   sizeof(protocol_binary_request_replace.message.body) [8 bytes] */
      .size = sizeof(protocol_binary_request_header) + 8
    },
    { .line = "append <key> <skip_flags> <skip_exptime> <bytes> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_APPEND,
      .cmdq = PROTOCOL_BINARY_CMD_APPENDQ,
      .size = sizeof(protocol_binary_request_append)
    },
    { .line = "prepend <key> <skip_flags> <skip_exptime> <bytes> [noreply]" ,
      .cmd  = PROTOCOL_BINARY_CMD_PREPEND,
      .cmdq = PROTOCOL_BINARY_CMD_PREPENDQ,
      .size = sizeof(protocol_binary_request_prepend)
    },
    { .line = "cas <key> <flags> <exptime> <bytes> <cas> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_SET,
      .cmdq = PROTOCOL_BINARY_CMD_SETQ,
      /* The size should be... */
      /*   sizeof(protocol_binary_request_header) [24 bytes] + */
      /*   sizeof(protocol_binary_request_set.message.body) [8 bytes] */
      .size = sizeof(protocol_binary_request_header) + 8
    },
    { .line = "delete <key> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_DELETE,
      .cmdq = PROTOCOL_BINARY_CMD_DELETEQ,
      .size = sizeof(protocol_binary_request_delete)
    },
    { .line = "incr <key> <value> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_INCREMENT,
      .cmdq = PROTOCOL_BINARY_CMD_INCREMENTQ,
      /* The size should be... */
      /*   sizeof(protocol_binary_request_header) [24 bytes] + */
      /*   sizeof(protocol_binary_request_incr.message.body) [20 bytes] */
      .size = sizeof(protocol_binary_request_header) + 20
    },
    { .line = "decr <key> <value> [noreply]",
      .cmd  = PROTOCOL_BINARY_CMD_DECREMENT,
      .cmdq = PROTOCOL_BINARY_CMD_DECREMENTQ,
      /* The size should be... */
      /*   sizeof(protocol_binary_request_header) [24 bytes] + */
      /*   sizeof(protocol_binary_request_decr.message.body) [20 bytes] */
      .size = sizeof(protocol_binary_request_header) + 20
    },
    { .line = "flush_all [xpiration] [noreply]", /* TODO: noreply tricky here. */
      .cmd  = PROTOCOL_BINARY_CMD_FLUSH,
      .cmdq = PROTOCOL_BINARY_CMD_FLUSHQ,
      /* The size should be... */
      /*   sizeof(protocol_binary_request_header) [24 bytes] + */
      /*   sizeof(protocol_binary_request_flush.message.body) [4 bytes] */
      .size = sizeof(protocol_binary_request_header) + 4,
      .broadcast = true
    },
    { .line = "get <key>*", /* Multi-key GET/GETS */
      .cmd  = PROTOCOL_BINARY_CMD_GETKQ,
      .cmdq = -1,
      .size = sizeof(protocol_binary_request_header)
    },
    { .line = "get <key>", /* Single-key GET/GETS. */
      .cmd  = PROTOCOL_BINARY_CMD_GETK,
      .cmdq = -1,
      .size = sizeof(protocol_binary_request_header)
    },
    { .line = "stats [args]*",
      .cmd  = PROTOCOL_BINARY_CMD_STAT,
      .cmdq = -1,
      .size = sizeof(protocol_binary_request_stats),
      .broadcast = true
    },
    { .line = "version",
      .cmd  = PROTOCOL_BINARY_CMD_VERSION,
      .cmdq = -1,
      .size = sizeof(protocol_binary_request_version)
    },
    { .line = "getl <key> <xpiration>", /* Single-key GETL. */
      .cmd  = PROTOCOL_BINARY_CMD_GETL,
      .cmdq = -1,
      .size = sizeof(protocol_binary_request_header) + 4
    },
    { .line = "unl <key> <cas>", /* Single-key UNL. */
      .cmd  = PROTOCOL_BINARY_CMD_UNL,
      .cmdq = -1,
      .size = sizeof(protocol_binary_request_header)
    },
    { .line = "touch <key> <xpiration>",
      .cmd  = PROTOCOL_BINARY_CMD_TOUCH,
      .cmdq = -1,
      /* The size should be... */
      /*   sizeof(protocol_binary_request_header) [24 bytes] + */
      /*   sizeof(protocol_binary_request_touch.message.body) [4 bytes] */
      .size = sizeof(protocol_binary_request_header) + 4,
    },
    { .line = 0 } /* NULL sentinel. */
};

/* These are immutable after init. */

struct A2BSpec *a2b_spec_map[0x100] = {0}; /* Lookup table by A2BSpec->cmd. */
int             a2b_size_max = 0;          /* Max header + extra frame bytes. */

int a2b_fill_request(short    cmd,
                     token_t *cmd_tokens,
                     int      cmd_ntokens,
                     bool     noreply,
                     protocol_binary_request_header *header,
                     uint8_t **out_key,
                     uint16_t *out_keylen,
                     uint8_t  *out_extlen);

bool a2b_fill_request_token(struct A2BSpec *spec,
                            int      cur_token,
                            token_t *cmd_tokens,
                            int      cmd_ntokens,
                            protocol_binary_request_header *header,
                            uint8_t **out_key,
                            uint16_t *out_keylen,
                            uint8_t  *out_extlen);

void a2b_process_downstream_response(conn *c);

int a2b_multiget_start(conn *c, char *cmd, int cmd_len);
int a2b_multiget_skey(conn *c, char *skey, int skey_len, int vbucket, int key_index);
int a2b_multiget_end(conn *c);

void a2b_set_opaque(conn *c, protocol_binary_request_header *header, bool noreply);

bool a2b_not_my_vbucket(conn *uc, conn *c,
                        protocol_binary_response_header *header);

void cproxy_init_a2b() {
    int i = 0;

    memset(&req_noop, 0, sizeof(req_noop));

    req_noop.message.header.request.magic    = PROTOCOL_BINARY_REQ;
    req_noop.message.header.request.opcode   = PROTOCOL_BINARY_CMD_NOOP;
    req_noop.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;

    /* Run through the a2b_specs to populate the a2b_spec_map. */

    while (true) {
        struct A2BSpec *spec = &a2b_specs[i];
        int j;
        int noreply_index;

        if (spec->line == NULL) {
            break;
        }

        spec->ntokens = scan_tokens(spec->line,
                                    spec->tokens,
                                    MAX_TOKENS, NULL);
        cb_assert(spec->ntokens > 1);

        noreply_index = spec->ntokens - 2;
        if (spec->tokens[noreply_index].value &&
            strcmp(spec->tokens[noreply_index].value,
                   "[noreply]") == 0) {
            spec->noreply_allowed = true;
        } else {
            spec->noreply_allowed = false;
        }

        spec->num_optional = 0;
        for (j = 0; j < spec->ntokens; j++) {
            if (spec->tokens[j].value &&
                spec->tokens[j].value[0] == '[') {
                spec->num_optional++;
            }
        }

        if (a2b_size_max < spec->size) {
            a2b_size_max = spec->size;
        }

        cb_assert(spec->cmd < (sizeof(a2b_spec_map) /
                            sizeof(struct A2BSpec *)));

        a2b_spec_map[spec->cmd] = spec;

        i = i + 1;
    }
}

int a2b_fill_request(short    cmd,
                     token_t *cmd_tokens,
                     int      cmd_ntokens,
                     bool     noreply,
                     protocol_binary_request_header *header,
                     uint8_t **out_key,
                     uint16_t *out_keylen,
                     uint8_t  *out_extlen) {
    struct A2BSpec *spec;
    cb_assert(header);
    cb_assert(cmd_tokens);
    cb_assert(cmd_ntokens > 1);
    cb_assert(cmd_tokens[CMD_TOKEN].value);
    cb_assert(cmd_tokens[CMD_TOKEN].length > 0);
    cb_assert(out_key);
    cb_assert(out_keylen);
    cb_assert(out_extlen);

    spec = a2b_spec_map[cmd];
    if (spec != NULL) {
        if (cmd_ntokens >= (spec->ntokens - spec->num_optional) &&
            cmd_ntokens <= (spec->ntokens)) {
            int i;

            header->request.magic = PROTOCOL_BINARY_REQ;

            if (noreply) {
                cb_assert(spec->cmd != (protocol_binary_command) -1);

                header->request.opcode = spec->cmdq;
                header->request.opaque = htonl(OPAQUE_IGNORE_REPLY);

                if (settings.verbose > 2) {
                    moxi_log_write("a2b_fill_request OPAQUE_IGNORE_REPLY, cmdq: %x\n",
                            spec->cmdq);
                }
            } else {
                header->request.opcode = spec->cmd;
            }

            /* Start at 1 to skip the CMD_TOKEN. */

            for (i = 1; i < cmd_ntokens - 1; i++) {
                if (a2b_fill_request_token(spec, i,
                                           cmd_tokens, cmd_ntokens,
                                           header,
                                           out_key,
                                           out_keylen,
                                           out_extlen) == false) {
                    return 0;
                }
            }

            return spec->size; /* Success. */
        }
    } else {
        if (settings.verbose > 2) {
            moxi_log_write("a2b_fill_request unknown cmd: %x\n", cmd);
        }
    }

    return 0;
}

bool a2b_fill_request_token(struct A2BSpec *spec,
                            int      cur_token,
                            token_t *cmd_tokens,
                            int      cmd_ntokens,
                            protocol_binary_request_header *header,
                            uint8_t **out_key,
                            uint16_t *out_keylen,
                            uint8_t  *out_extlen) {

    uint64_t delta;
    char t;

    (void)cmd_ntokens;
    cb_assert(header);
    cb_assert(spec);
    cb_assert(spec->tokens);
    cb_assert(spec->ntokens > 1);
    cb_assert(spec->tokens[cur_token].value);
    cb_assert(cur_token > 0);
    cb_assert(cur_token < cmd_ntokens);
    cb_assert(cur_token < spec->ntokens);

    if (settings.verbose > 2) {
        moxi_log_write("a2b_fill_request_token %s\n",
                spec->tokens[cur_token].value);
    }

    t = spec->tokens[cur_token].value[1];
    switch (t) {
    case 'k': /* key */
        cb_assert(out_key);
        cb_assert(out_keylen);
        *out_key    = (uint8_t *) cmd_tokens[cur_token].value;
        *out_keylen = (uint16_t)  cmd_tokens[cur_token].length;
        header->request.keylen =
            htons((uint16_t) cmd_tokens[cur_token].length);
        break;

    case 'v': /* value (for incr/decr) */
        delta = 0;
        if (safe_strtoull(cmd_tokens[cur_token].value, &delta)) {
            protocol_binary_request_incr *req;
            cb_assert(out_extlen);

            header->request.extlen   = *out_extlen = 20;
            header->request.datatype = PROTOCOL_BINARY_RAW_BYTES;

            req = (protocol_binary_request_incr *) header;

            req->message.body.delta = htonll(delta);
            req->message.body.initial = 0;
            req->message.body.expiration = 0xffffffff;
        } else {
            /* TODO: Send back better error. */
            return false;
        }
        break;

    case 'x': { /* xpiration (for flush_all) */
        int32_t exptime_int = 0;
        time_t  exptime = 0;

        if (safe_strtol(cmd_tokens[cur_token].value, &exptime_int)) {
            /* Ubuntu 8.04 breaks when I pass exptime to safe_strtol */
            protocol_binary_request_flush *req;
            exptime = exptime_int;

            header->request.extlen   = *out_extlen = 4;
            header->request.datatype = PROTOCOL_BINARY_RAW_BYTES;

            req = (protocol_binary_request_flush *) header;

            req->message.body.expiration = htonl(exptime);
        }
        break;
    }

    case 'a': /* args (for stats) */
        cb_assert(out_key);
        cb_assert(out_keylen);
        *out_key    = (uint8_t *) cmd_tokens[cur_token].value;
        *out_keylen = (uint16_t)  cmd_tokens[cur_token].length;
        header->request.keylen =
            htons((uint16_t) cmd_tokens[cur_token].length);
        break;

   case 'c': { /* cas value for unl */
        uint64_t cas = 0;
        if (safe_strtoull(cmd_tokens[cur_token].value, &cas)) {
            header->request.cas = cas;
        }
        break;
   }

    /* The noreply was handled in a2b_fill_request(). */

    /* case 'n': // noreply */

    /* The above are handled by looking at the item struct. */

    /* case 'f': // FALLTHRU, flags */
    /* case 'e': // FALLTHRU, exptime */
    /* case 'b': // FALLTHRU, bytes */
    /* case 's': // FALLTHRU, skip_xxx */
    /* case 'c': // FALLTHRU, cas */

    default:
        break;
    }

    return true;
}

/* Called when we receive a binary response header from
 * a downstream server, via try_read_command()/drive_machine().
 */
void cproxy_process_a2b_downstream(conn *c) {
    protocol_binary_response_header *header;
    int extlen;
    int keylen;
    uint32_t bodylen;

    cb_assert(c != NULL);
    cb_assert(c->cmd >= 0);
    cb_assert(c->next == NULL);
    cb_assert(c->item == NULL);
    cb_assert(IS_BINARY(c->protocol));
    cb_assert(IS_PROXY(c->protocol));

    /* Snapshot rcurr, because the caller, try_read_command(), changes it. */

    c->cmd_start = c->rcurr;

    header = (protocol_binary_response_header *) &c->binary_header;
    header->response.status = (uint16_t) ntohs(header->response.status);

    cb_assert(header->response.magic == (uint8_t) PROTOCOL_BINARY_RES);
    cb_assert(header->response.opcode == c->cmd);

    process_bin_noreply(c); /* Map quiet c->cmd values into non-quiet. */

    extlen = header->response.extlen;
    keylen = header->response.keylen;
    bodylen = header->response.bodylen;

    cb_assert(bodylen >= (uint32_t) keylen + extlen);

    /* Our approach is to read everything we can before */
    /* getting into big switch/case statements for the */
    /* actual processing. */

    /* If status is non-zero (an err code), then bodylen should be small. */
    /* If status is 0, then bodylen might be for a huge item during */
    /* a GET family of response. */

    /* If bodylen > extlen + keylen, then we should nread */
    /* the ext+key and set ourselves up for a later item nread. */

    /* We overload the meaning of the conn substates... */
    /* - bin_reading_get_key means do nread for ext and key data. */
    /* - bin_read_set_value means do nread for item data. */

    if (settings.verbose > 2) {
        moxi_log_write("<%d cproxy_process_a2b_downstream %x %d %d %u\n",
                c->sfd, c->cmd, extlen, keylen, bodylen);
    }

    if (keylen > 0 || extlen > 0) {
        /* One reason we reach here is during a */
        /* GET/GETQ/GETK/GETKQ hit response, because extlen */
        /* will be > 0 for the flags. */

        /* Also, we reach here during a GETK miss response, since */
        /* keylen will be > 0.  Oddly, a GETK miss response will have */
        /* a non-zero status of PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, */
        /* but won't have any extra error message string. */

        /* Also, we reach here during a STAT response, with */
        /* keylen > 0, extlen == 0, and bodylen == keylen. */

        cb_assert(c->cmd == PROTOCOL_BINARY_CMD_GET ||
               c->cmd == PROTOCOL_BINARY_CMD_GETK ||
               c->cmd == PROTOCOL_BINARY_CMD_GETL ||
               c->cmd == PROTOCOL_BINARY_CMD_STAT);

        bin_read_key(c, bin_reading_get_key, extlen);
    } else {
        cb_assert(keylen == 0 && extlen == 0);

        if (bodylen > 0) {
            /* We reach here on error response, version response, */
            /* or incr/decr responses, which all have only (relatively */
            /* small) body bytes, and with no ext bytes and no key bytes. */

            /* For example, error responses will have 0 keylen, */
            /* 0 extlen, with an error message string for the body. */

            /* We'll just reuse the key-reading code path, rather */
            /* than allocating an item. */

            cb_assert(header->response.status != 0 ||
                   c->cmd == PROTOCOL_BINARY_CMD_VERSION ||
                   c->cmd == PROTOCOL_BINARY_CMD_INCREMENT ||
                   c->cmd == PROTOCOL_BINARY_CMD_DECREMENT ||
                   c->cmd == PROTOCOL_BINARY_CMD_UNL);

            bin_read_key(c, bin_reading_get_key, bodylen);
        } else {
            cb_assert(keylen == 0 && extlen == 0 && bodylen == 0);

            /* We have the entire response in the header, */
            /* such as due to a general success response, */
            /* including a no-op response. */

            a2b_process_downstream_response(c);
        }
    }
}

/* We reach here after nread'ing a ext+key or item.
 */
void cproxy_process_a2b_downstream_nread(conn *c) {
    downstream *d;
    protocol_binary_response_header *header;
    int extlen;
    int keylen;
    uint32_t bodylen;

    cb_assert(c != NULL);
    cb_assert(c->cmd >= 0);
    cb_assert(c->next == NULL);
    cb_assert(c->cmd_start != NULL);
    cb_assert(IS_BINARY(c->protocol));
    cb_assert(IS_PROXY(c->protocol));

    d = c->extra;
    cb_assert(d);

    header = (protocol_binary_response_header *) &c->binary_header;
    extlen = header->response.extlen;
    keylen = header->response.keylen;
    bodylen = header->response.bodylen;

    if (settings.verbose > 2) {
        moxi_log_write("<%d cproxy_process_a2b_downstream_nread %d %d, cmd %x %d %d\n",
                c->sfd, c->ileft, c->isize, c->cmd, c->substate,
                header->response.status);
    }

    if (c->substate == bin_reading_get_key &&
        header->response.status == 0 &&
        (c->cmd == PROTOCOL_BINARY_CMD_GET ||
         c->cmd == PROTOCOL_BINARY_CMD_GETK ||
         c->cmd == PROTOCOL_BINARY_CMD_STAT ||
         c->cmd == PROTOCOL_BINARY_CMD_GETL)) {

        item *it;
        char *key;
        int vlen;
        int flags = 0;

        if (settings.verbose > 2) {
            moxi_log_write("<%d cproxy_process_a2b_downstream_nread %d %d %x get/getk/stat\n",
                    c->sfd, c->ileft, c->isize, c->cmd);
        }

        cb_assert(c->item == NULL);

        /* Alloc an item and continue with an item nread. */
        /* We item_alloc() even if vlen is 0, so that later */
        /* code can assume an item exists. */

        key = binary_get_key(c);
        vlen = bodylen - (keylen + extlen);

        cb_assert(key);
        cb_assert(vlen >= 0);

        if (c->cmd == PROTOCOL_BINARY_CMD_GET ||
            c->cmd == PROTOCOL_BINARY_CMD_GETK ||
            c->cmd == PROTOCOL_BINARY_CMD_GETL) {
            protocol_binary_response_get *response_get =
                (protocol_binary_response_get *) binary_get_request(c);

            cb_assert(extlen == sizeof(response_get->message.body));

            flags = ntohl(response_get->message.body.flags);
        }

        it = item_alloc(key, keylen, flags, 0, vlen + 2);
        if (it != NULL) {
            uint64_t cas = CPROXY_NOT_CAS;
            conn *uc;

            c->item = it;
            c->ritem = ITEM_data(it);
            c->rlbytes = vlen;
            c->substate = bin_read_set_value;

            uc = d->upstream_conn;
            if (uc != NULL &&
                uc->cmd_start != NULL &&
                (strncmp(uc->cmd_start, "gets ", 5) == 0 ||
                 strncmp(uc->cmd_start, "getl ", 5) == 0)) {
                cas = header->response.cas;
            }

            ITEM_set_cas(it, cas);

            conn_set_state(c, conn_nread);
        } else {
            d->ptd->stats.stats.err_oom++;
            cproxy_close_conn(c);
        }
    } else {
        a2b_process_downstream_response(c);
    }
}

static void a2b_out_error(conn *uc, uint16_t status) {
    switch (status) {
    case PROTOCOL_BINARY_RESPONSE_SUCCESS:
        out_string(uc, "OK");
        break;
    case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
        out_string(uc, "NOT_FOUND");
        break;
    case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
        out_string(uc, "EXISTS");
        break;
    case PROTOCOL_BINARY_RESPONSE_E2BIG:
        out_string(uc, "SERVER_ERROR a2b e2big");
        break;
    case PROTOCOL_BINARY_RESPONSE_EINVAL:
        out_string(uc, "SERVER_ERROR a2b einval");
        break;
    case PROTOCOL_BINARY_RESPONSE_NOT_STORED:
        out_string(uc, "NOT_STORED");
        break;
    case PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL:
        out_string(uc, "SERVER_ERROR a2b delta_badval");
        break;
    case PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET:
        out_string(uc, "SERVER_ERROR a2b not_my_vbucket");
        break;
    case PROTOCOL_BINARY_RESPONSE_AUTH_ERROR:
        out_string(uc, "SERVER_ERROR a2b auth_error");
        break;
    case PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE:
        out_string(uc, "SERVER_ERROR a2b auth_continue");
        break;
    case PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND:
        out_string(uc, "SERVER_ERROR a2b unknown");
        break;
    case PROTOCOL_BINARY_RESPONSE_ENOMEM:
        out_string(uc, "SERVER_ERROR a2b out of memory");
        break;
    case PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED:
        out_string(uc, "SERVER_ERROR a2b not supported");
        break;
    case PROTOCOL_BINARY_RESPONSE_EINTERNAL:
        out_string(uc, "SERVER_ERROR a2b einternal");
        break;
    case PROTOCOL_BINARY_RESPONSE_EBUSY:
        out_string(uc, "SERVER_ERROR a2b ebusy");
        break;
    case PROTOCOL_BINARY_RESPONSE_ETMPFAIL:
        out_string(uc, "SERVER_ERROR temporary failure");
        break;
    default:
        out_string(uc, "SERVER_ERROR a2b error");
        break;
    }
}

/* Invoked when we have read a complete downstream binary response,
 * including header, ext, key, and item data, as appropriate.
 */
void a2b_process_downstream_response(conn *c) {
    protocol_binary_response_header *header;
    uint32_t extlen;
    uint32_t keylen;
    uint32_t bodylen;
    uint16_t status;
    downstream *d;
    item *it;
    conn *uc;

    cb_assert(c != NULL);
    cb_assert(c->cmd >= 0);
    cb_assert(c->next == NULL);
    cb_assert(c->cmd_start != NULL);
    cb_assert(IS_BINARY(c->protocol));
    cb_assert(IS_PROXY(c->protocol));

    header = (protocol_binary_response_header *) &c->binary_header;

    extlen = header->response.extlen;
    keylen = header->response.keylen;
    bodylen = header->response.bodylen;
    status = header->response.status;

    if (settings.verbose > 2) {
        moxi_log_write("<%d cproxy_process_a2b_downstream_response, cmd: %x, item: %d, status: %d\n",
                c->sfd, c->cmd, (c->item != NULL), status);
    }

    /* We reach here when we have the entire response, */
    /* including header, ext, key, and possibly item data. */
    /* Now we can get into big switch/case processing. */

    d = c->extra;
    cb_assert(d != NULL);
    cb_assert(d->ptd != NULL);
    cb_assert(d->ptd->proxy != NULL);

    it = c->item;

    /* Clear c->item because we either move it to the upstream or */
    /* item_remove() it on error. */

    c->item = NULL;

    if (cproxy_binary_ignore_reply(c, header, it)) {
        return;
    }

    uc = d->upstream_conn;

    /* Handle not-my-vbucket error response. */

    if (status == PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET) {
        cb_assert(it == NULL);

        if (a2b_not_my_vbucket(uc, c, header)) {
            return;
        }
    }

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_GETK:
        if (settings.verbose > 2) {
            moxi_log_write("%d: cproxy_process_a2b_downstream_response GETK "
                    "noreply: %d\n", c->sfd, c->noreply);
        }

        if (c->noreply == false) {
            /* Single-key GET/GETS. */

            if (status == 0) {
                cb_assert(it != NULL);
                cb_assert(it->nbytes >= 2);
                cb_assert(keylen > 0);
                cb_assert(extlen > 0);

                if (bodylen >= keylen + extlen) {
                    *(ITEM_data(it) + it->nbytes - 2) = '\r';
                    *(ITEM_data(it) + it->nbytes - 1) = '\n';

                    multiget_ascii_downstream_response(d, it);
                } else {
                    cb_assert(false); /* TODO. */
                }

                item_remove(it);
            }

            conn_set_state(c, conn_pause);

            cproxy_update_event_write(d, uc);

            return;
        }

        /* Multi-key GET/GETS. */

        /* We should keep processing for a non-quiet */
        /* terminating response (NO-OP). */

        conn_set_state(c, conn_new_cmd);

        if (status != 0) {
            cb_assert(it == NULL);

            if (status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT) {
                return; /* Swallow miss response. */
            }

            /* TODO: Handle error case.  Should we pause the conn */
            /*       or keep looking for more responses? */

            cb_assert(false);
            return;
        }

        cb_assert(status == 0);
        cb_assert(it != NULL);
        cb_assert(it->nbytes >= 2);
        cb_assert(keylen > 0);
        cb_assert(extlen > 0);

        if (bodylen >= keylen + extlen) {
            *(ITEM_data(it) + it->nbytes - 2) = '\r';
            *(ITEM_data(it) + it->nbytes - 1) = '\n';

            multiget_ascii_downstream_response(d, it);
        } else {
            cb_assert(false); /* TODO. */
        }

        item_remove(it);
        break;
    case PROTOCOL_BINARY_CMD_GETL:
        if (settings.verbose > 2) {
            moxi_log_write("%d: cproxy_process_a2b_downstream_response GETL "
                    "noreply: %d\n", c->sfd, c->noreply);
        }

        if (c->noreply == false) {
            switch (status) {
                case PROTOCOL_BINARY_RESPONSE_SUCCESS:
                    cb_assert(it != NULL);
                    cb_assert(it->nbytes >= 2);
                    cb_assert(extlen > 0);

                    if (bodylen >= keylen + extlen) {
                        *(ITEM_data(it) + it->nbytes - 2) = '\r';
                        *(ITEM_data(it) + it->nbytes - 1) = '\n';

                        cproxy_upstream_ascii_item_response(it, uc, -1);
                    } else {
                        cb_assert(false); /* TODO. */
                    }

                    item_remove(it);
                    break;

                case PROTOCOL_BINARY_RESPONSE_ETMPFAIL:
                case PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND:
                    /*
                     * currently membase does not send ETMPFAIL for
                     * engine error code for ENGINE_TMPFAIL
                     */
                    d->upstream_suffix = "LOCK_ERROR\r\n";
                    d->upstream_suffix_len = 0;
                    d->upstream_status = status;
                    d->upstream_retry = 0;
                    d->target_host_ident = NULL;
                    break;

                case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
                    d->upstream_suffix = "NOT_FOUND\r\n";
                    d->upstream_suffix_len = 0;
                    d->upstream_status = status;
                    d->upstream_retry = 0;
                    d->target_host_ident = NULL;
                    break;
            }

            conn_set_state(c, conn_pause);

            cproxy_update_event_write(d, uc);

            return;
        }

    case PROTOCOL_BINARY_CMD_FLUSH:
        conn_set_state(c, conn_pause);

        if (uc != NULL) {
            if (status == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED) {
                if (d->upstream_suffix != NULL &&
                    d->upstream_suffix_len == 0 &&
                    strncmp(d->upstream_suffix, "OK\r\n", 4) == 0) {
                    d->upstream_suffix = "SERVER_ERROR flush_all not supported\r\n";
                }
            }
        }

        /* TODO: Handle flush_all's expiration parameter against */
        /* the front_cache. */

        /* TODO: We flush the front_cache too often, inefficiently */
        /* on every downstream FLUSH response, rather than on */
        /* just the last FLUSH response. */

        if (uc != NULL) {
            mcache_flush_all(&d->ptd->proxy->front_cache, 0);
        }
        break;

    case PROTOCOL_BINARY_CMD_NOOP:
        conn_set_state(c, conn_pause);
        break;

    case PROTOCOL_BINARY_CMD_SET: /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_ADD:
    case PROTOCOL_BINARY_CMD_REPLACE:
    case PROTOCOL_BINARY_CMD_APPEND:
    case PROTOCOL_BINARY_CMD_PREPEND:
    case PROTOCOL_BINARY_CMD_TOUCH:
        conn_set_state(c, conn_pause);

        if (uc != NULL) {
            cb_assert(uc->next == NULL);

            switch (status) {
            case PROTOCOL_BINARY_RESPONSE_SUCCESS:
                out_string(uc, "STORED");
                break;
            case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
                if (c->cmd == PROTOCOL_BINARY_CMD_ADD) {
                    out_string(uc, "NOT_STORED");
                } else {
                    out_string(uc, "EXISTS");
                }
                break;
            case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
                if (c->cmd == PROTOCOL_BINARY_CMD_REPLACE) {
                    out_string(uc, "NOT_STORED");
                } else {
                    out_string(uc, "NOT_FOUND");
                }
                break;
            default:
                a2b_out_error(uc, status);
                break;
            }

            cproxy_del_front_cache_key_ascii(d, uc->cmd_start);

            cproxy_update_event_write(d, uc);
        }
        break;

    case PROTOCOL_BINARY_CMD_DELETE:
        conn_set_state(c, conn_pause);

        if (uc != NULL) {
            cb_assert(uc->next == NULL);

            switch (status) {
            case 0:
                out_string(uc, "DELETED");
                break;
            case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
                out_string(uc, "EXISTS");
                break;
            case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
            case PROTOCOL_BINARY_RESPONSE_NOT_STORED:
            case PROTOCOL_BINARY_RESPONSE_ENOMEM: /* TODO. */
            default:
                out_string(uc, "NOT_FOUND");
                break;
            }

            cproxy_del_front_cache_key_ascii(d, uc->cmd_start);

            cproxy_update_event_write(d, uc);
        }
        break;

    case PROTOCOL_BINARY_CMD_INCREMENT: /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_DECREMENT:
        conn_set_state(c, conn_pause);

        if (uc != NULL) {
            protocol_binary_response_incr *response_incr;
            cb_assert(uc->next == NULL);

            /* TODO: Any weird alignment/padding issues on different */
            /*       platforms in this cast to worry about here? */

            response_incr = (protocol_binary_response_incr *) c->cmd_start;

            switch (status) {
            case 0: {
                char *s = add_conn_suffix(uc);
                if (s != NULL) {
                    uint64_t v = mc_swap64(response_incr->message.body.value);
                    sprintf(s, "%"PRIu64"", v);
                    out_string(uc, s);
                } else {
                    d->ptd->stats.stats.err_oom++;
                    cproxy_close_conn(uc);
                }
                break;
            }
            case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS: /* Due to CAS. */
                out_string(uc, "EXISTS");
                break;
            case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
                out_string(uc, "NOT_FOUND");
                break;
            case PROTOCOL_BINARY_RESPONSE_NOT_STORED:
                out_string(uc, "NOT_STORED");
                break;
            case PROTOCOL_BINARY_RESPONSE_ENOMEM: /* TODO. */
            default:
                out_string(uc, "SERVER_ERROR a2b arith error");
                break;
            }

            cproxy_del_front_cache_key_ascii(d, uc->cmd_start);

            cproxy_update_event_write(d, uc);
        }
        break;

    case PROTOCOL_BINARY_CMD_VERSION:
    case PROTOCOL_BINARY_CMD_UNL:
        conn_set_state(c, conn_pause);

        if (uc != NULL) {
            cb_assert(uc->next == NULL);

            if ((header->response.status == 0 ||
                 c->cmd == PROTOCOL_BINARY_CMD_UNL) &&
                header->response.keylen == 0 &&
                header->response.extlen == 0 &&
                header->response.bodylen > 0) {
                char *s = add_conn_suffix(uc);
                uint32_t buf_offset = 0;

                if (s != NULL) {
                    /* TODO: Assuming bodylen is not that long. */
                    if (c->cmd == PROTOCOL_BINARY_CMD_VERSION) {
                        memcpy(s, "VERSION ", 8);
                        buf_offset = 8; /* sizeof "VERSION " */
                    }
                    memcpy(s + buf_offset,
                           c->cmd_start + sizeof(protocol_binary_response_version),
                           header->response.bodylen);
                    s[buf_offset + header->response.bodylen] = '\0';
                    out_string(uc, s);
                } else {
                    d->ptd->stats.stats.err_oom++;
                    cproxy_close_conn(uc);
                }
            } else {
                out_string(uc, "SERVER_ERROR");
            }

            cproxy_update_event_write(d, uc);
        }
        break;

    case PROTOCOL_BINARY_CMD_STAT:
        cb_assert(c->noreply == false);

        if (keylen > 0) {
            cb_assert(it != NULL);      /* Holds the stat value. */
            cb_assert(it->nbytes >= 2); /* Note: ep-engine-to-mc_couch can return empty STAT val. */
            cb_assert(bodylen >= keylen);
            cb_assert(d->merger != NULL);

            if (uc != NULL) {
                cb_assert(uc->next == NULL);

                /* TODO: Handle ITEM and PREFIX. */

                protocol_stats_merge_name_val(d->merger,
                                              "STAT", 4,
                                              ITEM_key(it), it->nkey,
                                              ITEM_data(it), it->nbytes - 2);
            }

            item_remove(it);
            conn_set_state(c, conn_new_cmd);
        } else {
            /* Handle the stats terminator, which might have an error or */
            /* non-empty bodylen, for some implementations of memcached protocol. */

            cb_assert(it == NULL);
            conn_set_state(c, conn_pause);
        }
        break;

    case PROTOCOL_BINARY_CMD_QUIT:
    default:
        cb_assert(false); /* TODO: Handled unexpected responses. */
        break;
    }
}

/* Do the actual work of forwarding the command from an
 * upstream ascii conn to its assigned binary downstream.
 */
bool cproxy_forward_a2b_downstream(downstream *d) {
    conn *uc;
    int server_index = -1;
    int nc;

    cb_assert(d != NULL);

    uc = d->upstream_conn;

    cb_assert(uc != NULL);
    cb_assert(uc->state == conn_pause);
    cb_assert(uc->cmd_start != NULL);
    cb_assert(uc->thread != NULL);
    cb_assert(uc->thread->base != NULL);
    cb_assert(IS_ASCII(uc->protocol));
    cb_assert(IS_PROXY(uc->protocol));

    if (cproxy_is_broadcast_cmd(uc->cmd_curr) == true) {
        cproxy_ascii_broadcast_suffix(d);
    } else {
        char *key = NULL;
        int   key_len = 0;

        if (ascii_scan_key(uc->cmd_start, &key, &key_len) &&
            key != NULL &&
            key_len > 0) {
            server_index = cproxy_server_index(d, key, key_len, NULL);
            if (server_index < 0) {
                return false;
            }
        }
    }

    nc = cproxy_connect_downstream(d, uc->thread, server_index);
    if (nc == -1) {
        return true;
    }

    if (nc > 0) {
        cb_assert(d->downstream_conns != NULL);

        if (d->usec_start == 0 &&
            d->ptd->behavior_pool.base.time_stats) {
            d->usec_start = usec_now();
        }

        if (uc->cmd == -1) {
            return cproxy_forward_a2b_simple_downstream(d, uc->cmd_start, uc);
        } else {
            return cproxy_forward_a2b_item_downstream(d, uc->cmd, uc->item, uc);
        }
    }

    return false;
}

/* Forward a simple one-liner ascii command to a binary downstream.
 * For example, get, incr/decr, delete, etc.
 * The response, though, might be a simple line or
 * multiple VALUE+END lines.
 */
bool cproxy_forward_a2b_simple_downstream(downstream *d,
                                          char *command, conn *uc) {
    int vbucket = -1;
    bool local;
    conn *c;
    uint8_t *out_key = NULL;
    uint16_t out_keylen = 0;
    uint8_t out_extlen = 0;
    int cmd_len = 0;
    token_t tokens[MAX_TOKENS];
    size_t ntokens;
    char *key;
    int key_len;

    cb_assert(d != NULL);
    cb_assert(d->ptd != NULL);
    cb_assert(d->ptd->proxy != NULL);
    cb_assert(d->downstream_conns != NULL);
    cb_assert(command != NULL);
    cb_assert(uc != NULL);
    cb_assert(uc->item == NULL);
    cb_assert(uc->cmd_curr != (protocol_binary_command) -1);
    cb_assert(d->merger == NULL);

    /* Handles multi-key get and gets. */

    if (uc->cmd_curr == PROTOCOL_BINARY_CMD_GETKQ) {
        /* Only use front_cache for 'get', not for 'gets'. */
        mcache *front_cache =
            (command[3] == ' ') ? &d->ptd->proxy->front_cache : NULL;

        return multiget_ascii_downstream(d, uc,
                                         a2b_multiget_start,
                                         a2b_multiget_skey,
                                         a2b_multiget_end,
                                         front_cache);
    }

    if (uc->cmd_curr == PROTOCOL_BINARY_CMD_GETK ||
        uc->cmd_curr == PROTOCOL_BINARY_CMD_GETL) {
        d->upstream_suffix = "END\r\n";
        d->upstream_suffix_len = 0;
        d->upstream_status = PROTOCOL_BINARY_RESPONSE_SUCCESS;
        d->upstream_retry = 0;
        d->target_host_ident = NULL;
    }

    cb_assert(uc->next == NULL);

    /* TODO: Inefficient repeated scan_tokens. */

    ntokens = scan_tokens(command, tokens, MAX_TOKENS, &cmd_len);
    key= tokens[KEY_TOKEN].value;
    key_len = tokens[KEY_TOKEN].length;

    if (ntokens <= 1) { /* This was checked long ago, while parsing */
        cb_assert(false);  /* the upstream conn. */
        return false;
    }

    if (uc->cmd_curr == PROTOCOL_BINARY_CMD_FLUSH) {
        protocol_binary_request_flush req;
        protocol_binary_request_header *preq = (void*)&req;
        int size;

        memset(&req, 0, sizeof(req));
        size = a2b_fill_request(uc->cmd_curr,
                                tokens, ntokens,
                                uc->noreply, preq,
                                &out_key,
                                &out_keylen,
                                &out_extlen);
        if (size > 0) {
            cb_assert(out_key == NULL);
            cb_assert(out_keylen == 0);

            if (settings.verbose > 2) {
                moxi_log_write("a2b broadcast flush_all\n");
            }

            if (out_extlen == 0) {
                preq->request.extlen   = out_extlen = 4;
                preq->request.datatype = PROTOCOL_BINARY_RAW_BYTES;
            }

            return cproxy_broadcast_a2b_downstream(d, preq, size,
                                                   out_key,
                                                   out_keylen,
                                                   out_extlen, uc,
                                                   "OK\r\n");
        }

        if (settings.verbose > 2) {
            moxi_log_write("a2b broadcast flush_all no size\n");
        }

        return false;
    }

    if (uc->cmd_curr == PROTOCOL_BINARY_CMD_STAT) {
        protocol_binary_request_stats req;
        protocol_binary_request_header *preq = (void*)&req;
        int size;

        memset(&req, 0, sizeof(req));
        size = a2b_fill_request(uc->cmd_curr,
                                tokens, ntokens,
                                uc->noreply, preq,
                                &out_key,
                                &out_keylen,
                                &out_extlen);
        if (size > 0) {
            cb_assert(out_extlen == 0);
            cb_assert(uc->noreply == false);

            if (settings.verbose > 2) {
                moxi_log_write("a2b broadcast %s\n", command);
            }

            if (strncmp(command + 5, " reset", 6) == 0) {
                return cproxy_broadcast_a2b_downstream(d, preq, size,
                                                       out_key,
                                                       out_keylen,
                                                       out_extlen, uc,
                                                       "RESET\r\n");
            }

            if (cproxy_broadcast_a2b_downstream(d, preq, size,
                                                out_key,
                                                out_keylen,
                                                out_extlen, uc,
                                                "END\r\n")) {
                d->merger = genhash_init(128, skeyhash_ops);
                return true;
            }
        }

        return false;
    }

    if (uc->cmd_curr == PROTOCOL_BINARY_CMD_VERSION) {
        /* Fake key so that we hash to some server. */

        key     = "v";
        key_len = 1;
    }

    /* Assuming we're already connected to downstream. */
    /* Handle all other simple commands. */
    c = cproxy_find_downstream_conn_ex(d, key, key_len,
                                       &local, &vbucket);

    if (uc->cmd_curr == PROTOCOL_BINARY_CMD_VERSION) {
        key     = NULL;
        key_len = 0;
    }

    if (c != NULL) {
        if (local) {
            uc->hit_local = true;
        }

        if (cproxy_prep_conn_for_write(c)) {
            protocol_binary_request_header *header;
            int size;

            cb_assert(c->state == conn_pause);
            cb_assert(c->wbuf);
            cb_assert(c->wsize >= a2b_size_max);

            header = (protocol_binary_request_header *) c->wbuf;
            memset(header, 0, a2b_size_max);
            size = a2b_fill_request(uc->cmd_curr,
                                    tokens, ntokens,
                                    uc->noreply,
                                    header,
                                    &out_key,
                                    &out_keylen,
                                    &out_extlen);
            if (size > 0) {
                cb_assert(size <= a2b_size_max);
                cb_assert(key     == (char *) out_key);
                cb_assert(key_len == (int)    out_keylen);
                cb_assert(header->request.bodylen == 0);

                if (vbucket >= 0) {
                    header->request.reserved = htons(vbucket);
                    header->request.opaque   = htonl(vbucket);
                }

                header->request.bodylen =
                    htonl(out_keylen + out_extlen);

                a2b_set_opaque(c, header, uc->noreply);

                add_iov(c, header, size);

                if (out_key != NULL &&
                    out_keylen > 0) {
                    add_iov(c, out_key, out_keylen);
                }

                if (settings.verbose > 2) {
                    moxi_log_write("forwarding a2b to %d, cmd %x, noreply %d, vbucket %d\n",
                                   c->sfd, header->request.opcode, uc->noreply, vbucket);

                    cproxy_dump_header(c->sfd, (char *) header);
                }

                conn_set_state(c, conn_mwrite);
                c->write_and_go = conn_new_cmd;

                if (update_event(c, EV_WRITE | EV_PERSIST)) {
                    d->downstream_used_start = 1;
                    d->downstream_used       = 1;

                    if (cproxy_dettach_if_noreply(d, uc) == false) {
                        cproxy_start_downstream_timeout(d, c);
                    } else {
                        c->write_and_go = conn_pause;

                        cproxy_front_cache_delete(d->ptd, key, key_len);
                    }

                    return true;
                } else {
                    /* TODO: Error handling. */

                    if (settings.verbose > 1) {
                        moxi_log_write("ERROR: Couldn't a2b update write event\n");
                    }

                    if (d->upstream_suffix == NULL) {
                        d->upstream_suffix = "SERVER_ERROR a2b event oom\r\n";
                        d->upstream_suffix_len = 0;
                        d->upstream_status = PROTOCOL_BINARY_RESPONSE_ENOMEM;
                        d->upstream_retry = 0;
                        d->target_host_ident = NULL;
                    }
                }
            } else {
                /* TODO: Error handling. */

                if (settings.verbose > 1) {
                    moxi_log_write("ERROR: Couldn't a2b fill request: %s (%x)\n",
                            command, uc->cmd_curr);
                }

                if (d->upstream_suffix == NULL) {
                    d->upstream_suffix = "CLIENT_ERROR a2b parse request\r\n";
                    d->upstream_suffix_len = 0;
                    d->upstream_status = PROTOCOL_BINARY_RESPONSE_EINVAL;
                    d->upstream_retry = 0;
                    d->target_host_ident = NULL;
                }
            }

            d->ptd->stats.stats.err_oom++;
            cproxy_close_conn(c);
        } else {
            d->ptd->stats.stats.err_downstream_write_prep++;
            cproxy_close_conn(c);
        }
    }

    return false;
}

int a2b_multiget_start(conn *c, char *cmd, int cmd_len) {
    (void)c;
    (void)cmd;
    (void)cmd_len;
    return 0; /* No-op. */
}

/* An skey is a space prefixed key string.
 */
int a2b_multiget_skey(conn *c, char *skey, int skey_length, int vbucket, int key_index) {
    char *key     = skey + 1;
    int   key_len = skey_length - 1;

    item *it = item_alloc("b", 1, 0, 0, sizeof(protocol_binary_request_get));
    if (it != NULL) {
        if (add_conn_item(c, it)) {
            protocol_binary_request_getk *req =
                (protocol_binary_request_getk *) ITEM_data(it);

            memset(req, 0, sizeof(req->bytes));

            req->message.header.request.magic  = PROTOCOL_BINARY_REQ;
            req->message.header.request.opcode = PROTOCOL_BINARY_CMD_GETKQ;
            req->message.header.request.keylen = htons((uint16_t) key_len);
            req->message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
            req->message.header.request.bodylen  = htonl(key_len);
            req->message.header.request.opaque   = htonl(key_index);

            if (vbucket >= 0) {
                req->message.header.request.reserved = htons(vbucket);

                if (settings.verbose > 2) {
                    char key_buf[KEY_MAX_LENGTH + 10];
                    cb_assert(key_len <= KEY_MAX_LENGTH);
                    memcpy(key_buf, key, key_len);
                    key_buf[key_len] = '\0';

                    moxi_log_write("<%d a2b_multiget_skey '%s' %d %d\n",
                            c->sfd, key_buf, vbucket, key_index);
                }
            }

            if (add_iov(c, ITEM_data(it), sizeof(req->bytes)) == 0 &&
                add_iov(c, key, key_len) == 0) {
                return 0; /* Success. */
            }

            return -1;
        }

        item_remove(it);
    }

    return -1;
}

int a2b_multiget_end(conn *c) {
    return add_iov(c, &req_noop.bytes, sizeof(req_noop.bytes));
}

/* Used for broadcast commands, like flush_all or stats.
 */
bool cproxy_broadcast_a2b_downstream(downstream *d,
                                     protocol_binary_request_header *req,
                                     int req_size,
                                     uint8_t *key,
                                     uint16_t keylen,
                                     uint8_t  extlen,
                                     conn *uc,
                                     char *suffix) {
    int nwrite = 0;
    int nconns;
    int i;

    cb_assert(d != NULL);
    cb_assert(d->ptd != NULL);
    cb_assert(d->ptd->proxy != NULL);
    cb_assert(d->downstream_conns != NULL);
    cb_assert(d->downstream_used_start == 0);
    cb_assert(d->downstream_used == 0);
    cb_assert(req != NULL);
    cb_assert(req_size >= (int) sizeof(req));
    cb_assert(req->request.bodylen == 0);
    cb_assert(uc != NULL);
    cb_assert(uc->next == NULL);
    cb_assert(uc->item == NULL);

    req->request.bodylen = htonl(keylen + extlen);

    nconns = mcs_server_count(&d->mst);
    for (i = 0; i < nconns; i++) {
        conn *c = d->downstream_conns[i];
        if (c != NULL &&
            c != NULL_CONN) {
            if (cproxy_prep_conn_for_write(c)) {
                cb_assert(c->state == conn_pause);
                cb_assert(c->wbuf);
                cb_assert(c->wsize >= req_size);

                memcpy(c->wbuf, req, req_size);

                add_iov(c, c->wbuf, req_size);

                if (key != NULL &&
                    keylen > 0) {
                    add_iov(c, key, keylen);
                }

                conn_set_state(c, conn_mwrite);
                c->write_and_go = conn_new_cmd;

                if (update_event(c, EV_WRITE | EV_PERSIST)) {
                    nwrite++;

                    if (uc->noreply) {
                        c->write_and_go = conn_pause;
                    }
                } else {
                    if (settings.verbose > 1) {
                        moxi_log_write("ERROR: Update cproxy write event failed\n");
                    }

                    d->ptd->stats.stats.err_oom++;
                    cproxy_close_conn(c);
                }
            } else {
                if (settings.verbose > 1) {
                    moxi_log_write("ERROR: a2b broadcast prep conn failed\n");
                }

                d->ptd->stats.stats.err_downstream_write_prep++;
                cproxy_close_conn(c);
            }
        }
    }

    if (settings.verbose > 2) {
        moxi_log_write("%d: a2b broadcast nwrite %d out of %d\n",
                uc->sfd, nwrite, nconns);
    }

    if (nwrite > 0) {
        d->downstream_used_start = nwrite;
        d->downstream_used       = nwrite;

        if (cproxy_dettach_if_noreply(d, uc) == false) {
            d->upstream_suffix = suffix;
            d->upstream_suffix_len = 0;
            d->upstream_status = PROTOCOL_BINARY_RESPONSE_SUCCESS;
            d->upstream_retry = 0;
            d->target_host_ident = NULL;

            cproxy_start_downstream_timeout(d, NULL);
        } else {
            /* TODO: Handle flush_all's expiration parameter against */
            /* the front_cache. */

            if (req->request.opcode == PROTOCOL_BINARY_CMD_FLUSH ||
                req->request.opcode == PROTOCOL_BINARY_CMD_FLUSHQ) {
                mcache_flush_all(&d->ptd->proxy->front_cache, 0);
            }
        }

        return true;
    }

    return false;
}

/* Forward an upstream command that came with item data,
 * like set/add/replace/etc.
 */
bool cproxy_forward_a2b_item_downstream(downstream *d, short cmd,
                                        item *it, conn *uc) {

    int  vbucket = -1;
    bool local;
    conn *c;

    cb_assert(d != NULL);
    cb_assert(d->ptd != NULL);
    cb_assert(d->ptd->proxy != NULL);
    cb_assert(d->downstream_conns != NULL);
    cb_assert(it != NULL);
    cb_assert(it->nbytes >= 2);
    cb_assert(uc != NULL);
    cb_assert(uc->next == NULL);
    cb_assert(cmd > 0);

    /* Assuming we're already connected to downstream. */

    c = cproxy_find_downstream_conn_ex(d, ITEM_key(it), it->nkey,
                                       &local, &vbucket);
    if (c != NULL) {
        if (local) {
            uc->hit_local = true;
        }
        if (cproxy_prep_conn_for_write(c)) {
            uint8_t  extlen;
            uint32_t hdrlen;
            item *it_hdr;

            if (settings.verbose > 2) {
                moxi_log_write("%d: a2b_item_forward, state: %s\n",
                               c->sfd, state_text(c->state));
            }

            cb_assert(c->state == conn_pause);

            extlen = (cmd == NREAD_APPEND || cmd == NREAD_PREPEND) ? 0 : 8;
            hdrlen = sizeof(protocol_binary_request_header) +
                extlen;

            it_hdr = item_alloc("i", 1, 0, 0, hdrlen);
            if (it_hdr != NULL) {
                if (add_conn_item(c, it_hdr)) {
                    protocol_binary_request_header *req =
                        (protocol_binary_request_header *) ITEM_data(it_hdr);

                    memset(req, 0, hdrlen);

                    req->request.magic    = PROTOCOL_BINARY_REQ;
                    req->request.datatype = PROTOCOL_BINARY_RAW_BYTES;
                    req->request.keylen   = htons((uint16_t) it->nkey);
                    req->request.extlen   = extlen;

                    if (vbucket >= 0) {
                        /* We also put the vbucket id into the opaque, */
                        /* so we can have it later for not-my-vbucket */
                        /* error handling. */

                        req->request.reserved = htons(vbucket);
                        req->request.opaque   = htonl(vbucket);
                    }

                    switch (cmd) {
                    case NREAD_SET:
                        req->request.opcode =
                            uc->noreply ?
                            PROTOCOL_BINARY_CMD_SETQ :
                            PROTOCOL_BINARY_CMD_SET;
                        break;
                    case NREAD_CAS: {
                        uint64_t cas = ITEM_get_cas(it);
                        req->request.cas = mc_swap64(cas);
                        req->request.opcode =
                            uc->noreply ?
                            PROTOCOL_BINARY_CMD_SETQ :
                            PROTOCOL_BINARY_CMD_SET;
                        break;
                    }
                    case NREAD_ADD:
                        req->request.opcode =
                            uc->noreply ?
                            PROTOCOL_BINARY_CMD_ADDQ :
                            PROTOCOL_BINARY_CMD_ADD;
                        break;
                    case NREAD_REPLACE:
                        req->request.opcode =
                            uc->noreply ?
                            PROTOCOL_BINARY_CMD_REPLACEQ :
                            PROTOCOL_BINARY_CMD_REPLACE;
                        break;
                    case NREAD_APPEND:
                        req->request.opcode =
                            uc->noreply ?
                            PROTOCOL_BINARY_CMD_APPENDQ :
                            PROTOCOL_BINARY_CMD_APPEND;
                        break;
                    case NREAD_PREPEND:
                        req->request.opcode =
                            uc->noreply ?
                            PROTOCOL_BINARY_CMD_PREPENDQ :
                            PROTOCOL_BINARY_CMD_PREPEND;
                        break;
                    default:
                        cb_assert(false); /* TODO. */
                        break;
                    }

                    a2b_set_opaque(c, req, uc->noreply);

                    if (cmd != NREAD_APPEND &&
                        cmd != NREAD_PREPEND) {
                        protocol_binary_request_set *req_set =
                            (protocol_binary_request_set *) req;

                        req_set->message.body.flags =
                            htonl(strtoul(ITEM_suffix(it), NULL, 10));

                        req_set->message.body.expiration =
                            htonl(it->exptime);
                    }

                    req->request.bodylen =
                        htonl(it->nkey + (it->nbytes - 2) + extlen);

                    /* MB-24509: Prevent item being free'd until we are done
                     * with it. Will be free'd when we have finished sending it
                     * or when we close. If we finish before the upstream, the
                     * upstream will free it when calling conn_cleanup.
                     */
                    it->refcount++;
                    c->item = it;

                    if (add_iov(c, ITEM_data(it_hdr), hdrlen) == 0 &&
                        add_iov(c, ITEM_key(it),  it->nkey) == 0 &&
                        add_iov(c, ITEM_data(it), it->nbytes - 2) == 0) {
                        conn_set_state(c, conn_mwrite);
                        c->write_and_go = conn_new_cmd;

                        if (update_event(c, EV_WRITE | EV_PERSIST)) {
                            d->downstream_used_start = 1;
                            d->downstream_used       = 1;

                            if (cproxy_dettach_if_noreply(d, uc) == false) {
                                cproxy_start_downstream_timeout(d, c);

                                if (cmd == NREAD_SET &&
                                    cproxy_optimize_set_ascii(d, uc,
                                                              ITEM_key(it),
                                                              it->nkey)) {
                                    d->ptd->stats.stats.tot_optimize_sets++;
                                }
                            } else {
                                c->write_and_go = conn_pause;

                                /* TODO: At this point, the item key string is */
                                /* not '\0' or space terminated, which is */
                                /* required by the mcache API. */
                                /* Be sure to config front_cache to be off */
                                /* for binary protocol downstreams. */

                                /* mcache_delete(&d->ptd->proxy->front_cache, */
                                /*               ITEM_key(it), it->nkey); */
                            }

                            return true;
                        }
                    }
                }

                item_remove(it_hdr);
            }

            d->ptd->stats.stats.err_oom++;
            cproxy_close_conn(c);
        } else {
            d->ptd->stats.stats.err_downstream_write_prep++;
            cproxy_close_conn(c);
        }
    }

    return false;
}

void a2b_set_opaque(conn *c, protocol_binary_request_header *header,
                    bool noreply) {
    if (noreply) {
        /* Set a magic opaque value during quiet commands that tells us later */
        /* that we can ignore the downstream's error response messge, */
        /* since the upstream ascii client doesn't want it. */

        header->request.opaque = htonl(OPAQUE_IGNORE_REPLY);

        if (settings.verbose > 2) {
            moxi_log_write("%d: a2b_set_opaque OPAQUE_IGNORE_REPLY, cmdq: %x\n",
                           c->sfd, header->request.opcode);
        }
    }
}

bool a2b_not_my_vbucket(conn *uc, conn *c,
                        protocol_binary_response_header *header) {
    downstream *d = c->extra;
    cb_assert(d != NULL);
    cb_assert(d->ptd != NULL);

    if (settings.verbose > 2) {
        moxi_log_write("<%d a2b_not_my_vbucket, "
                       "cmd: %x %d\n",
                       c->sfd, header->response.opcode, uc != NULL);
    }

    if ((c->cmd != PROTOCOL_BINARY_CMD_GETK &&
         c->cmd != PROTOCOL_BINARY_CMD_GETL) ||
        c->noreply == false) {
        int max_retries;
        int vbucket;
        int sindex;

        /* For non-multi-key GET commands, enqueue a retry after */
        /* informing the vbucket map.  This includes single-key GET's. */

        if (uc == NULL) {
            /* If the client went away, though, don't retry. */

            conn_set_state(c, conn_pause);
            return true;
        }

        vbucket = ntohl(header->response.opaque);
        sindex = downstream_conn_index(d, c);

        if (settings.verbose > 2) {
            moxi_log_write("<%d a2b_not_my_vbucket, "
                           "cmd: %x not multi-key get, sindex %d, vbucket %d, retries %d\n",
                           c->sfd, header->response.opcode, sindex, vbucket, uc->cmd_retries);
        }

        mcs_server_invalid_vbucket(&d->mst, sindex, vbucket);

        /* As long as the upstream is still open and we haven't */
        /* retried too many times already. */

        max_retries = cproxy_max_retries(d);

        if (uc->cmd_retries < max_retries) {
            uc->cmd_retries++;

            d->upstream_retry++;
            d->ptd->stats.stats.tot_retry_vbucket++;

            conn_set_state(c, conn_pause);
            return true;
        }

        if (settings.verbose > 2) {
            moxi_log_write("%d: a2b_not_my_vbucket, "
                           "cmd: %x skipping retry %d >= %d\n",
                           c->sfd, header->response.opcode, uc->cmd_retries,
                           max_retries);
        }

        return false;
    } else {
        int key_index;
        char *key;
        int key_len;
        char key_buf[KEY_MAX_LENGTH + 10];
        int vbucket = -1;
        int sindex;

        /* Handle ascii multi-GET commands by awaiting all NOOP's from */
        /* downstream servers, eating the NOOP's, and retrying with */
        /* the same multiget de-duplication map, which might be partially */
        /* filled in already. */

        if (uc == NULL) {
            /* If the client went away, though, don't retry, */
            /* but keep looking for that NOOP. */

            conn_set_state(c, conn_new_cmd);
            return true;
        }

        cb_assert(uc->cmd_start != NULL);
        cb_assert(header->response.opaque != 0);

        key_index = ntohl(header->response.opaque);
        key = uc->cmd_start + key_index;
        key_len = skey_len(key);

        /* The key is not NULL or space terminated. */

        cb_assert(key_len <= KEY_MAX_LENGTH);
        memcpy(key_buf, key, key_len);
        key_buf[key_len] = '\0';

        sindex = downstream_conn_index(d, c);

        mcs_key_hash(&d->mst, key_buf, key_len, &vbucket);

        if (settings.verbose > 2) {
            moxi_log_write("<%d a2b_not_my_vbucket, "
                           "cmd: %x get/getk '%s' %d retry %d, sindex %d, vbucket %d\n",
                           c->sfd, header->response.opcode, key_buf, key_len,
                           d->upstream_retry + 1, sindex, vbucket);
        }

        mcs_server_invalid_vbucket(&d->mst, sindex, vbucket);

        /* Update the de-duplication map, removing the key, so that */
        /* we'll reattempt another request for the key during the */
        /* retry. */

        if (d->multiget != NULL) {
            multiget_entry *entry = genhash_find(d->multiget, key_buf);

            if (settings.verbose > 2) {
                moxi_log_write("<%d a2b_not_my_vbucket, "
                               "cmd: %x get/getk '%s' %d retry: %d, entry: %d, vbucket %d "
                               "deleting multiget entry\n",
                               c->sfd, header->response.opcode, key_buf, key_len,
                               d->upstream_retry + 1, entry != NULL, vbucket);
            }

            genhash_delete(d->multiget, key_buf);

            while (entry != NULL) {
                multiget_entry *curr = entry;
                entry = entry->next;
                free(curr);
            }
        } else {
            if (settings.verbose > 2) {
                moxi_log_write("<%d a2b_not_my_vbucket no dedupe map, "
                               "cmd: %x get/getk '%s' %d retry: %d, vbucket %d\n",
                               c->sfd, header->response.opcode, key_buf, key_len,
                               d->upstream_retry + 1, vbucket);
            }
        }

        /* Signal that we need to retry, where this counter is */
        /* later checked after all NOOP's from downstreams are */
        /* received. */

        d->upstream_retry++;
        d->ptd->stats.stats.tot_retry_vbucket++;

        conn_set_state(c, conn_new_cmd);
        return true;
    }
}

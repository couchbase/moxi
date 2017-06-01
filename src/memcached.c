/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  memcached - memory caching daemon
 *
 *       http://www.danga.com/memcached/
 *
 *  Copyright 2003 Danga Interactive, Inc.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Anatoly Vorobey <mellon@pobox.com>
 *      Brad Fitzpatrick <brad@danga.com>
 */
#include "memcached.h"
#include <sys/stat.h>
#include <signal.h>
#include <ctype.h>
#include <stdarg.h>

#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <platform/cbassert.h>
#include <limits.h>
#include <stddef.h>
#include <getopt.h>
// MB-14649 log() crash on windows on some CPU's
#include <math.h>

#include "cproxy.h"
#include "agent.h"
#include "stdin_check.h"
#include "log.h"

int IS_UDP(enum network_transport protocol) {
    return protocol == udp_transport;
}

#ifdef WIN32
static int is_blocking(DWORD dw) {
    return (dw == WSAEWOULDBLOCK);
}
static int is_emfile(DWORD dw) {
    return (dw == WSAEMFILE);
}
static int is_closed_conn(DWORD dw) {
    return (dw == WSAENOTCONN || WSAECONNRESET);
}
static int is_addrinuse(DWORD dw) {
    return (dw == WSAEADDRINUSE);
}
#else
static int is_blocking(int dw) {
    return (dw == EAGAIN || dw == EWOULDBLOCK);
}

static int is_emfile(int dw) {
    return (dw == EMFILE);
}

static int is_closed_conn(int dw) {
    return  (dw == ENOTCONN || dw != ECONNRESET);
}

static int is_addrinuse(int dw) {
    return (dw == EADDRINUSE);
}
#endif

/*
 * forward declarations
 */
static SOCKET new_socket(struct addrinfo *ai);

enum try_read_result {
    READ_DATA_RECEIVED,
    READ_NO_DATA_RECEIVED,
    READ_ERROR,            /** an error occured (on the socket) (or client closed connection) */
    READ_MEMORY_ERROR      /** failed to allocate more memory */
};

static enum try_read_result try_read_network(conn *c);
static enum try_read_result try_read_udp(conn *c);

/* stats */
static void stats_init(void);

/* defaults */
static void settings_init(void);

/* event handling, network IO */
static void event_handler(evutil_socket_t fd, short which, void *arg);
static void conn_close(conn *c);
static void conn_init(void);
static void write_and_free(conn *c, char *buf, size_t bytes);

/* time handling */
static rel_time_t realtime(const time_t exptime);
static void set_current_time(void);  /* update the global variable holding
                              global 32-bit seconds-since-start time
                              (to avoid 64 bit time_t) */

static void conn_free(conn *c);

/** exported globals **/
struct stats stats;
struct settings settings;
time_t process_started;     /* when the process was started */
conn *listen_conn = NULL;

/** file scope variables **/
static struct event_base *main_base;

enum transmit_result {
    TRANSMIT_COMPLETE,   /** All done writing. */
    TRANSMIT_INCOMPLETE, /** More data remaining to write. */
    TRANSMIT_SOFT_ERROR, /** Can't write any more right now. */
    TRANSMIT_HARD_ERROR  /** Can't write (c->state is set to conn_closing) */
};

static enum transmit_result transmit(conn *c);

conn_funcs conn_funcs_default = {
    .conn_init                   = NULL,
    .conn_close                  = NULL,
    .conn_connect                = NULL,
    .conn_process_ascii_command  = process_command,
    .conn_process_binary_command = dispatch_bin_command,
    .conn_complete_nread_ascii   = complete_nread_ascii,
    .conn_complete_nread_binary  = complete_nread_binary,
    .conn_pause                  = NULL,
    .conn_realtime               = realtime,
    .conn_binary_command_magic   = PROTOCOL_BINARY_REQ,
    .conn_state_change           = NULL
};

#ifdef MAIN_CHECK
int main_check(int argc, char **argv);
#endif

/* global logger handle */
moxi_log *ml;

/*
 * given time value that's either unix time or delta from current unix time, return
 * unix time. Use the fact that delta can't exceed one month (and real time value can't
 * be that low).
 */
static rel_time_t realtime(const time_t exptime) {
    /* no. of seconds in 30 days - largest possible delta exptime */

    if (exptime == 0) return 0; /* 0 means never expire */

    if (exptime > REALTIME_MAXDELTA) {
        /* if item expiration is at/before the server started, give it an
           expiration time of 1 second after the server started.
           (because 0 means don't expire).  without this, we'd
           underflow and wrap around to some large value way in the
           future, effectively making items expiring in the past
           really expiring never */
        if (exptime <= process_started)
            return (rel_time_t)1;
        return (rel_time_t)(exptime - process_started);
    } else {
        return (rel_time_t)(exptime + current_time);
    }
}

static void stats_init(void) {
    stats.curr_items = stats.total_items = stats.curr_conns = stats.total_conns = stats.conn_structs = 0;
    stats.get_cmds = stats.set_cmds = stats.get_hits = stats.get_misses = stats.evictions = 0;
    stats.curr_bytes = stats.listen_disabled_num = 0;
    stats.accepting_conns = true; /* assuming we start in this state. */

    /* make the time we started always be 2 seconds before we really
       did, so time(0) - time.started is never zero.  if so, things
       like 'settings.oldest_live' which act as booleans as well as
       values are now false in boolean context... */
    process_started = time(0) - 2;
    stats_prefix_init();
}

static void stats_reset(void) {
    STATS_LOCK();
    stats.total_items = stats.total_conns = 0;
    stats.evictions = 0;
    stats.listen_disabled_num = 0;
    stats_prefix_clear();
    STATS_UNLOCK();
    threadlocal_stats_reset();
    item_stats_reset();
}

static void settings_init(void) {
    settings.use_cas = true;
    settings.access = 0700;
    settings.port = UNSPECIFIED;
    settings.udpport = UNSPECIFIED;
    /* By default this string should be NULL for getaddrinfo() */
    settings.inter = NULL;
    settings.maxbytes = 64 * 1024 * 1024; /* default is 64MB */
    settings.maxconns = 1024;         /* to limit connections-related memory to about 5MB */
    settings.verbose = 0;
    settings.oldest_live = 0;
    settings.evict_to_free = 1;       /* push old items out of cache when memory runs out */
    settings.socketpath = NULL;       /* by default, not using a unix socket */
    settings.factor = 1.25;
    settings.chunk_size = 48;         /* space for a modest key and value */
    settings.num_threads = 4 + 1;     /* N workers + 1 dispatcher */
    settings.prefix_delimiter = ':';
    settings.detail_enabled = 0;
    settings.reqs_per_event = 20;
    settings.backlog = 1024;
    settings.binding_protocol = negotiating_prot;
}

/*
 * Adds a message header to a connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
int add_msghdr(conn *c) {
    struct msghdr *msg;

    cb_assert(c != NULL);

    if (c->msgsize == c->msgused) {
        msg = realloc(c->msglist, c->msgsize * 2 * sizeof(struct msghdr));
        if (! msg)
            return -1;
        c->msglist = msg;
        c->msgsize *= 2;
    }

    msg = c->msglist + c->msgused;

    /* this wipes msg_iovlen, msg_control, msg_controllen, and
       msg_flags, the last 3 of which aren't defined on solaris: */
    memset(msg, 0, sizeof(struct msghdr));

    msg->msg_iov = &c->iov[c->iovused];

    if (c->request_addr_size > 0) {
        msg->msg_name = &c->request_addr;
        msg->msg_namelen = c->request_addr_size;
    }

    c->msgbytes = 0;
    c->msgused++;

    if (IS_UDP(c->transport)) {
        /* Leave room for the UDP header, which we'll fill in later. */
        return add_iov(c, NULL, UDP_HEADER_SIZE);
    }

    return 0;
}


/*
 * Free list management for connections.
 */

static conn **freeconns;
static size_t freetotal;
static size_t freecurr;
/* Lock for connection freelist */
static cb_mutex_t conn_lock;

static void conn_init(void) {
    freetotal = 200;
    freecurr = 0;
    cb_mutex_initialize(&conn_lock);
    if ((freeconns = calloc(freetotal, sizeof(conn *))) == NULL) {
        moxi_log_write("Failed to allocate connection structures\n");
    }
    return;
}

/*
 * Returns a connection from the freelist, if any.
 */
conn *conn_from_freelist() {
    conn *c;

    cb_mutex_enter(&conn_lock);
    if (freecurr > 0) {
        c = freeconns[--freecurr];
    } else {
        c = NULL;
    }
    cb_mutex_exit(&conn_lock);

    return c;
}

/*
 * Adds a connection to the freelist. 0 = success.
 */
bool conn_add_to_freelist(conn *c) {
    bool ret = true;
    cb_mutex_enter(&conn_lock);
    if (freecurr < freetotal) {
        freeconns[freecurr++] = c;
        ret = false;
    } else {
        /* try to enlarge free connections array */
        size_t newsize = freetotal * 2;
        conn **new_freeconns = realloc(freeconns, sizeof(conn *) * newsize);
        if (new_freeconns) {
            freetotal = newsize;
            freeconns = new_freeconns;
            freeconns[freecurr++] = c;
            ret = false;
        }
    }
    cb_mutex_exit(&conn_lock);
    return ret;
}

static const char *prot_text(enum protocol prot) {
    char *rv = "unknown";
    switch(prot) {
        case ascii_prot:
            rv = "ascii";
            break;
        case binary_prot:
            rv = "binary";
            break;
        case proxy_upstream_ascii_prot:
            rv = "proxy-upstream-ascii";
            break;
        case proxy_upstream_binary_prot:
            rv = "proxy-upstream-binary";
            break;
        case proxy_downstream_ascii_prot:
            rv = "proxy-downstream-ascii";
            break;
        case proxy_downstream_binary_prot:
            rv = "proxy-downstream-binary";
            break;
        case negotiating_proxy_prot:
            rv = "auto-negotiate-proxy";
        case negotiating_prot:
            rv = "auto-negotiate";
            break;
    }
    return rv;
}

conn *conn_new(const SOCKET sfd, enum conn_states init_state,
               const int event_flags,
               const int read_buffer_size,
               enum network_transport transport,
               struct event_base *base,
               conn_funcs *funcs, void *extra) {
    conn *c = conn_from_freelist();

    if (NULL == c) {
        if (!(c = (conn *)calloc(1, sizeof(conn)))) {
            moxi_log_write("calloc()\n");
            return NULL;
        }
        MEMCACHED_CONN_CREATE(c);

        c->rbuf = c->wbuf = 0;
        c->ilist = 0;
        c->suffixlist = 0;
        c->iov = 0;
        c->msglist = 0;
        c->hdrbuf = 0;

        c->rsize = read_buffer_size;
        c->wsize = DATA_BUFFER_SIZE;
        c->isize = ITEM_LIST_INITIAL;
        c->suffixsize = SUFFIX_LIST_INITIAL;
        c->iovsize = IOV_LIST_INITIAL;
        c->msgsize = MSG_LIST_INITIAL;
        c->hdrsize = 0;

        c->rbuf = (char *)malloc((size_t)c->rsize);
        c->wbuf = (char *)malloc((size_t)c->wsize);
        c->ilist = (item **)malloc(sizeof(item *) * c->isize);
        c->suffixlist = (char **)malloc(sizeof(char *) * c->suffixsize);
        c->iov = (struct iovec *)malloc(sizeof(struct iovec) * c->iovsize);
        c->msglist = (struct msghdr *)malloc(sizeof(struct msghdr) * c->msgsize);

        if (c->rbuf == 0 || c->wbuf == 0 || c->ilist == 0 || c->iov == 0 ||
                c->msglist == 0 || c->suffixlist == 0) {
            conn_free(c);
            moxi_log_write("malloc()\n");
            return NULL;
        }

        STATS_LOCK();
        stats.conn_structs++;
        STATS_UNLOCK();
    }

    c->transport = transport;
    c->protocol = settings.binding_protocol;

    /* unix socket mode doesn't need this, so zeroed out.  but why
     * is this done for every command?  presumably for UDP
     * mode.  */
    if (!settings.socketpath) {
        c->request_addr_size = sizeof(c->request_addr);
    } else {
        c->request_addr_size = 0;
    }

    if (settings.verbose > 1) {
        if (init_state == conn_listening) {
            moxi_log_write("<%d server listening (%s)\n", sfd,
                prot_text(c->protocol));
        } else if (IS_UDP(transport)) {
            moxi_log_write("<%d server listening (udp)\n", sfd);
        } else if (IS_NEGOTIATING(c->protocol)) {
            moxi_log_write("<%d new auto-negotiating client connection\n",
                    sfd);
        } else if (c->protocol == ascii_prot) {
            moxi_log_write("<%d new ascii client connection.\n", sfd);
        } else if (c->protocol == binary_prot) {
            moxi_log_write("<%d new binary client connection.\n", sfd);
        } else {
            moxi_log_write("<%d new %s client connection\n",
                    sfd, prot_text(c->protocol));
        }
    }

    c->sfd = sfd;
    c->state = init_state;
    c->rlbytes = 0;
    c->cmd = -1;
    c->rbytes = c->wbytes = 0;
    c->wcurr = c->wbuf;
    c->rcurr = c->rbuf;
    c->ritem = 0;
    c->icurr = c->ilist;
    c->suffixcurr = c->suffixlist;
    c->ileft = 0;
    c->suffixleft = 0;
    c->iovused = 0;
    c->msgcurr = 0;
    c->msgused = 0;

    c->write_and_go = init_state;
    c->write_and_free = 0;
    c->item = 0;

    c->noreply = false;

    c->funcs = funcs;
    if (c->funcs == NULL) {
        c->funcs = &conn_funcs_default;
        if (settings.verbose > 1)
            moxi_log_write( "<%d initialized conn_funcs to default\n", sfd);
    }

    c->cmd_curr = -1;
    c->cmd_start = NULL;
    c->cmd_start_time = 0;
    c->cmd_retries = 0;
    c->corked = NULL;
    c->host_ident = NULL;
    c->peer_host = NULL;
    c->peer_protocol = 0;
    c->peer_port = 0;
    c->update_diag = NULL;

    c->extra = extra;

    event_set(&c->event, sfd, event_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = event_flags;

    if (event_add(&c->event, 0) == -1) {
        event_del(&c->event);
        conn_free(c);
        perror("event_add\n");
        return NULL;
    }

    if (c->funcs->conn_init != NULL &&
        c->funcs->conn_init(c) == false) {
        event_del(&c->event);
        conn_free(c);
        return NULL;
    }

    STATS_LOCK();
    stats.curr_conns++;
    stats.total_conns++;
    STATS_UNLOCK();

    MEMCACHED_CONN_ALLOCATE(c->sfd);

    return c;
}

static void conn_cleanup(conn *c) {
    cb_assert(c != NULL);

    if (c->item) {
        item_remove(c->item);
        c->item = 0;
    }

    if (c->ileft != 0) {
        for (; c->ileft > 0; c->ileft--, c->icurr++) {
            item_remove(*(c->icurr));
        }
    }

    if (c->suffixleft != 0) {
        for (; c->suffixleft > 0; c->suffixleft--, c->suffixcurr++) {
            cache_free(c->thread->suffix_cache, *(c->suffixcurr));
        }
    }

    if (c->write_and_free) {
        free(c->write_and_free);
        c->write_and_free = 0;
    }
}

/*
 * Frees a connection.
 */
void conn_free(conn *c) {
    if (c) {
        MEMCACHED_CONN_DESTROY(c);
        if (c->hdrbuf)
            free(c->hdrbuf);
        if (c->msglist)
            free(c->msglist);
        if (c->rbuf)
            free(c->rbuf);
        if (c->wbuf)
            free(c->wbuf);
        if (c->ilist)
            free(c->ilist);
        if (c->suffixlist)
            free(c->suffixlist);
        if (c->iov)
            free(c->iov);
        if (c->host_ident)
            free(c->host_ident);

        while (c->corked != NULL) {
            bin_cmd *bc = c->corked;
            c->corked = c->corked->next;
            if (bc->request_item != NULL) {
                item_remove(bc->request_item);
            }
            if (bc->response_item != NULL) {
                item_remove(bc->response_item);
            }
            free(bc);
        }

        free(c);
    }
}

static void conn_close(conn *c) {
    cb_assert(c != NULL);

    /* delete the event, the socket and the conn */
    event_del(&c->event);

    if (settings.verbose > 1)
        moxi_log_write("<%d connection closed.\n", c->sfd);

    MEMCACHED_CONN_RELEASE(c->sfd);
    closesocket(c->sfd);
    accept_new_conns(true);
    conn_cleanup(c);

    /* if the connection has big buffers, just free it */
    if (c->rsize > READ_BUFFER_HIGHWAT || conn_add_to_freelist(c)) {
        conn_free(c);
    }

    STATS_LOCK();
    stats.curr_conns--;
    STATS_UNLOCK();

    return;
}

/*
 * Shrinks a connection's buffers if they're too big.  This prevents
 * periodic large "get" requests from permanently chewing lots of server
 * memory.
 *
 * This should only be called in between requests since it can wipe output
 * buffers!
 */
static void conn_shrink(conn *c) {
    cb_assert(c != NULL);

    if (IS_UDP(c->transport))
        return;

    if (c->rsize > READ_BUFFER_HIGHWAT && c->rbytes < DATA_BUFFER_SIZE) {
        char *newbuf;

        if (c->rcurr != c->rbuf)
            memmove(c->rbuf, c->rcurr, (size_t)c->rbytes);

        newbuf = (char *)realloc((void *)c->rbuf, DATA_BUFFER_SIZE);

        if (newbuf) {
            c->rbuf = newbuf;
            c->rsize = DATA_BUFFER_SIZE;
        }
        /* TODO check other branch... */
        c->rcurr = c->rbuf;
    }

    if (c->isize > ITEM_LIST_HIGHWAT) {
        item **newbuf = (item**) realloc((void *)c->ilist, ITEM_LIST_INITIAL * sizeof(c->ilist[0]));
        if (newbuf) {
            c->ilist = newbuf;
            c->isize = ITEM_LIST_INITIAL;
        }
    /* TODO check error condition? */
    }

    if (c->msgsize > MSG_LIST_HIGHWAT) {
        struct msghdr *newbuf = (struct msghdr *) realloc((void *)c->msglist, MSG_LIST_INITIAL * sizeof(c->msglist[0]));
        if (newbuf) {
            c->msglist = newbuf;
            c->msgsize = MSG_LIST_INITIAL;
        }
    /* TODO check error condition? */
    }

    if (c->iovsize > IOV_LIST_HIGHWAT) {
        struct iovec *newbuf = (struct iovec *) realloc((void *)c->iov, IOV_LIST_INITIAL * sizeof(c->iov[0]));
        if (newbuf) {
            c->iov = newbuf;
            c->iovsize = IOV_LIST_INITIAL;
        }
    /* TODO check return value */
    }
}

/**
 * Convert a state name to a human readable form.
 */
const char *state_text(enum conn_states state) {
    const char* const statenames[] = { "conn_listening",
                                       "conn_new_cmd",
                                       "conn_waiting",
                                       "conn_read",
                                       "conn_parse_cmd",
                                       "conn_write",
                                       "conn_nread",
                                       "conn_swallow",
                                       "conn_closing",
                                       "conn_mwrite",
                                       "conn_pause",
                                       "conn_connecting" };
    return statenames[state];
}

/*
 * Sets a connection's current state in the state machine. Any special
 * processing that needs to happen on certain state transitions can
 * happen here.
 */
void conn_set_state(conn* c, enum conn_states state) {
    cb_assert(c != NULL);
    cb_assert(state < conn_max_state);

    if (state != c->state) {
        if (c->funcs != NULL &&
            c->funcs->conn_state_change != NULL) {
            c->funcs->conn_state_change(c, state);
        }

        if (settings.verbose > 2) {
            moxi_log_write("%d: going from %s to %s\n",
                    c->sfd, state_text(c->state),
                    state_text(state));
        }

        c->state = state;

        if (state == conn_write || state == conn_mwrite) {
            MEMCACHED_PROCESS_COMMAND_END(c->sfd, c->wbuf, c->wbytes);
        }
    }
}

/*
 * Ensures that there is room for another struct iovec in a connection's
 * iov list.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
int ensure_iov_space(conn *c) {
    cb_assert(c != NULL);

    if (c->iovused >= c->iovsize) {
        int i, iovnum;
        struct iovec *new_iov = (struct iovec *)realloc(c->iov,
                                (c->iovsize * 2) * sizeof(struct iovec));
        if (! new_iov)
            return -1;
        c->iov = new_iov;
        c->iovsize *= 2;

        /* Point all the msghdr structures at the new list. */
        for (i = 0, iovnum = 0; i < c->msgused; i++) {
            c->msglist[i].msg_iov = &c->iov[iovnum];
            iovnum += c->msglist[i].msg_iovlen;
        }
    }

    return 0;
}


/*
 * Adds data to the list of pending data that will be written out to a
 * connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
int add_iov(conn *c, const void *buf, int len) {
    struct msghdr *m;
    int leftover;
    bool limit_to_mtu;

    cb_assert(c != NULL);
    cb_assert(c->msgused > 0);

    do {
        m = &c->msglist[c->msgused - 1];

        /*
         * Limit UDP packets, and the first payloads of TCP replies, to
         * UDP_MAX_PAYLOAD_SIZE bytes.
         */
        limit_to_mtu = IS_UDP(c->transport) || (1 == c->msgused);

        /* We may need to start a new msghdr if this one is full. */
        if (m->msg_iovlen == IOV_MAX ||
            (limit_to_mtu && c->msgbytes >= UDP_MAX_PAYLOAD_SIZE)) {
            add_msghdr(c);
            m = &c->msglist[c->msgused - 1];
        }

        if (ensure_iov_space(c) != 0)
            return -1;

        /* If the fragment is too big to fit in the datagram, split it up */
        if (limit_to_mtu && len + c->msgbytes > UDP_MAX_PAYLOAD_SIZE) {
            leftover = len + c->msgbytes - UDP_MAX_PAYLOAD_SIZE;
            len -= leftover;
        } else {
            leftover = 0;
        }

        m = &c->msglist[c->msgused - 1];
        m->msg_iov[m->msg_iovlen].iov_base = (void *)buf;
        m->msg_iov[m->msg_iovlen].iov_len = len;

        c->msgbytes += len;
        c->iovused++;
        m->msg_iovlen++;

        buf = ((char *)buf) + len;
        len = leftover;
    } while (leftover > 0);

    return 0;
}


/*
 * Constructs a set of UDP headers and attaches them to the outgoing messages.
 */
static int build_udp_headers(conn *c) {
    int i;
    unsigned char *hdr;

    cb_assert(c != NULL);

    if (c->msgused > c->hdrsize) {
        void *new_hdrbuf;
        if (c->hdrbuf)
            new_hdrbuf = realloc(c->hdrbuf, c->msgused * 2 * UDP_HEADER_SIZE);
        else
            new_hdrbuf = malloc(c->msgused * 2 * UDP_HEADER_SIZE);
        if (! new_hdrbuf)
            return -1;
        c->hdrbuf = (unsigned char *)new_hdrbuf;
        c->hdrsize = c->msgused * 2;
    }

    hdr = c->hdrbuf;
    for (i = 0; i < c->msgused; i++) {
        c->msglist[i].msg_iov[0].iov_base = (void*)hdr;
        c->msglist[i].msg_iov[0].iov_len = UDP_HEADER_SIZE;
        *hdr++ = c->request_id / 256;
        *hdr++ = c->request_id % 256;
        *hdr++ = i / 256;
        *hdr++ = i % 256;
        *hdr++ = c->msgused / 256;
        *hdr++ = c->msgused % 256;
        *hdr++ = 0;
        *hdr++ = 0;
        cb_assert((void *) hdr == (caddr_t)c->msglist[i].msg_iov[0].iov_base + UDP_HEADER_SIZE);
    }

    return 0;
}


void out_string(conn *c, const char *str) {
    int len;

    cb_assert(c != NULL);

    if (c->noreply) {
        if (settings.verbose > 1)
            moxi_log_write(">%d NOREPLY %s\n", c->sfd, str);
        c->noreply = false;
        conn_set_state(c, conn_new_cmd);
        return;
    }

    if (settings.verbose > 1)
        moxi_log_write(">%d %s\n", c->sfd, str);

    len = (int)strlen(str);
    if ((len + 2) > c->wsize) {
        /* ought to be always enough. just fail for simplicity */
        str = "SERVER_ERROR output line too long";
        len = (int)strlen(str);
    }

    memcpy(c->wbuf, str, len);
    memcpy(c->wbuf + len, "\r\n", 2);
    c->wbytes = len + 2;
    c->wcurr = c->wbuf;

    conn_set_state(c, conn_write);
    c->write_and_go = conn_new_cmd;
    return;
}

/*
 * we get here after reading the value in set/add/replace commands. The command
 * has been stored in c->cmd, and the item is ready in c->item.
 */
void complete_nread_ascii(conn *c) {
    cb_assert(c != NULL);

    item *it = c->item;
    int comm = c->cmd;
    enum store_item_type ret;

    cb_mutex_enter(&c->thread->stats.mutex);
    c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    cb_mutex_exit(&c->thread->stats.mutex);

    if (strncmp(ITEM_data(it) + it->nbytes - 2, "\r\n", 2) != 0) {
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
      ret = store_item(it, comm, c);

#ifdef ENABLE_DTRACE
      uint64_t cas = ITEM_get_cas(it);
      switch (c->cmd) {
      case NREAD_ADD:
          MEMCACHED_COMMAND_ADD(c->sfd, ITEM_key(it), it->nkey,
                                (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_REPLACE:
          MEMCACHED_COMMAND_REPLACE(c->sfd, ITEM_key(it), it->nkey,
                                    (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_APPEND:
          MEMCACHED_COMMAND_APPEND(c->sfd, ITEM_key(it), it->nkey,
                                   (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_PREPEND:
          MEMCACHED_COMMAND_PREPEND(c->sfd, ITEM_key(it), it->nkey,
                                    (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_SET:
          MEMCACHED_COMMAND_SET(c->sfd, ITEM_key(it), it->nkey,
                                (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_CAS:
          MEMCACHED_COMMAND_CAS(c->sfd, ITEM_key(it), it->nkey, it->nbytes,
                                cas);
          break;
      }
#endif

      switch (ret) {
      case STORED:
          out_string(c, "STORED");
          break;
      case EXISTS:
          out_string(c, "EXISTS");
          break;
      case NOT_FOUND:
          out_string(c, "NOT_FOUND");
          break;
      case NOT_STORED:
          out_string(c, "NOT_STORED");
          break;
      default:
          out_string(c, "SERVER_ERROR Unhandled storage type.");
      }

    }

    item_remove(c->item);       /* release the c->item reference */
    c->item = 0;
}

/**
 * get a pointer to the start of the request struct for the current command
 */
void* binary_get_request(conn *c) {
    char *ret = c->rcurr;
    ret -= (sizeof(c->binary_header) + c->binary_header.request.keylen +
            c->binary_header.request.extlen);

    cb_assert(ret >= c->rbuf);
    return ret;
}

/**
 * get a pointer to the key in this request
 */
char* binary_get_key(conn *c) {
    return c->rcurr - (c->binary_header.request.keylen);
}

static void add_bin_header(conn *c, uint16_t err, uint8_t hdr_len, uint16_t key_len, uint32_t body_len) {
    protocol_binary_response_header* header;

    cb_assert(c);

    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    if (add_msghdr(c) != 0) {
        /* XXX:  out_string is inappropriate here */
        out_string(c, "SERVER_ERROR out of memory");
        return;
    }

    header = (protocol_binary_response_header *)c->wbuf;

    header->response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header->response.opcode = c->binary_header.request.opcode;
    header->response.keylen = (uint16_t)htons(key_len);

    header->response.extlen = (uint8_t)hdr_len;
    header->response.datatype = (uint8_t)PROTOCOL_BINARY_RAW_BYTES;
    header->response.status = (uint16_t)htons(err);

    header->response.bodylen = htonl(body_len);
    header->response.opaque = c->opaque;
    header->response.cas = mc_swap64(c->cas);

    if (settings.verbose > 1) {
        moxi_log_write(">%d Writing bin response:\n", c->sfd);
        cproxy_dump_header(c->sfd, (char *) header->bytes);
    }

    add_iov(c, c->wbuf, sizeof(header->response));
}

void write_bin_error(conn *c, protocol_binary_response_status err, int swallow) {
    const char *errstr = "Unknown error";
    size_t len;

    switch (err) {
    case PROTOCOL_BINARY_RESPONSE_ENOMEM:
        errstr = "Out of memory";
        break;
    case PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND:
        errstr = "Unknown command";
        break;
    case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
        errstr = "Not found";
        break;
    case PROTOCOL_BINARY_RESPONSE_EINVAL:
        errstr = "Invalid arguments";
        break;
    case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
        errstr = "Data exists for key.";
        break;
    case PROTOCOL_BINARY_RESPONSE_E2BIG:
        errstr = "Too large.";
        break;
    case PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL:
        errstr = "Non-numeric server-side value for incr or decr";
        break;
    case PROTOCOL_BINARY_RESPONSE_NOT_STORED:
        errstr = "Not stored.";
        break;
    case PROTOCOL_BINARY_RESPONSE_AUTH_ERROR:
        errstr = "Auth failure";
        break;
    case PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE:
        errstr = "Auth continue";
        break;
    case PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET:
        errstr = "Not my vbucket";
        break;
    case PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED:
        errstr = "Not supported";
        break;
    case PROTOCOL_BINARY_RESPONSE_EINTERNAL:
        errstr = "Internal error";
        break;
    case PROTOCOL_BINARY_RESPONSE_EBUSY:
        errstr = "System is busy";
        break;
    case PROTOCOL_BINARY_RESPONSE_ETMPFAIL:
        errstr = "Temporary failure";
        break;
    default:
        cb_assert(false);
        errstr = "UNHANDLED ERROR";
        moxi_log_write(">%d UNHANDLED ERROR: %d\n", c->sfd, err);
    }

    if (settings.verbose > 1) {
        moxi_log_write(">%d Writing an error: %d %s\n", c->sfd, (int) err, errstr);
    }

    len = strlen(errstr);
    add_bin_header(c, err, 0, 0, (uint32_t)len);
    if (len > 0) {
        add_iov(c, errstr, (uint32_t)len);
    }
    conn_set_state(c, conn_mwrite);
    if(swallow > 0) {
        c->sbytes = swallow;
        c->write_and_go = conn_swallow;
    } else {
        c->write_and_go = conn_new_cmd;
    }
}

/* Form and send a response to a command over the binary protocol */
void write_bin_response(conn *c, void *d, int hlen, int keylen, int dlen) {
    if (!c->noreply || c->cmd == PROTOCOL_BINARY_CMD_GET ||
        c->cmd == PROTOCOL_BINARY_CMD_GETK ||
        c->cmd == PROTOCOL_BINARY_CMD_GETL) {
        add_bin_header(c, 0, hlen, keylen, dlen);
        if(dlen > 0) {
            add_iov(c, d, dlen);
        }
        conn_set_state(c, conn_mwrite);
        c->write_and_go = conn_new_cmd;
    } else {
        conn_set_state(c, conn_new_cmd);
    }
}

/* Byte swap a 64-bit number */
uint64_t mc_swap64(uint64_t in) {
#ifdef ENDIAN_LITTLE
    /* Little endian, flip the bytes around until someone makes a faster/better
    * way to do this. */
    int64_t rv = 0;
    int i = 0;
     for(i = 0; i<8; i++) {
        rv = (rv << 8) | (in & 0xff);
        in >>= 8;
     }
    return rv;
#else
    /* big-endian machines don't need byte swapping */
    return in;
#endif
}

static void complete_incr_bin(conn *c) {
    item *it;
    char *key;
    size_t nkey;

    protocol_binary_response_incr* rsp = (protocol_binary_response_incr*)c->wbuf;
    protocol_binary_request_incr* req = binary_get_request(c);

    cb_assert(c != NULL);
    cb_assert(c->wsize >= (int) sizeof(*rsp));

    /* fix byteorder in the request */
    req->message.body.delta = mc_swap64(req->message.body.delta);
    req->message.body.initial = mc_swap64(req->message.body.initial);
    req->message.body.expiration = ntohl(req->message.body.expiration);
    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;

    if (settings.verbose) {
        size_t i;
        moxi_log_write("incr ");

        for (i = 0; i < nkey; i++) {
            moxi_log_write("%c", key[i]);
        }
        moxi_log_write(" %lld, %llu, %d\n",
                (long long)req->message.body.delta,
                (long long)req->message.body.initial,
                req->message.body.expiration);
    }

    it = item_get(key, nkey);
    if (it && (c->binary_header.request.cas == 0 ||
               c->binary_header.request.cas == ITEM_get_cas(it))) {
        /* Weird magic in add_delta forces me to pad here */
        char tmpbuf[INCR_MAX_STORAGE_LEN];
        protocol_binary_response_status st = PROTOCOL_BINARY_RESPONSE_SUCCESS;

        switch(add_delta(c, it, c->cmd == PROTOCOL_BINARY_CMD_INCREMENT,
                         req->message.body.delta, tmpbuf)) {
        case OK:
            break;
        case NON_NUMERIC:
            st = PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL;
            break;
        case EOM:
            st = PROTOCOL_BINARY_RESPONSE_ENOMEM;
            break;
        }

        if (st != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
            write_bin_error(c, st, 0);
        } else {
            rsp->message.body.value = mc_swap64(strtoull(tmpbuf, NULL, 10));
            c->cas = ITEM_get_cas(it);
            write_bin_response(c, &rsp->message.body, 0, 0,
                               sizeof(rsp->message.body.value));
        }

        item_remove(it);         /* release our reference */
    } else if (!it && req->message.body.expiration != 0xffffffff) {
        /* Save some room for the response */
        rsp->message.body.value = mc_swap64(req->message.body.initial);
        it = item_alloc(key, nkey, 0, realtime(req->message.body.expiration),
                        INCR_MAX_STORAGE_LEN);

        if (it != NULL) {
            snprintf(ITEM_data(it), INCR_MAX_STORAGE_LEN, "%llu",
                     (unsigned long long)req->message.body.initial);

            if (store_item(it, NREAD_SET, c)) {
                c->cas = ITEM_get_cas(it);
                write_bin_response(c, &rsp->message.body, 0, 0, sizeof(rsp->message.body.value));
            } else {
                write_bin_error(c, PROTOCOL_BINARY_RESPONSE_NOT_STORED, 0);
            }
            item_remove(it);         /* release our reference */
        } else {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        }
    } else if (it) {
        /* incorrect CAS */
        item_remove(it);         /* release our reference */
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
    } else {

        cb_mutex_enter(&c->thread->stats.mutex);
        if (c->cmd == PROTOCOL_BINARY_CMD_INCREMENT) {
            c->thread->stats.incr_misses++;
        } else {
            c->thread->stats.decr_misses++;
        }
        cb_mutex_exit(&c->thread->stats.mutex);

        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
    }
}

static void complete_update_bin(conn *c) {
    protocol_binary_response_status eno = PROTOCOL_BINARY_RESPONSE_EINVAL;
    enum store_item_type ret = NOT_STORED;
    cb_assert(c != NULL);

    item *it = c->item;

    cb_mutex_enter(&c->thread->stats.mutex);
    c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    cb_mutex_exit(&c->thread->stats.mutex);

    /* We don't actually receive the trailing two characters in the bin
     * protocol, so we're going to just set them here */
    *(ITEM_data(it) + it->nbytes - 2) = '\r';
    *(ITEM_data(it) + it->nbytes - 1) = '\n';

    ret = store_item(it, c->cmd, c);

#ifdef ENABLE_DTRACE
    uint64_t cas = ITEM_get_cas(it);
    switch (c->cmd) {
    case NREAD_ADD:
        MEMCACHED_COMMAND_ADD(c->sfd, ITEM_key(it), it->nkey,
                              (ret == STORED) ? it->nbytes : -1, cas);
        break;
    case NREAD_REPLACE:
        MEMCACHED_COMMAND_REPLACE(c->sfd, ITEM_key(it), it->nkey,
                                  (ret == STORED) ? it->nbytes : -1, cas);
        break;
    case NREAD_APPEND:
        MEMCACHED_COMMAND_APPEND(c->sfd, ITEM_key(it), it->nkey,
                                 (ret == STORED) ? it->nbytes : -1, cas);
        break;
    case NREAD_PREPEND:
        MEMCACHED_COMMAND_PREPEND(c->sfd, ITEM_key(it), it->nkey,
                                 (ret == STORED) ? it->nbytes : -1, cas);
        break;
    case NREAD_SET:
        MEMCACHED_COMMAND_SET(c->sfd, ITEM_key(it), it->nkey,
                              (ret == STORED) ? it->nbytes : -1, cas);
        break;
    }
#endif

    switch (ret) {
    case STORED:
        /* Stored */
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case EXISTS:
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
        break;
    case NOT_FOUND:
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    case NOT_STORED:
        if (c->cmd == NREAD_ADD) {
            eno = PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
        } else if(c->cmd == NREAD_REPLACE) {
            eno = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        } else {
            eno = PROTOCOL_BINARY_RESPONSE_NOT_STORED;
        }
        write_bin_error(c, eno, 0);
    }

    item_remove(c->item);       /* release the c->item reference */
    c->item = 0;
}

static void process_bin_get(conn *c) {
    item *it;

    protocol_binary_response_get* rsp = (protocol_binary_response_get*)c->wbuf;
    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;

    if (settings.verbose) {
        size_t ii;
        moxi_log_write("<%d GET ", c->sfd);
        for (ii = 0; ii < nkey; ++ii) {
            moxi_log_write("%c", key[ii]);
        }
        moxi_log_write("\n");
    }

    it = item_get(key, nkey);
    if (it) {
        /* the length has two unnecessary bytes ("\r\n") */
        uint16_t keylen = 0;
        uint32_t bodylen = sizeof(rsp->message.body) + (it->nbytes - 2);

        cb_mutex_enter(&c->thread->stats.mutex);
        c->thread->stats.get_cmds++;
        c->thread->stats.slab_stats[it->slabs_clsid].get_hits++;
        cb_mutex_exit(&c->thread->stats.mutex);

        MEMCACHED_COMMAND_GET(c->sfd, ITEM_key(it), it->nkey,
                              it->nbytes, ITEM_get_cas(it));

        if (c->cmd == PROTOCOL_BINARY_CMD_GETK ||
            c->cmd == PROTOCOL_BINARY_CMD_GETL) {
            bodylen += (uint32_t)nkey;
            keylen = (uint16_t)nkey;
        }
        add_bin_header(c, 0, sizeof(rsp->message.body), keylen, bodylen);
        rsp->message.header.response.cas = mc_swap64(ITEM_get_cas(it));

        /* add the flags */
        rsp->message.body.flags = htonl(strtoul(ITEM_suffix(it), NULL, 10));
        add_iov(c, &rsp->message.body, sizeof(rsp->message.body));

        if (c->cmd == PROTOCOL_BINARY_CMD_GETK ||
            c->cmd == PROTOCOL_BINARY_CMD_GETL) {
            add_iov(c, ITEM_key(it), (uint32_t)nkey);
        }

        /* Add the data minus the CRLF */
        add_iov(c, ITEM_data(it), it->nbytes - 2);
        conn_set_state(c, conn_mwrite);
        /* Remember this command so we can garbage collect it later */
        c->item = it;
    } else {
        cb_mutex_enter(&c->thread->stats.mutex);
        c->thread->stats.get_cmds++;
        c->thread->stats.get_misses++;
        cb_mutex_exit(&c->thread->stats.mutex);

        MEMCACHED_COMMAND_GET(c->sfd, key, nkey, -1, 0);

        if (c->noreply) {
            conn_set_state(c, conn_new_cmd);
        } else {
            if (c->cmd == PROTOCOL_BINARY_CMD_GETK ||
                c->cmd == PROTOCOL_BINARY_CMD_GETL) {
                char *ofs = c->wbuf + sizeof(protocol_binary_response_header);
                add_bin_header(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                        0, (uint16_t)nkey, (uint32_t)nkey);
                memcpy(ofs, key, nkey);
                add_iov(c, ofs, (int)nkey);
                conn_set_state(c, conn_mwrite);
            } else {
                write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
            }
        }
    }

    if (settings.detail_enabled) {
        stats_prefix_record_get(key, nkey, NULL != it);
    }
}

static void append_bin_stats(const char *key, const uint16_t klen,
                             const char *val, const uint32_t vlen,
                             conn *c) {
    char *buf = c->stats.buffer + c->stats.offset;
    uint32_t bodylen = klen + vlen;
    protocol_binary_response_header header = {
        .response = {
            .magic = (uint8_t)PROTOCOL_BINARY_RES,
            .opcode = PROTOCOL_BINARY_CMD_STAT,
            .keylen = (uint16_t)htons(klen),
            .datatype = (uint8_t)PROTOCOL_BINARY_RAW_BYTES,
            .bodylen = htonl(bodylen),
            .opaque = c->opaque
        }
    };

    memcpy(buf, header.bytes, sizeof(header.response));
    buf += sizeof(header.response);

    if (klen > 0) {
        memcpy(buf, key, klen);
        buf += klen;

        if (vlen > 0) {
            memcpy(buf, val, vlen);
        }
    }

    c->stats.offset += sizeof(header.response) + bodylen;
}

static void append_ascii_stats(const char *key, const uint16_t klen,
                               const char *val, const uint32_t vlen,
                               conn *c) {
    char *pos = c->stats.buffer + c->stats.offset;
    uint32_t nbytes = 0;
    size_t remaining = c->stats.size - c->stats.offset;
    size_t room = remaining - 1;

    if (klen == 0 && vlen == 0) {
        nbytes = snprintf(pos, room, "END\r\n");
    } else if (vlen == 0) {
        nbytes = snprintf(pos, room, "STAT %s\r\n", key);
    } else {
        nbytes = snprintf(pos, room, "STAT %s %s\r\n", key, val);
    }

    c->stats.offset += nbytes;
}

static bool grow_stats_buf(conn *c, size_t needed) {
    size_t nsize = c->stats.size;
    size_t available = nsize - c->stats.offset;
    bool rv = true;

    /* Special case: No buffer -- need to allocate fresh */
    if (c->stats.buffer == NULL) {
        nsize = 1024;
        available = c->stats.size = c->stats.offset = 0;
    }

    while (needed > available) {
        cb_assert(nsize > 0);
        nsize = nsize << 1;
        available = nsize - c->stats.offset;
    }

    if (nsize != c->stats.size) {
        char *ptr = realloc(c->stats.buffer, nsize);
        if (ptr) {
            c->stats.buffer = ptr;
            c->stats.size = nsize;
        } else {
            rv = false;
        }
    }

    return rv;
}

static void append_stats(const char *key, const uint16_t klen,
                  const char *val, const uint32_t vlen,
                  const void *cookie)
{
    /* value without a key is invalid */
    if (klen == 0 && vlen > 0) {
        return ;
    }

    conn *c = (conn*)cookie;

    if (IS_BINARY(c->protocol)) {
        size_t needed = vlen + klen + sizeof(protocol_binary_response_header);
        if (!grow_stats_buf(c, needed)) {
            return ;
        }
        append_bin_stats(key, klen, val, vlen, c);
    } else {
        size_t needed = vlen + klen + 10; /* 10 == "STAT = \r\n" */
        if (!grow_stats_buf(c, needed)) {
            return ;
        }
        append_ascii_stats(key, klen, val, vlen, c);
    }

    cb_assert(c->stats.offset <= c->stats.size);
}

void process_bin_proxy_stats(conn *c) {
    struct proxy_stats_cmd_info psci = {
        .do_info = false,
        .do_settings = false,
        .do_behaviors = false,
        .do_frontcache = false,
        .do_keystats = false,
        .do_stats = true,
        .do_zeros = true
    };

    /* proxy_stats_dump_proxy_main(&append_stats, c, &psci); */
    proxy_stats_dump_proxies(&append_stats, c, &psci);

    /* Append termination package and start the transfer */
    append_stats(NULL, 0, NULL, 0, c);
    if (c->stats.buffer == NULL) {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
    } else {
        write_and_free(c, c->stats.buffer, c->stats.offset);
        c->stats.buffer = NULL;
    }
}

static void process_bin_stat(conn *c) {
    char *subcommand = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;

    if (settings.verbose) {
        size_t ii;
        moxi_log_write("<%d STATS ", c->sfd);
        for (ii = 0; ii < nkey; ++ii) {
            moxi_log_write("%c", subcommand[ii]);
        }
        moxi_log_write("\n");
    }

    if (nkey == 0) {
        /* request all statistics */
        server_stats(&append_stats, c, NULL);
        (void)get_stats(NULL, 0, &append_stats, c);
    } else if (strncmp(subcommand, "reset", 5) == 0) {
        stats_reset();
    } else if (strncmp(subcommand, "settings", 8) == 0) {
        process_stat_settings(&append_stats, c, NULL);
    } else if (strncmp(subcommand, "detail", 6) == 0) {
        char *subcmd_pos = subcommand + 6;
        if (strncmp(subcmd_pos, " dump", 5) == 0) {
            int len;
            char *dump_buf = stats_prefix_dump(&len);
            if (dump_buf == NULL || len <= 0) {
                write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
                return ;
            } else {
                append_stats("detailed", (uint16_t)strlen("detailed"), dump_buf, len, c);
                free(dump_buf);
            }
        } else if (strncmp(subcmd_pos, " on", 3) == 0) {
            settings.detail_enabled = 1;
        } else if (strncmp(subcmd_pos, " off", 4) == 0) {
            settings.detail_enabled = 0;
        } else {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
            return;
        }
    } else {
        if (get_stats(subcommand, (int)nkey, &append_stats, c)) {
            if (c->stats.buffer == NULL) {
                write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
            } else {
                write_and_free(c, c->stats.buffer, c->stats.offset);
                c->stats.buffer = NULL;
            }
        } else {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        }

        return;
    }

    /* Append termination package and start the transfer */
    append_stats(NULL, 0, NULL, 0, c);
    if (c->stats.buffer == NULL) {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
    } else {
        write_and_free(c, c->stats.buffer, c->stats.offset);
        c->stats.buffer = NULL;
    }
}

void bin_read_key(conn *c, enum bin_substates next_substate, int extra) {
    cb_assert(c);
    c->substate = next_substate;
    c->rlbytes = c->keylen + extra;

    /* Ok... do we have room for the extras and the key in the input buffer? */
    ptrdiff_t offset = c->rcurr + sizeof(protocol_binary_request_header) - c->rbuf;
    if (c->rlbytes > c->rsize - offset) {
        int nsize = c->rsize;
        int size = c->rlbytes + sizeof(protocol_binary_request_header);

        while (size > nsize) {
            nsize *= 2;
        }

        if (nsize != c->rsize) {
            if (settings.verbose) {
                moxi_log_write("%d: Need to grow buffer from %lu to %lu\n",
                        c->sfd, (unsigned long)c->rsize, (unsigned long)nsize);
            }
            char *newm = realloc(c->rbuf, nsize);
            if (newm == NULL) {
                if (settings.verbose) {
                    moxi_log_write("%d: Failed to grow buffer.. closing connection\n",
                            c->sfd);
                }
                conn_set_state(c, conn_closing);
                return;
            }

            c->rbuf= newm;
            /* rcurr should point to the same offset in the packet */
            c->rcurr = c->rbuf + offset - sizeof(protocol_binary_request_header);
            c->rsize = nsize;
        }
        if (c->rbuf != c->rcurr) {
            memmove(c->rbuf, c->rcurr, c->rbytes);
            c->rcurr = c->rbuf;
            if (settings.verbose) {
                moxi_log_write("%d: Repack input buffer\n", c->sfd);
            }
        }
    }

    /* preserve the header in the buffer.. */
    c->ritem = c->rcurr + sizeof(protocol_binary_request_header);
    conn_set_state(c, conn_nread);
}

void process_bin_noreply(conn *c) {
    cb_assert(c);
    c->noreply = true;
    switch (c->binary_header.request.opcode) {
    case PROTOCOL_BINARY_CMD_SETQ:
        c->cmd = PROTOCOL_BINARY_CMD_SET;
        break;
    case PROTOCOL_BINARY_CMD_ADDQ:
        c->cmd = PROTOCOL_BINARY_CMD_ADD;
        break;
    case PROTOCOL_BINARY_CMD_REPLACEQ:
        c->cmd = PROTOCOL_BINARY_CMD_REPLACE;
        break;
    case PROTOCOL_BINARY_CMD_DELETEQ:
        c->cmd = PROTOCOL_BINARY_CMD_DELETE;
        break;
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
        c->cmd = PROTOCOL_BINARY_CMD_INCREMENT;
        break;
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
        c->cmd = PROTOCOL_BINARY_CMD_DECREMENT;
        break;
    case PROTOCOL_BINARY_CMD_QUITQ:
        c->cmd = PROTOCOL_BINARY_CMD_QUIT;
        break;
    case PROTOCOL_BINARY_CMD_FLUSHQ:
        c->cmd = PROTOCOL_BINARY_CMD_FLUSH;
        break;
    case PROTOCOL_BINARY_CMD_APPENDQ:
        c->cmd = PROTOCOL_BINARY_CMD_APPEND;
        break;
    case PROTOCOL_BINARY_CMD_PREPENDQ:
        c->cmd = PROTOCOL_BINARY_CMD_PREPEND;
        break;
    case PROTOCOL_BINARY_CMD_GETQ:
        c->cmd = PROTOCOL_BINARY_CMD_GET;
        break;
    case PROTOCOL_BINARY_CMD_GETKQ:
        c->cmd = PROTOCOL_BINARY_CMD_GETK;
        break;
    default:
        c->noreply = false;
    }
}

void dispatch_bin_command(conn *c) {
    int protocol_error = 0;

    uint32_t extlen = c->binary_header.request.extlen;
    uint32_t keylen = c->binary_header.request.keylen;
    uint32_t bodylen = c->binary_header.request.bodylen;

    if ((extlen + keylen) > bodylen) {
        /* Just write an error message and disconnect the client */
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
        if (settings.verbose) {
            moxi_log_write("Protocol error (opcode %02x), close connection %d\n",
                           c->binary_header.request.opcode,
                           c->sfd);
        }
        c->write_and_go = conn_closing;
        return;
    }

    MEMCACHED_PROCESS_COMMAND_START(c->sfd, c->rcurr, c->rbytes);

    process_bin_noreply(c);

    switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_VERSION:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                write_bin_response(c, VERSION, 0, 0, (int)strlen(VERSION));
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_FLUSH:
            if (keylen == 0 && bodylen == extlen && (extlen == 0 || extlen == 4)) {
                bin_read_key(c, bin_read_flush_exptime, extlen);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_NOOP:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                write_bin_response(c, NULL, 0, 0, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SET: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_ADD: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_REPLACE:
            if (extlen == 8 && keylen != 0 && bodylen >= (keylen + 8)) {
                bin_read_key(c, bin_reading_set_header, 8);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_GETQ:  /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GET:   /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GETKQ: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GETK:
        case PROTOCOL_BINARY_CMD_GETL:
            if (extlen == 0 && bodylen == keylen && keylen > 0) {
                bin_read_key(c, bin_reading_get_key, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_DELETE:
            if (keylen > 0 && extlen == 0 && bodylen == keylen) {
                bin_read_key(c, bin_reading_del_header, extlen);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_INCREMENT:
        case PROTOCOL_BINARY_CMD_DECREMENT:
            if (keylen > 0 && extlen == 20 && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_incr_header, 20);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_APPEND:
        case PROTOCOL_BINARY_CMD_PREPEND:
            if (keylen > 0 && extlen == 0) {
                bin_read_key(c, bin_reading_set_header, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_STAT:
            if (extlen == 0) {
                bin_read_key(c, bin_reading_stat, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_QUIT:
            if (keylen == 0 && extlen == 0 && bodylen == 0) {
                write_bin_response(c, NULL, 0, 0, 0);
                c->write_and_go = conn_closing;
                if (c->noreply) {
                    conn_set_state(c, conn_closing);
                }
            } else {
                protocol_error = 1;
            }
            break;
        default:
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, bodylen);
    }

    if (protocol_error) {
        /* Just write an error message and disconnect the client */
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
        if (settings.verbose) {
            moxi_log_write("Protocol error (opcode %02x), close connection %d\n",
                    c->binary_header.request.opcode, c->sfd);
        }
        c->write_and_go = conn_closing;
    }
}

static void process_bin_update(conn *c) {
    char *key;
    int nkey;
    int vlen;
    item *it;
    protocol_binary_request_set* req = binary_get_request(c);

    cb_assert(c != NULL);

    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;

    /* fix byteorder in the request */
    req->message.body.flags = ntohl(req->message.body.flags);
    req->message.body.expiration = ntohl(req->message.body.expiration);

    vlen = c->binary_header.request.bodylen - (nkey + c->binary_header.request.extlen);

    if (settings.verbose) {
        int ii;
        if (c->cmd == PROTOCOL_BINARY_CMD_ADD) {
            moxi_log_write("<%d ADD ", c->sfd);
        } else if (c->cmd == PROTOCOL_BINARY_CMD_SET) {
            moxi_log_write("<%d SET ", c->sfd);
        } else {
            moxi_log_write("<%d REPLACE ", c->sfd);
        }
        for (ii = 0; ii < nkey; ++ii) {
            moxi_log_write("%c", key[ii]);
        }

        if (settings.verbose > 1) {
            moxi_log_write(" Value len is %d", vlen);
        }
        moxi_log_write("\n");
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    it = item_alloc(key, nkey, req->message.body.flags,
            c->funcs->conn_realtime(req->message.body.expiration), vlen+2);

    if (it == 0) {
        if (! item_size_ok(nkey, req->message.body.flags, vlen + 2)) {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_E2BIG, vlen);
        } else {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        }

        /* Avoid stale data persisting in cache because we failed alloc.
         * Unacceptable for SET. Anywhere else too? */
        if (c->cmd == PROTOCOL_BINARY_CMD_SET) {
            it = item_get(key, nkey);
            if (it) {
                item_unlink(it);
                item_remove(it);
            }
        }

        /* swallow the data line */
        c->write_and_go = conn_swallow;
        return;
    }

    ITEM_set_cas(it, c->binary_header.request.cas);

    switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_ADD:
            c->cmd = NREAD_ADD;
            break;
        case PROTOCOL_BINARY_CMD_SET:
            c->cmd = NREAD_SET;
            break;
        case PROTOCOL_BINARY_CMD_REPLACE:
            c->cmd = NREAD_REPLACE;
            break;
        default:
            cb_assert(0);
    }

    if (ITEM_get_cas(it) != 0) {
        c->cmd = NREAD_CAS;
    }

    c->item = it;
    c->ritem = ITEM_data(it);
    c->rlbytes = vlen;
    conn_set_state(c, conn_nread);
    c->substate = bin_read_set_value;
}

static void process_bin_append_prepend(conn *c) {
    char *key;
    int nkey;
    int vlen;
    item *it;

    cb_assert(c != NULL);

    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;
    vlen = c->binary_header.request.bodylen - nkey;

    if (settings.verbose > 1) {
        moxi_log_write("Value len is %d\n", vlen);
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    it = item_alloc(key, nkey, 0, 0, vlen+2);

    if (it == 0) {
        if (! item_size_ok(nkey, 0, vlen + 2)) {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_E2BIG, vlen);
        } else {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        }
        /* swallow the data line */
        c->write_and_go = conn_swallow;
        return;
    }

    ITEM_set_cas(it, c->binary_header.request.cas);

    switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_APPEND:
            c->cmd = NREAD_APPEND;
            break;
        case PROTOCOL_BINARY_CMD_PREPEND:
            c->cmd = NREAD_PREPEND;
            break;
        default:
            cb_assert(0);
    }

    c->item = it;
    c->ritem = ITEM_data(it);
    c->rlbytes = vlen;
    conn_set_state(c, conn_nread);
    c->substate = bin_read_set_value;
}

static void process_bin_flush(conn *c) {
    time_t exptime = 0;
    protocol_binary_request_flush* req = binary_get_request(c);

    if (c->binary_header.request.extlen == sizeof(req->message.body)) {
        exptime = ntohl(req->message.body.expiration);
    }

    set_current_time();

    if (exptime > 0) {
        settings.oldest_live = c->funcs->conn_realtime(exptime) - 1;
    } else {
        settings.oldest_live = current_time - 1;
    }
    item_flush_expired();

    cb_mutex_enter(&c->thread->stats.mutex);
    c->thread->stats.flush_cmds++;
    cb_mutex_exit(&c->thread->stats.mutex);

    write_bin_response(c, NULL, 0, 0, 0);
}

static void process_bin_delete(conn *c) {
    item *it;

    protocol_binary_request_delete* req = binary_get_request(c);

    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;

    cb_assert(c != NULL);

    if (settings.verbose) {
        moxi_log_write("Deleting %s\n", key);
    }

    if (settings.detail_enabled) {
        stats_prefix_record_delete(key, nkey);
    }

    it = item_get(key, nkey);
    if (it) {
        uint64_t cas=mc_swap64(req->message.header.request.cas);
        if (cas == 0 || cas == ITEM_get_cas(it)) {
            MEMCACHED_COMMAND_DELETE(c->sfd, ITEM_key(it), it->nkey);
            item_unlink(it);
            write_bin_response(c, NULL, 0, 0, 0);
        } else {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
        }
        item_remove(it);      /* release our reference */
    } else {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
    }
}

void complete_nread_binary(conn *c) {
    cb_assert(c != NULL);
    cb_assert(c->cmd >= 0);

    switch(c->substate) {
    case bin_reading_set_header:
        if (c->cmd == PROTOCOL_BINARY_CMD_APPEND ||
                c->cmd == PROTOCOL_BINARY_CMD_PREPEND) {
            process_bin_append_prepend(c);
        } else {
            process_bin_update(c);
        }
        break;
    case bin_read_set_value:
        complete_update_bin(c);
        break;
    case bin_reading_get_key:
        process_bin_get(c);
        break;
    case bin_reading_stat:
        process_bin_stat(c);
        break;
    case bin_reading_del_header:
        process_bin_delete(c);
        break;
    case bin_reading_incr_header:
        complete_incr_bin(c);
        break;
    case bin_read_flush_exptime:
        process_bin_flush(c);
        break;
    default:
        moxi_log_write("Not handling substate %d\n", c->substate);
        cb_assert(0);
    }
}

void reset_cmd_handler(conn *c) {
    c->cmd = -1;
    c->cmd_curr = -1;
    c->substate = bin_no_state;
    conn_cleanup(c);
    conn_shrink(c);
    if (c->rbytes > 0) {
        conn_set_state(c, conn_parse_cmd);
    } else {
        conn_set_state(c, conn_waiting);
    }
}

void complete_nread(conn *c) {
    cb_assert(c != NULL);
    cb_assert(c->funcs != NULL);

    if (IS_ASCII(c->protocol)) {
        c->funcs->conn_complete_nread_ascii(c);
    } else if (IS_BINARY(c->protocol)) {
        c->funcs->conn_complete_nread_binary(c);
    }
}

/*
 * Stores an item in the cache according to the semantics of one of the set
 * commands. In threaded mode, this is protected by the cache lock.
 *
 * Returns the state of storage.
 */
enum store_item_type do_store_item(item *it, int comm, conn *c) {
    char *key = ITEM_key(it);
    item *old_it = do_item_get(key, it->nkey);
    enum store_item_type stored = NOT_STORED;

    item *new_it = NULL;
    int flags;

    if (old_it != NULL && comm == NREAD_ADD) {
        /* add only adds a nonexistent item, but promote to head of LRU */
        do_item_update(old_it);
    } else if (!old_it && (comm == NREAD_REPLACE
        || comm == NREAD_APPEND || comm == NREAD_PREPEND))
    {
        /* replace only replaces an existing value; don't store */
    } else if (comm == NREAD_CAS) {
        /* validate cas operation */
        if(old_it == NULL) {
            /* LRU expired */
            stored = NOT_FOUND;
            cb_mutex_enter(&c->thread->stats.mutex);
            c->thread->stats.cas_misses++;
            cb_mutex_exit(&c->thread->stats.mutex);
        }
        else if (ITEM_get_cas(it) == ITEM_get_cas(old_it)) {
            /* cas validates */
            /* it and old_it may belong to different classes. */
            /* I'm updating the stats for the one that's getting pushed out */
            cb_mutex_enter(&c->thread->stats.mutex);
            c->thread->stats.slab_stats[old_it->slabs_clsid].cas_hits++;
            cb_mutex_exit(&c->thread->stats.mutex);

            item_replace(old_it, it);
            stored = STORED;
        } else {
            cb_mutex_enter(&c->thread->stats.mutex);
            c->thread->stats.slab_stats[old_it->slabs_clsid].cas_badval++;
            cb_mutex_exit(&c->thread->stats.mutex);

            if(settings.verbose > 1) {
                moxi_log_write("CAS:  failure: expected %llu, got %llu\n",
                        (unsigned long long)ITEM_get_cas(old_it),
                        (unsigned long long)ITEM_get_cas(it));
            }
            stored = EXISTS;
        }
    } else {
        /*
         * Append - combine new and old record into single one. Here it's
         * atomic and thread-safe.
         */
        if (comm == NREAD_APPEND || comm == NREAD_PREPEND) {
            /*
             * Validate CAS
             */
            if (ITEM_get_cas(it) != 0) {
                /* CAS much be equal */
                if (ITEM_get_cas(it) != ITEM_get_cas(old_it)) {
                    stored = EXISTS;
                }
            }

            if (stored == NOT_STORED) {
                /* we have it and old_it here - alloc memory to hold both */
                /* flags was already lost - so recover them from ITEM_suffix(it) */

                flags = (int) strtol(ITEM_suffix(old_it), (char **) NULL, 10);

                new_it = do_item_alloc(key, it->nkey, flags, old_it->exptime, it->nbytes + old_it->nbytes - 2 /* CRLF */);

                if (new_it == NULL) {
                    /* SERVER_ERROR out of memory */
                    if (old_it != NULL)
                        do_item_remove(old_it);

                    return NOT_STORED;
                }

                /* copy data from it and old_it to new_it */

                if (comm == NREAD_APPEND) {
                    memcpy(ITEM_data(new_it), ITEM_data(old_it), old_it->nbytes);
                    memcpy(ITEM_data(new_it) + old_it->nbytes - 2 /* CRLF */, ITEM_data(it), it->nbytes);
                } else {
                    /* NREAD_PREPEND */
                    memcpy(ITEM_data(new_it), ITEM_data(it), it->nbytes);
                    memcpy(ITEM_data(new_it) + it->nbytes - 2 /* CRLF */, ITEM_data(old_it), old_it->nbytes);
                }

                it = new_it;
            }
        }

        if (stored == NOT_STORED) {
            if (old_it != NULL)
                item_replace(old_it, it);
            else
                do_item_link(it);

            c->cas = ITEM_get_cas(it);

            stored = STORED;
        }
    }

    if (old_it != NULL)
        do_item_remove(old_it);         /* release our reference */
    if (new_it != NULL)
        do_item_remove(new_it);

    if (stored == STORED) {
        c->cas = ITEM_get_cas(it);
    }

    return stored;
}

#define COMMAND_TOKEN 0
#define SUBCOMMAND_TOKEN 1
#define KEY_TOKEN 1

#define MAX_TOKENS 8

/*
 * Tokenize the command string by replacing whitespace with '\0' and update
 * the token array tokens with pointer to start of each token and length.
 * Returns total number of tokens.  The last valid token is the terminal
 * token (value points to the first unprocessed character of the string and
 * length zero).
 *
 * Usage example:
 *
 *  while(tokenize_command(command, ncommand, tokens, max_tokens) > 0) {
 *      for(int ix = 0; tokens[ix].length != 0; ix++) {
 *          ...
 *      }
 *      ncommand = tokens[ix].value - command;
 *      command  = tokens[ix].value;
 *   }
 */
size_t tokenize_command(char *command, token_t *tokens, const size_t max_tokens) {
    char *s, *e;
    size_t ntokens = 0;

    cb_assert(command != NULL && tokens != NULL && max_tokens > 1);

    for (s = e = command; ntokens < max_tokens - 1; ++e) {
        if (*e == ' ') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
                *e = '\0';
            }
            s = e + 1;
        }
        else if (*e == '\0') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
            }

            break; /* string end */
        }
    }

    /*
     * If we scanned the whole string, the terminal value pointer is null,
     * otherwise it is the first unprocessed character.
     */
    tokens[ntokens].value =  *e == '\0' ? NULL : e;
    tokens[ntokens].length = 0;
    ntokens++;

    return ntokens;
}

/* set up a connection to write a buffer then free it, used for stats */
static void write_and_free(conn *c, char *buf, size_t bytes) {
    if (buf) {
        c->write_and_free = buf;
        c->wcurr = buf;
        c->wbytes = (int)bytes;
        conn_set_state(c, conn_write);
        c->write_and_go = conn_new_cmd;
    } else {
        out_string(c, "SERVER_ERROR out of memory writing stats");
    }
}

void set_noreply_maybe(conn *c, token_t *tokens, size_t ntokens) {
    int noreply_index = (int)ntokens - 2;

    cb_assert(noreply_index >= 0);

    /*
      NOTE: this function is not the first place where we are going to
      send the reply.  We could send it instead from process_command()
      if the request line has wrong number of tokens.  However parsing
      malformed line for "noreply" option is not reliable anyway, so
      it can't be helped.
    */
    if (tokens[noreply_index].value
        && strcmp(tokens[noreply_index].value, "noreply") == 0) {
        c->noreply = true;
    }
}

void append_stat(const char *name, ADD_STAT add_stats, void *c,
                 const char *fmt, ...) {
    char val_str[STAT_VAL_LEN];
    int vlen;
    va_list ap;

    cb_assert(name);
    cb_assert(add_stats);
    cb_assert(c);
    cb_assert(fmt);

    va_start(ap, fmt);
    vlen = vsnprintf(val_str, sizeof(val_str) - 1, fmt, ap);
    va_end(ap);

    add_stats(name, (uint16_t)strlen(name), val_str, vlen, c);
}

void append_prefix_stat(const char *prefix, const char *name, ADD_STAT add_stats, void *c,
                        const char *fmt, ...) {
    char val_str[STAT_VAL_LEN];
    int vlen;
    char *val_free = 0;
    char *val;
    va_list ap;

    cb_assert(name);
    cb_assert(add_stats);
    cb_assert(c);
    cb_assert(fmt);

    va_start(ap, fmt);
    vlen = vsnprintf(val_str, sizeof(val_str), fmt, ap);
    va_end(ap);

    if (vlen > (int)sizeof(val_str)-1) {
        val_free = malloc(vlen+1);
        if (val_free != 0) {
            val = val_free;
            va_start(ap, fmt);
            vsnprintf(val_free, vlen+1, fmt, ap);
            va_end(ap);
        } else {
            val = val_str;
            vlen = sizeof(val_str)-1;
        }
    } else {
        val = val_str;
    }

    if (prefix == NULL) {
        add_stats(name, (uint16_t)strlen(name), val, vlen, c);
    } else {
        char key_str[STAT_KEY_LEN];
        strcpy(key_str, prefix); strcat(key_str, name);
        add_stats(key_str, (uint16_t)strlen(key_str), val, vlen, c);
    }

    free(val_free);
}

static void process_stats_detail(conn *c, const char *command) {
    cb_assert(c != NULL);

    if (strcmp(command, "on") == 0) {
        settings.detail_enabled = 1;
        out_string(c, "OK");
    }
    else if (strcmp(command, "off") == 0) {
        settings.detail_enabled = 0;
        out_string(c, "OK");
    }
    else if (strcmp(command, "dump") == 0) {
        int len;
        char *stats_dump = stats_prefix_dump(&len);
        write_and_free(c, stats_dump, len);
    }
    else {
        out_string(c, "CLIENT_ERROR usage: stats detail on|off|dump");
    }
}

/* return server specific stats only */
void server_stats(ADD_STAT add_stats, void *c, const char *prefix) {
#ifndef _MSC_VER
    pid_t pid = getpid();
#endif
    rel_time_t now = current_time;

    struct thread_stats thread_stats;
    threadlocal_stats_aggregate(&thread_stats);
    struct slab_stats slab_stats;
    slab_stats_aggregate(&thread_stats, &slab_stats);

#ifndef WIN32
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
#endif /* !WIN32 */

    STATS_LOCK();
#ifndef _MSC_VER
    APPEND_PREFIX_STAT("pid", "%lu", (long)pid);
#endif
    APPEND_PREFIX_STAT("uptime", "%u", now);
    APPEND_PREFIX_STAT("time", "%ld", now + (long)process_started);
    APPEND_PREFIX_STAT("version", "%s", VERSION);
    APPEND_PREFIX_STAT("pointer_size", "%d", (int)(8 * sizeof(void *)));

#ifndef WIN32
    if (1) {
        char rusage_buf[128];
        snprintf(rusage_buf, sizeof(rusage_buf),  "%ld.%06ld",
                 (long)usage.ru_utime.tv_sec, (long)usage.ru_utime.tv_usec);
        APPEND_PREFIX_STAT("rusage_user", "%s", rusage_buf);
        snprintf(rusage_buf, sizeof(rusage_buf),  "%ld.%06ld",
                 (long)usage.ru_stime.tv_sec, (long)usage.ru_stime.tv_usec);
        APPEND_PREFIX_STAT("rusage_system", "%s", rusage_buf);
    }
#endif

    APPEND_PREFIX_STAT("curr_connections", "%u", stats.curr_conns - 1);
    APPEND_PREFIX_STAT("total_connections", "%u", stats.total_conns);
    APPEND_PREFIX_STAT("connection_structures", "%u", stats.conn_structs);
    APPEND_PREFIX_STAT("cmd_get", "%llu", (unsigned long long)thread_stats.get_cmds);
    APPEND_PREFIX_STAT("cmd_set", "%llu", (unsigned long long)slab_stats.set_cmds);
    APPEND_PREFIX_STAT("cmd_flush", "%llu", (unsigned long long)thread_stats.flush_cmds);
    APPEND_PREFIX_STAT("get_hits", "%llu", (unsigned long long)slab_stats.get_hits);
    APPEND_PREFIX_STAT("get_misses", "%llu", (unsigned long long)thread_stats.get_misses);
    APPEND_PREFIX_STAT("delete_misses", "%llu", (unsigned long long)thread_stats.delete_misses);
    APPEND_PREFIX_STAT("delete_hits", "%llu", (unsigned long long)slab_stats.delete_hits);
    APPEND_PREFIX_STAT("incr_misses", "%llu", (unsigned long long)thread_stats.incr_misses);
    APPEND_PREFIX_STAT("incr_hits", "%llu", (unsigned long long)slab_stats.incr_hits);
    APPEND_PREFIX_STAT("decr_misses", "%llu", (unsigned long long)thread_stats.decr_misses);
    APPEND_PREFIX_STAT("decr_hits", "%llu", (unsigned long long)slab_stats.decr_hits);
    APPEND_PREFIX_STAT("cas_misses", "%llu", (unsigned long long)thread_stats.cas_misses);
    APPEND_PREFIX_STAT("cas_hits", "%llu", (unsigned long long)slab_stats.cas_hits);
    APPEND_PREFIX_STAT("cas_badval", "%llu", (unsigned long long)slab_stats.cas_badval);
    APPEND_PREFIX_STAT("bytes_read", "%llu", (unsigned long long)thread_stats.bytes_read);
    APPEND_PREFIX_STAT("bytes_written", "%llu", (unsigned long long)thread_stats.bytes_written);
    APPEND_PREFIX_STAT("limit_maxbytes", "%llu", (unsigned long long)settings.maxbytes);
    APPEND_PREFIX_STAT("accepting_conns", "%u", stats.accepting_conns);
    APPEND_PREFIX_STAT("listen_disabled_num", "%llu", (unsigned long long)stats.listen_disabled_num);
    APPEND_PREFIX_STAT("threads", "%d", settings.num_threads);
    APPEND_PREFIX_STAT("conn_yields", "%llu", (unsigned long long)thread_stats.conn_yields);

    STATS_UNLOCK();
}

void process_stat_settings(ADD_STAT add_stats, void *c, const char *prefix) {
    cb_assert(add_stats);
    APPEND_PREFIX_STAT("maxbytes", "%u", (unsigned int)settings.maxbytes);
    APPEND_PREFIX_STAT("maxconns", "%d", settings.maxconns);
    APPEND_PREFIX_STAT("tcpport", "%d", settings.port);
    APPEND_PREFIX_STAT("udpport", "%d", settings.udpport);
    APPEND_PREFIX_STAT("inter", "%s", settings.inter ? settings.inter : "NULL");
    APPEND_PREFIX_STAT("verbosity", "%d", settings.verbose);
    APPEND_PREFIX_STAT("oldest", "%lu", (unsigned long)settings.oldest_live);
    APPEND_PREFIX_STAT("evictions", "%s", settings.evict_to_free ? "on" : "off");
    APPEND_PREFIX_STAT("domain_socket", "%s",
                settings.socketpath ? settings.socketpath : "NULL");
    APPEND_PREFIX_STAT("umask", "%o", settings.access);
    APPEND_PREFIX_STAT("growth_factor", "%.2f", settings.factor);
    APPEND_PREFIX_STAT("chunk_size", "%d", settings.chunk_size);
    APPEND_PREFIX_STAT("num_threads", "%d", settings.num_threads);
    APPEND_PREFIX_STAT("stat_key_prefix", "%c", settings.prefix_delimiter);
    APPEND_PREFIX_STAT("detail_enabled", "%s",
                settings.detail_enabled ? "yes" : "no");
    APPEND_PREFIX_STAT("reqs_per_event", "%d", settings.reqs_per_event);
    APPEND_PREFIX_STAT("cas_enabled", "%s", settings.use_cas ? "yes" : "no");
    APPEND_PREFIX_STAT("tcp_backlog", "%d", settings.backlog);
    APPEND_PREFIX_STAT("binding_protocol", "%s",
                prot_text(settings.binding_protocol));
}

static void process_stat(conn *c, token_t *tokens, const size_t ntokens) {
    const char *subcommand = tokens[SUBCOMMAND_TOKEN].value;
    cb_assert(c != NULL);

    if (ntokens < 2) {
        out_string(c, "CLIENT_ERROR bad command line");
        return;
    }

    if (ntokens == 2) {
        server_stats(&append_stats, c, NULL);
        (void)get_stats(NULL, 0, &append_stats, c);
    } else if (strcmp(subcommand, "reset") == 0) {
        stats_reset();
        out_string(c, "RESET");
        return ;
    } else if (strcmp(subcommand, "detail") == 0) {
        /* NOTE: how to tackle detail with binary? */
        if (ntokens < 4)
            process_stats_detail(c, "");  /* outputs the error message */
        else
            process_stats_detail(c, tokens[2].value);
        /* Output already generated */
        return ;
    } else if (strcmp(subcommand, "settings") == 0) {
        process_stat_settings(&append_stats, c, NULL);
    } else if (strcmp(subcommand, "cachedump") == 0) {
        char *buf;
        unsigned int bytes, id, limit = 0;

        if (ntokens < 5) {
            out_string(c, "CLIENT_ERROR bad command line");
            return;
        }

        if (!safe_strtoul(tokens[2].value, &id) ||
            !safe_strtoul(tokens[3].value, &limit)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        buf = item_cachedump(id, limit, &bytes);
        write_and_free(c, buf, bytes);
        return ;
    } else {
        /* getting here means that the subcommand is either engine specific or
           is invalid. query the engine and see. */
        if (get_stats(subcommand, (int)strlen(subcommand), &append_stats, c)) {
            if (c->stats.buffer == NULL) {
                out_string(c, "SERVER_ERROR out of memory writing stats");
            } else {
                write_and_free(c, c->stats.buffer, c->stats.offset);
                c->stats.buffer = NULL;
            }
        } else {
            out_string(c, "ERROR");
        }
        return ;
    }

    /* append terminator and start the transfer */
    append_stats(NULL, 0, NULL, 0, c);

    if (c->stats.buffer == NULL) {
        out_string(c, "SERVER_ERROR out of memory writing stats");
    } else {
        write_and_free(c, c->stats.buffer, c->stats.offset);
        c->stats.buffer = NULL;
    }
}

/* ntokens is overwritten here... shrug.. */
static void process_get_command(conn *c, token_t *tokens, size_t ntokens, bool return_cas) {
    char *key;
    size_t nkey;
    int i = 0, sid = 0;
    item *it;
    token_t *key_token = &tokens[KEY_TOKEN];
    char *suffix;
    int stats_get_cmds   = 0;
    int stats_get_misses = 0;
    int stats_get_hits[MAX_NUMBER_OF_SLAB_CLASSES];
    cb_assert(c != NULL);

    memset(&stats_get_hits, 0, sizeof(stats_get_hits));

    do {
        while(key_token->length != 0) {

            key = key_token->value;
            nkey = key_token->length;

            if(nkey > KEY_MAX_LENGTH) {
                cb_mutex_enter(&c->thread->stats.mutex);
                c->thread->stats.get_cmds   += stats_get_cmds;
                c->thread->stats.get_misses += stats_get_misses;
                for(sid = 0; sid < MAX_NUMBER_OF_SLAB_CLASSES; sid++) {
                    c->thread->stats.slab_stats[sid].get_hits += stats_get_hits[sid];
                }
                cb_mutex_exit(&c->thread->stats.mutex);
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }

            stats_get_cmds++;
            it = item_get(key, nkey);
            if (settings.detail_enabled) {
                stats_prefix_record_get(key, nkey, NULL != it);
            }
            if (it) {
                if (i >= c->isize) {
                    item **new_list = realloc(c->ilist, sizeof(item *) * c->isize * 2);
                    if (new_list) {
                        c->isize *= 2;
                        c->ilist = new_list;
                    } else {
                        item_remove(it);
                        break;
                    }
                }

                /*
                 * Construct the response. Each hit adds three elements to the
                 * outgoing data list:
                 *   "VALUE "
                 *   key
                 *   " " + flags + " " + data length + "\r\n" + data (with \r\n)
                 */

                if (return_cas)
                {
                  MEMCACHED_COMMAND_GET(c->sfd, ITEM_key(it), it->nkey,
                                        it->nbytes, ITEM_get_cas(it));
                  /* Goofy mid-flight realloc. */
                  if (i >= c->suffixsize) {
                    char **new_suffix_list = realloc(c->suffixlist,
                                           sizeof(char *) * c->suffixsize * 2);
                    if (new_suffix_list) {
                        c->suffixsize *= 2;
                        c->suffixlist  = new_suffix_list;
                    } else {
                        item_remove(it);
                        break;
                    }
                  }

                  suffix = cache_alloc(c->thread->suffix_cache);
                  if (suffix == NULL) {
                    cb_mutex_enter(&c->thread->stats.mutex);
                    c->thread->stats.get_cmds   += stats_get_cmds;
                    c->thread->stats.get_misses += stats_get_misses;
                    for(sid = 0; sid < MAX_NUMBER_OF_SLAB_CLASSES; sid++) {
                        c->thread->stats.slab_stats[sid].get_hits += stats_get_hits[sid];
                    }
                    cb_mutex_exit(&c->thread->stats.mutex);
                    out_string(c, "SERVER_ERROR out of memory making CAS suffix");
                    item_remove(it);
                    return;
                  }
                  *(c->suffixlist + i) = suffix;
                  int suffix_len = snprintf(suffix, SUFFIX_SIZE,
                                            " %llu\r\n",
                                            (unsigned long long)ITEM_get_cas(it));
                  if (add_iov(c, "VALUE ", 6) != 0 ||
                      add_iov(c, ITEM_key(it), it->nkey) != 0 ||
                      add_iov(c, ITEM_suffix(it), it->nsuffix - 2) != 0 ||
                      add_iov(c, suffix, suffix_len) != 0 ||
                      add_iov(c, ITEM_data(it), it->nbytes) != 0)
                      {
                          item_remove(it);
                          break;
                      }
                }
                else
                {
                  MEMCACHED_COMMAND_GET(c->sfd, ITEM_key(it), it->nkey,
                                        it->nbytes, ITEM_get_cas(it));
                  if (add_iov(c, "VALUE ", 6) != 0 ||
                      add_iov(c, ITEM_key(it), it->nkey) != 0 ||
                      add_iov(c, ITEM_suffix(it), it->nsuffix + it->nbytes) != 0)
                      {
                          item_remove(it);
                          break;
                      }
                }


                if (settings.verbose > 1)
                    moxi_log_write(">%d sending key %s\n", c->sfd, ITEM_key(it));

                /* item_get() has incremented it->refcount for us */
                stats_get_hits[it->slabs_clsid]++;
                item_update(it);
                *(c->ilist + i) = it;
                i++;

            } else {
                stats_get_misses++;
                MEMCACHED_COMMAND_GET(c->sfd, key, nkey, -1, 0);
            }

            key_token++;
        }

        /*
         * If the command string hasn't been fully processed, get the next set
         * of tokens.
         */
        if(key_token->value != NULL) {
            ntokens = tokenize_command(key_token->value, tokens, MAX_TOKENS);
            key_token = tokens;
        }

    } while(key_token->value != NULL);

    c->icurr = c->ilist;
    c->ileft = i;
    if (return_cas) {
        c->suffixcurr = c->suffixlist;
        c->suffixleft = i;
    }

    if (settings.verbose > 1)
        moxi_log_write(">%d END\n", c->sfd);

    /*
        If the loop was terminated because of out-of-memory, it is not
        reliable to add END\r\n to the buffer, because it might not end
        in \r\n. So we send SERVER_ERROR instead.
    */
    if (key_token->value != NULL || add_iov(c, "END\r\n", 5) != 0
        || (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
        out_string(c, "SERVER_ERROR out of memory writing get response");
    }
    else {
        conn_set_state(c, conn_mwrite);
        c->write_and_go = conn_new_cmd;
        c->msgcurr = 0;
    }

    cb_mutex_enter(&c->thread->stats.mutex);
    c->thread->stats.get_cmds   += stats_get_cmds;
    c->thread->stats.get_misses += stats_get_misses;
    for(sid = 0; sid < MAX_NUMBER_OF_SLAB_CLASSES; sid++) {
        c->thread->stats.slab_stats[sid].get_hits += stats_get_hits[sid];
    }
    cb_mutex_exit(&c->thread->stats.mutex);

    return;
}

void process_update_command(conn *c, token_t *tokens, const size_t ntokens, int comm, bool handle_cas) {
    char *key;
    size_t nkey;
    unsigned int flags;
    int32_t exptime_int = 0;
    time_t exptime;
    int vlen;
    uint64_t req_cas_id=0;
    item *it;

    cb_assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;

    if (! (safe_strtoul(tokens[2].value, (uint32_t *)&flags)
           && safe_strtol(tokens[3].value, &exptime_int)
           && safe_strtol(tokens[4].value, (int32_t *)&vlen))) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    /* Ubuntu 8.04 breaks when I pass exptime to safe_strtol */
    exptime = exptime_int;

    /* does cas value exist? */
    if (handle_cas) {
        if (!safe_strtoull(tokens[5].value, &req_cas_id)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
    }

    vlen += 2;
    if (vlen < 0 || vlen - 2 < 0) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    it = item_alloc(key, nkey, flags, c->funcs->conn_realtime(exptime), vlen);

    if (it == 0) {
        if (! item_size_ok(nkey, flags, vlen))
            out_string(c, "SERVER_ERROR object too large for cache");
        else
            out_string(c, "SERVER_ERROR out of memory storing object");
        /* swallow the data line */
        c->write_and_go = conn_swallow;
        c->sbytes = vlen;

        /* Avoid stale data persisting in cache because we failed alloc.
         * Unacceptable for SET. Anywhere else too? */
        if (comm == NREAD_SET) {
            it = item_get(key, nkey);
            if (it) {
                item_unlink(it);
                item_remove(it);
            }
        }

        return;
    }
    ITEM_set_cas(it, req_cas_id);

    c->item = it;
    c->ritem = ITEM_data(it);
    c->rlbytes = it->nbytes;
    c->cmd = comm;
    conn_set_state(c, conn_nread);
}

static void process_arithmetic_command(conn *c, token_t *tokens, const size_t ntokens, const bool incr) {
    char temp[INCR_MAX_STORAGE_LEN];
    item *it;
    uint64_t delta;
    char *key;
    size_t nkey;

    cb_assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;

    if (!safe_strtoull(tokens[2].value, &delta)) {
        out_string(c, "CLIENT_ERROR invalid numeric delta argument");
        return;
    }

    it = item_get(key, nkey);
    if (!it) {
        cb_mutex_enter(&c->thread->stats.mutex);
        if (incr) {
            c->thread->stats.incr_misses++;
        } else {
            c->thread->stats.decr_misses++;
        }
        cb_mutex_exit(&c->thread->stats.mutex);

        out_string(c, "NOT_FOUND");
        return;
    }

    switch(add_delta(c, it, incr, delta, temp)) {
    case OK:
        out_string(c, temp);
        break;
    case NON_NUMERIC:
        out_string(c, "CLIENT_ERROR cannot increment or decrement non-numeric value");
        break;
    case EOM:
        out_string(c, "SERVER_ERROR out of memory");
        break;
    }
    item_remove(it);         /* release our reference */
}

/*
 * adds a delta value to a numeric item.
 *
 * c     connection requesting the operation
 * it    item to adjust
 * incr  true to increment value, false to decrement
 * delta amount to adjust value by
 * buf   buffer for response string
 *
 * returns a response string to send back to the client.
 */
enum delta_result_type do_add_delta(conn *c, item *it, const bool incr,
                                    const int64_t delta, char *buf) {
    char *ptr;
    int64_t value;
    int res;

    ptr = ITEM_data(it);

    if (!safe_strtoull(ptr, (uint64_t *)&value)) {
        return NON_NUMERIC;
    }

    if (incr) {
        value += delta;
        MEMCACHED_COMMAND_INCR(c->sfd, ITEM_key(it), it->nkey, value);
    } else {
        if(delta > value) {
            value = 0;
        } else {
            value -= delta;
        }
        MEMCACHED_COMMAND_DECR(c->sfd, ITEM_key(it), it->nkey, value);
    }

    cb_mutex_enter(&c->thread->stats.mutex);
    if (incr) {
        c->thread->stats.slab_stats[it->slabs_clsid].incr_hits++;
    } else {
        c->thread->stats.slab_stats[it->slabs_clsid].decr_hits++;
    }
    cb_mutex_exit(&c->thread->stats.mutex);

    snprintf(buf, INCR_MAX_STORAGE_LEN, "%llu", (unsigned long long)value);
    res = (int)strlen(buf);
    if (res + 2 > it->nbytes) { /* need to realloc */
        item *new_it;
        new_it = do_item_alloc(ITEM_key(it), it->nkey, atoi(ITEM_suffix(it) + 1), it->exptime, res + 2 );
        if (new_it == 0) {
            return EOM;
        }
        memcpy(ITEM_data(new_it), buf, res);
        memcpy(ITEM_data(new_it) + res, "\r\n", 2);
        item_replace(it, new_it);
        do_item_remove(new_it);       /* release our reference */
    } else { /* replace in-place */
        /* When changing the value without replacing the item, we
           need to update the CAS on the existing item. */
        ITEM_set_cas(it, (settings.use_cas) ? get_cas_id() : 0);

        memcpy(ITEM_data(it), buf, res);
        memset(ITEM_data(it) + res, ' ', it->nbytes - res - 2);
    }

    return OK;
}

static void process_delete_command(conn *c, token_t *tokens, const size_t ntokens) {
    char *key;
    size_t nkey;
    item *it;

    cb_assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;

    if(nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_delete(key, nkey);
    }

    it = item_get(key, nkey);
    if (it) {
        MEMCACHED_COMMAND_DELETE(c->sfd, ITEM_key(it), it->nkey);

        cb_mutex_enter(&c->thread->stats.mutex);
        c->thread->stats.slab_stats[it->slabs_clsid].delete_hits++;
        cb_mutex_exit(&c->thread->stats.mutex);

        item_unlink(it);
        item_remove(it);      /* release our reference */
        out_string(c, "DELETED");
    } else {
        cb_mutex_enter(&c->thread->stats.mutex);
        c->thread->stats.delete_misses++;
        cb_mutex_exit(&c->thread->stats.mutex);

        out_string(c, "NOT_FOUND");
    }
}

void process_verbosity_command(conn *c, token_t *tokens, const size_t ntokens) {
    unsigned int level;

    cb_assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);
    if (c->noreply && ntokens == 3) {
        /* "verbosity noreply" is not according to the correct syntax */
        c->noreply = false;
        out_string(c, "ERROR");
        return;
    }

    if (safe_strtoul(tokens[1].value, &level)) {
        settings.verbose = level > MAX_VERBOSITY_LEVEL ? MAX_VERBOSITY_LEVEL : level;
        out_string(c, "OK");
    } else {
        out_string(c, "ERROR");
    }
}

void process_command(conn *c, char *command) {

    token_t tokens[MAX_TOKENS];
    size_t ntokens;
    int comm;

    cb_assert(c != NULL);

    MEMCACHED_PROCESS_COMMAND_START(c->sfd, c->rcurr, c->rbytes);

    if (settings.verbose > 1)
        moxi_log_write("<%d %s\n", c->sfd, command);

    /*
     * for commands set/add/replace, we build an item and read the data
     * directly into it, then continue in nread_complete().
     */

    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    if (add_msghdr(c) != 0) {
        out_string(c, "SERVER_ERROR out of memory preparing response");
        return;
    }

    ntokens = tokenize_command(command, tokens, MAX_TOKENS);
    if (ntokens >= 3 &&
        ((strcmp(tokens[COMMAND_TOKEN].value, "get") == 0) ||
         (strcmp(tokens[COMMAND_TOKEN].value, "bget") == 0))) {

        process_get_command(c, tokens, ntokens, false);

    } else if ((ntokens == 6 || ntokens == 7) &&
               ((strcmp(tokens[COMMAND_TOKEN].value, "add") == 0 && (comm = NREAD_ADD)) ||
                (strcmp(tokens[COMMAND_TOKEN].value, "set") == 0 && (comm = NREAD_SET)) ||
                (strcmp(tokens[COMMAND_TOKEN].value, "replace") == 0 && (comm = NREAD_REPLACE)) ||
                (strcmp(tokens[COMMAND_TOKEN].value, "prepend") == 0 && (comm = NREAD_PREPEND)) ||
                (strcmp(tokens[COMMAND_TOKEN].value, "append") == 0 && (comm = NREAD_APPEND)) )) {

        process_update_command(c, tokens, ntokens, comm, false);

    } else if ((ntokens == 7 || ntokens == 8) && (strcmp(tokens[COMMAND_TOKEN].value, "cas") == 0 && (comm = NREAD_CAS))) {

        process_update_command(c, tokens, ntokens, comm, true);

    } else if ((ntokens == 4 || ntokens == 5) && (strcmp(tokens[COMMAND_TOKEN].value, "incr") == 0)) {

        process_arithmetic_command(c, tokens, ntokens, 1);

    } else if (ntokens >= 3 && (strcmp(tokens[COMMAND_TOKEN].value, "gets") == 0)) {

        process_get_command(c, tokens, ntokens, true);

    } else if ((ntokens == 4 || ntokens == 5) && (strcmp(tokens[COMMAND_TOKEN].value, "decr") == 0)) {

        process_arithmetic_command(c, tokens, ntokens, 0);

    } else if (ntokens >= 3 && ntokens <= 4 && (strcmp(tokens[COMMAND_TOKEN].value, "delete") == 0)) {

        process_delete_command(c, tokens, ntokens);

    } else if (ntokens >= 2 && (strcmp(tokens[COMMAND_TOKEN].value, "stats") == 0)) {

        process_stat(c, tokens, ntokens);

    } else if (ntokens >= 2 && ntokens <= 4 && (strcmp(tokens[COMMAND_TOKEN].value, "flush_all") == 0)) {
        time_t exptime = 0;
        set_current_time();

        set_noreply_maybe(c, tokens, ntokens);

        cb_mutex_enter(&c->thread->stats.mutex);
        c->thread->stats.flush_cmds++;
        cb_mutex_exit(&c->thread->stats.mutex);

        if(ntokens == (c->noreply ? 3 : 2)) {
            settings.oldest_live = current_time - 1;
            item_flush_expired();
            out_string(c, "OK");
            return;
        }

        exptime = strtol(tokens[1].value, NULL, 10);
        if(errno == ERANGE) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        /*
          If exptime is zero realtime() would return zero too, and
          realtime(exptime) - 1 would overflow to the max unsigned
          value.  So we process exptime == 0 the same way we do when
          no delay is given at all.
        */
        if (exptime > 0)
            settings.oldest_live = c->funcs->conn_realtime(exptime) - 1;
        else /* exptime == 0 */
            settings.oldest_live = current_time - 1;
        item_flush_expired();
        out_string(c, "OK");
        return;

    } else if (ntokens == 2 && (strcmp(tokens[COMMAND_TOKEN].value, "version") == 0)) {

        out_string(c, "VERSION " VERSION);

    } else if (ntokens == 2 && (strcmp(tokens[COMMAND_TOKEN].value, "quit") == 0)) {

        conn_set_state(c, conn_closing);

    } else if (ntokens == 5 && (strcmp(tokens[COMMAND_TOKEN].value, "slabs") == 0 &&
                                strcmp(tokens[COMMAND_TOKEN + 1].value, "reassign") == 0)) {
#ifdef ALLOW_SLABS_REASSIGN

        int src, dst, rv;

        src = strtol(tokens[2].value, NULL, 10);
        dst  = strtol(tokens[3].value, NULL, 10);

        if(errno == ERANGE) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        rv = slabs_reassign(src, dst);
        if (rv == 1) {
            out_string(c, "DONE");
            return;
        }
        if (rv == 0) {
            out_string(c, "CANT");
            return;
        }
        if (rv == -1) {
            out_string(c, "BUSY");
            return;
        }
#else
        out_string(c, "CLIENT_ERROR Slab reassignment not supported");
#endif
    } else if ((ntokens == 3 || ntokens == 4) && (strcmp(tokens[COMMAND_TOKEN].value, "verbosity") == 0)) {
        process_verbosity_command(c, tokens, ntokens);
    } else {
        out_string(c, "ERROR");
    }
    return;
}

void process_stats_proxy_command(conn *c, token_t *tokens, const size_t ntokens) {
    if (ntokens == 4 && strcmp(tokens[2].value, "reset") == 0) {
        proxy_td *ptd = c->extra;
        if (ptd != NULL) {
            proxy_stats_reset(ptd->proxy->main);
        }

        out_string(c, "OK");
        return;
    }

    if (ntokens == 4 && strcmp(tokens[2].value, "timings") == 0) {
        proxy_stats_dump_timings(&append_stats, c);
    } else if (ntokens == 4 && strcmp(tokens[2].value, "config") == 0) {
        proxy_stats_dump_config(&append_stats, c);
    } else {
        bool do_all = (ntokens == 3 || strcmp(tokens[2].value, "all") == 0);
        struct proxy_stats_cmd_info psci = {
            .do_info       = (do_all || strcmp(tokens[2].value, "info") == 0),
            .do_settings   = (do_all || strcmp(tokens[2].value, "settings") == 0),
            .do_behaviors  = (do_all || strcmp(tokens[2].value, "behaviors") == 0),
            .do_frontcache = (do_all || strcmp(tokens[2].value, "frontcache") == 0),
            .do_keystats   = (do_all || strcmp(tokens[2].value, "keystats") == 0),
            .do_stats      = (do_all || strcmp(tokens[2].value, "stats") == 0),
            .do_zeros      = (do_all || ntokens == 4)
        };

        if (psci.do_info) {
            proxy_stats_dump_basic(&append_stats, c, "basic:");
        }

        if (psci.do_settings) {
            process_stat_settings(&append_stats, c, "memcached:settings:");
        }

        if (psci.do_stats) {
            server_stats(&append_stats, c, "memcached:stats:" );
        }

        proxy_stats_dump_proxy_main(&append_stats, c, &psci);

        proxy_stats_dump_proxies(&append_stats, c, &psci);
    }

    /* append terminator and start the transfer */
    append_stats(NULL, 0, NULL, 0, c);

    if (c->stats.buffer == NULL) {
        out_string(c, "SERVER_ERROR out of memory writing stats");
    } else {
        write_and_free(c, c->stats.buffer, c->stats.offset);
        c->stats.buffer = NULL;
    }
}

/*
 * if we have a complete line in the buffer, process it.
 */
int try_read_command(conn *c) {
    cb_assert(c != NULL);
    cb_assert(c->rcurr <= (c->rbuf + c->rsize));
    cb_assert(c->rbytes > 0);

    if (IS_NEGOTIATING(c->protocol) || c->transport == udp_transport)  {
        if ((unsigned char)c->rbuf[0] == (unsigned char)PROTOCOL_BINARY_REQ) {
            c->protocol = IS_PROXY(c->protocol) ? proxy_upstream_binary_prot : binary_prot;
        } else {
            c->protocol = IS_PROXY(c->protocol) ? proxy_upstream_ascii_prot : ascii_prot;
        }

        if (settings.verbose) {
            moxi_log_write("%d: Client using the %s protocol\n", c->sfd,
                    prot_text(c->protocol));
        }
    }

    if (IS_BINARY(c->protocol)) {
        /* Do we have the complete packet header? */
        if (c->rbytes < (int)sizeof(c->binary_header)) {
            /* need more data! */
            return 0;
        } else {
#ifdef NEED_ALIGN
            if (((long)(c->rcurr)) % 8 != 0) {
                /* must realign input buffer */
                memmove(c->rbuf, c->rcurr, c->rbytes);
                c->rcurr = c->rbuf;
                if (settings.verbose) {
                    moxi_log_write("%d: Realign input buffer\n", c->sfd);
                }
            }
#endif
            protocol_binary_request_header* req;
            req = (protocol_binary_request_header*)c->rcurr;

            if (settings.verbose > 1) {
                /* Dump the packet before we convert it to host order */
                moxi_log_write("<%d Read binary protocol data:\n", c->sfd);
                cproxy_dump_header(c->sfd, (char *) req->bytes);
            }

            c->binary_header = *req;
            c->binary_header.request.keylen = ntohs(req->request.keylen);
            c->binary_header.request.bodylen = ntohl(req->request.bodylen);
            c->binary_header.request.cas = mc_swap64(req->request.cas);

            if (c->binary_header.request.magic != c->funcs->conn_binary_command_magic) {
                if (settings.verbose) {
                    moxi_log_write("Invalid magic:  %x\n",
                            c->binary_header.request.magic);
                }
                conn_set_state(c, conn_closing);
                return -1;
            }

            /*
             * disconnect the client if it tries to send illegal packets
             * or packets that's too big. The max limit is 20Mb, so
             * disconnect the client if it sends > 21.
             */
            if (c->binary_header.request.bodylen > (21 * 1024 * 1024)) {
                if (settings.verbose) {
                    moxi_log_write("Packet too big: 0x%x\n",
                            c->binary_header.request.bodylen);
                }
                conn_set_state(c, conn_closing);
                return -1;
            }

            uint32_t body = c->binary_header.request.keylen;
            body += c->binary_header.request.extlen;
            if (body > c->binary_header.request.bodylen) {
                if (settings.verbose) {
                    moxi_log_write("Content of packet bigger than encoded body: 0x%x > 0x%x\n",
                                   body, c->binary_header.request.bodylen);
                }
                conn_set_state(c, conn_closing);
                return -1;
            }

            c->msgcurr = 0;
            c->msgused = 0;
            c->iovused = 0;
            if (add_msghdr(c) != 0) {
                out_string(c, "SERVER_ERROR out of memory");
                return 0;
            }

            c->cmd = c->binary_header.request.opcode;
            c->keylen = c->binary_header.request.keylen;
            c->opaque = c->binary_header.request.opaque;
            /* clear the returned cas value */
            c->cas = 0;

            c->funcs->conn_process_binary_command(c);

            c->rbytes -= sizeof(c->binary_header);
            c->rcurr += sizeof(c->binary_header);
        }
    } else {
        char *el, *cont;

        if (c->rbytes == 0)
            return 0;
        el = memchr(c->rcurr, '\n', c->rbytes);
        if (!el)
            return 0;
        cont = el + 1;
        if ((el - c->rcurr) > 1 && *(el - 1) == '\r') {
            el--;
        }
        *el = '\0';

        cb_assert(cont <= (c->rcurr + c->rbytes));

        c->funcs->conn_process_ascii_command(c, c->rcurr);

        c->rbytes -= (int)(cont - c->rcurr);
        c->rcurr = cont;

        cb_assert(c->rcurr <= (c->rbuf + c->rsize));
    }

    return 1;
}

/*
 * read a UDP request.
 */
static enum try_read_result try_read_udp(conn *c) {
    int res;

    cb_assert(c != NULL);

    c->request_addr_size = sizeof(c->request_addr);
    res = recvfrom(c->sfd, c->rbuf, c->rsize,
                   0, &c->request_addr, &c->request_addr_size);
    if (res > 8) {
        unsigned char *buf = (unsigned char *)c->rbuf;

        cb_mutex_enter(&c->thread->stats.mutex);
        c->thread->stats.bytes_read += res;
        cb_mutex_exit(&c->thread->stats.mutex);

        add_bytes_read(c, res);

        /* Beginning of UDP packet is the request ID; save it. */
        c->request_id = buf[0] * 256 + buf[1];

        /* If this is a multi-packet request, drop it. */
        if (buf[4] != 0 || buf[5] != 1) {
            out_string(c, "SERVER_ERROR multi-packet request not supported");
            return READ_NO_DATA_RECEIVED;
        }

        /* Don't care about any of the rest of the header. */
        res -= 8;
        memmove(c->rbuf, c->rbuf + 8, res);

        c->rbytes += res;
        c->rcurr = c->rbuf;
        return READ_DATA_RECEIVED;
    }
    return READ_NO_DATA_RECEIVED;
}

/*
 * read from network as much as we can, handle buffer overflow and connection
 * close.
 * before reading, move the remaining incomplete fragment of a command
 * (if any) to the beginning of the buffer.
 * @return enum try_read_result
 */
static enum try_read_result try_read_network(conn *c) {
    enum try_read_result gotdata = READ_NO_DATA_RECEIVED;
    int res;

    cb_assert(c != NULL);

    if (c->rcurr != c->rbuf) {
        if (c->rbytes != 0) /* otherwise there's nothing to copy */
            memmove(c->rbuf, c->rcurr, c->rbytes);
        c->rcurr = c->rbuf;
    }

    while (1) {
        if (c->rbytes >= c->rsize) {
            char *new_rbuf = realloc(c->rbuf, c->rsize * 2);
            if (!new_rbuf) {
                if (settings.verbose > 0)
                    moxi_log_write("Couldn't realloc input buffer\n");
                c->rbytes = 0; /* ignore what we read */
                out_string(c, "SERVER_ERROR out of memory reading request");
                c->write_and_go = conn_closing;
                return READ_MEMORY_ERROR;
            }
            c->rcurr = c->rbuf = new_rbuf;
            c->rsize *= 2;
        }

        int avail = c->rsize - c->rbytes;
        cb_assert(avail > 0);
        res = recv(c->sfd, c->rbuf + c->rbytes, avail, 0);
        if (res > 0) {
            cb_mutex_enter(&c->thread->stats.mutex);
            c->thread->stats.bytes_read += res;
            cb_mutex_exit(&c->thread->stats.mutex);

            add_bytes_read(c, res);

            gotdata = READ_DATA_RECEIVED;
            c->rbytes += res;
            if (res == avail) {
                continue;
            } else {
                break;
            }
        }
        if (res == 0) {
            return READ_ERROR;
        }
        if (res == -1) {
            if (is_blocking(errno)) {
                break;
            }
            return READ_ERROR;
        }
    }
    return gotdata;
}

bool update_event_real(conn *c, const int new_flags, const char *update_diag) {
    return update_event_timed_real(c, new_flags, NULL, update_diag);
}

bool update_event_timed_real(conn *c, const int new_flags, struct timeval *timeout, const char *update_diag) {
    cb_assert(c != NULL);

    struct event_base *base = c->event.ev_base;
    if (c->ev_flags == new_flags && timeout == NULL)
        return true;

    c->update_diag = update_diag;

    if (event_del(&c->event) == -1)
        return false;
    c->ev_flags = new_flags;
    if (new_flags == 0 && timeout == NULL)
        return true;
    event_set(&c->event, c->sfd, new_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    if (event_add(&c->event, timeout) == -1)
        return false;
    return true;
}

/*
 * Sets whether we are listening for new connections or not.
 */
void do_accept_new_conns(const bool do_accept) {
    conn *next;

    for (next = listen_conn; next; next = next->next) {
        if (do_accept) {
            update_event(next, EV_READ | EV_PERSIST);
            if (listen(next->sfd, settings.backlog) != 0) {
                perror("listen");
            }
        }
        else {
            update_event(next, 0);
            if (listen(next->sfd, 0) != 0) {
                perror("listen");
            }
        }
    }

    if (do_accept) {
        STATS_LOCK();
        stats.accepting_conns = true;
        STATS_UNLOCK();
    } else {
        STATS_LOCK();
        stats.accepting_conns = false;
        stats.listen_disabled_num++;
        STATS_UNLOCK();
    }
}

/*
 * Transmit the next chunk of data from our list of msgbuf structures.
 *
 * Returns:
 *   TRANSMIT_COMPLETE   All done writing.
 *   TRANSMIT_INCOMPLETE More data remaining to write.
 *   TRANSMIT_SOFT_ERROR Can't write any more right now.
 *   TRANSMIT_HARD_ERROR Can't write (c->state is set to conn_closing)
 */
static enum transmit_result transmit(conn *c) {
    cb_assert(c != NULL);

    if (c->msgcurr < c->msgused &&
            c->msglist[c->msgcurr].msg_iovlen == 0) {
        /* Finished writing the current msg; advance to the next. */
        c->msgcurr++;
    }
    if (c->msgcurr < c->msgused) {
        ssize_t res;
        struct msghdr *m = &c->msglist[c->msgcurr];
#ifdef WIN32
        DWORD error;
#else
        int error;
#endif

        res = sendmsg(c->sfd, m, 0);
#ifdef WIN32
        error = WSAGetLastError();
#else
        error = errno;
#endif
        if (res > 0) {
            cb_mutex_enter(&c->thread->stats.mutex);
            c->thread->stats.bytes_written += res;
            cb_mutex_exit(&c->thread->stats.mutex);

            /* We've written some of the data. Remove the completed
               iovec entries from the list of pending writes. */
            while (m->msg_iovlen > 0 && res >= (ssize_t)(m->msg_iov->iov_len)) {
                res -= (ssize_t)m->msg_iov->iov_len;
                m->msg_iovlen--;
                m->msg_iov++;
            }

            /* Might have written just part of the last iovec entry;
               adjust it so the next write will do the rest. */
            if (res > 0) {
                m->msg_iov->iov_base = (caddr_t)m->msg_iov->iov_base + res;
                m->msg_iov->iov_len -= res;
            }
            return TRANSMIT_INCOMPLETE;
        }

        if (res == -1 && is_blocking(error)) {
            if (!update_event(c, EV_WRITE | EV_PERSIST)) {
                if (settings.verbose > 0)
                    moxi_log_write("Couldn't update event\n");
                conn_set_state(c, conn_closing);
                return TRANSMIT_HARD_ERROR;
            }
            return TRANSMIT_SOFT_ERROR;
        }
        /* if res == 0 or res == -1 and error is not EAGAIN or EWOULDBLOCK,
           we have a real error, on which we close the connection */
        if (settings.verbose > 0) {
            moxi_log_write("%u: Failed to write, and not due to blocking: %s\n",
                           c->sfd, strerror(error));
        }

        if (IS_UDP(c->transport))
            conn_set_state(c, conn_read);
        else
            conn_set_state(c, conn_closing);
        return TRANSMIT_HARD_ERROR;
    } else {
        return TRANSMIT_COMPLETE;
    }
}

void drive_machine(conn *c) {
    bool stop = false;
    SOCKET sfd;
    socklen_t addrlen;
    struct sockaddr_storage addr;
    int nreqs = settings.reqs_per_event;
    int res;
#ifdef WIN32
    DWORD error;
#else
    int error;
#endif

    cb_assert(c != NULL);

    while (!stop) {
        if (settings.verbose > 2) {
            moxi_log_write("%d: drive_machine %s\n",
                    c->sfd, state_text(c->state));
        }

        switch(c->state) {
        case conn_listening:
            addrlen = sizeof(addr);
            if ((sfd = accept(c->sfd, (struct sockaddr *)&addr, &addrlen)) == INVALID_SOCKET) {
#ifdef WIN32
                error = WSAGetLastError();
#else
                error = errno;
#endif
                if (is_blocking(error)) {
                    /* these are transient, so don't log anything */
                    stop = true;
                } else if (is_emfile(error)) {
                    if (settings.verbose > 0)
                        moxi_log_write("Too many open connections\n");
                    accept_new_conns(false);
                    stop = true;
                } else {
                    perror("accept()");
                    stop = true;
                }
                break;
            }
            if (evutil_make_socket_nonblocking(sfd) == -1) {
                perror("setting O_NONBLOCK");
                closesocket(sfd);
                break;
            }

            dispatch_conn_new(sfd, conn_new_cmd, EV_READ | EV_PERSIST,
                              DATA_BUFFER_SIZE,
                              c->protocol,
                              tcp_transport,
                              c->funcs, c->extra);
            stop = true;
            break;

        case conn_connecting:
            if (c->funcs->conn_connect != NULL) {
                if (c->funcs->conn_connect(c) == true) {
                    stop = true;
                }
            } else {
                conn_set_state(c, conn_closing);
                update_event(c, 0);
            }
            break;

        case conn_waiting:
            if (!update_event(c, EV_READ | EV_PERSIST)) {
                if (settings.verbose > 0)
                    moxi_log_write("Couldn't update event\n");
                conn_set_state(c, conn_closing);
                break;
            }

            conn_set_state(c, conn_read);
            stop = true;
            break;

        case conn_read:
            res = IS_UDP(c->transport) ? try_read_udp(c) : try_read_network(c);

            switch (res) {
            case READ_NO_DATA_RECEIVED:
                conn_set_state(c, conn_waiting);
                break;
            case READ_DATA_RECEIVED:
                conn_set_state(c, conn_parse_cmd);
                break;
            case READ_ERROR:
                conn_set_state(c, conn_closing);
                break;
            case READ_MEMORY_ERROR: /* Failed to allocate more memory */
                /* State already set by try_read_network */
                break;
            }
            break;

        case conn_parse_cmd:
            if (try_read_command(c) == 0) {
                /* wee need more data! */
                conn_set_state(c, conn_waiting);
            }

            break;

        case conn_new_cmd:
            /* Only process nreqs at a time to avoid starving other
               connections */

            --nreqs;
            if (IS_DOWNSTREAM(c->protocol) || nreqs >= 0) {
                reset_cmd_handler(c);
            } else {
                cb_mutex_enter(&c->thread->stats.mutex);
                c->thread->stats.conn_yields++;
                cb_mutex_exit(&c->thread->stats.mutex);
                if (c->rbytes > 0) {
                    /* We have already read in data into the input buffer,
                       so libevent will most likely not signal read events
                       on the socket (unless more data is available. As a
                       hack we should just put in a request to write data,
                       because that should be possible ;-)
                    */
                    if (!update_event(c, EV_WRITE | EV_PERSIST)) {
                        if (settings.verbose > 0) {
                            moxi_log_write("Couldn't update event\n");
                        }

                        conn_set_state(c, conn_closing);
                    }
                }
                stop = true;
            }
            break;

        case conn_nread:
            cb_assert(c->rlbytes >= 0);
            if (c->rlbytes == 0) {
                complete_nread(c);
                break;
            }
            /* first check if we have leftovers in the conn_read buffer */
            if (c->rbytes > 0) {
                int tocopy = c->rbytes > c->rlbytes ? c->rlbytes : c->rbytes;
                if (c->ritem != c->rcurr) {
                    memmove(c->ritem, c->rcurr, tocopy);
                }
                c->ritem += tocopy;
                c->rlbytes -= tocopy;
                c->rcurr += tocopy;
                c->rbytes -= tocopy;
                if (c->rlbytes == 0) {
                    break;
                }
            }

            /*  now try reading from the socket */
            res = recv(c->sfd, c->ritem, c->rlbytes, 0);
#ifdef WIN32
            error = WSAGetLastError();
#else
            error = errno;
#endif

            if (res > 0) {
                add_bytes_read(c, res);

                if (c->rcurr == c->ritem) {
                    c->rcurr += res;
                }
                c->ritem += res;
                c->rlbytes -= res;
                break;
            }
            if (res == 0) { /* end of stream */
                conn_set_state(c, conn_closing);
                break;
            }
            if (res == -1 && is_blocking(error)) {
                if (!update_event(c, EV_READ | EV_PERSIST)) {
                    if (settings.verbose > 0)
                        moxi_log_write( "Couldn't update event\n");
                    conn_set_state(c, conn_closing);
                    break;
                }
                stop = true;
                break;
            }
            /* otherwise we have a real error, on which we close the connection */
            if (!is_closed_conn(error) && settings.verbose > 0) {
                moxi_log_write("Failed to read, and not due to blocking:\n"
                        "errno: %d %s \n"
                        "rcurr=%lx ritem=%lx rbuf=%lx rlbytes=%d rsize=%d\n",
                        errno, strerror(errno),
                        (long)c->rcurr, (long)c->ritem, (long)c->rbuf,
                        (int)c->rlbytes, (int)c->rsize);
            }
            conn_set_state(c, conn_closing);
            break;

        case conn_swallow:
            /* we are reading sbytes and throwing them away */
            if (c->sbytes == 0) {
                conn_set_state(c, conn_new_cmd);
                break;
            }

            /* first check if we have leftovers in the conn_read buffer */
            if (c->rbytes > 0) {
                int tocopy = c->rbytes > c->sbytes ? c->sbytes : c->rbytes;
                c->sbytes -= tocopy;
                c->rcurr += tocopy;
                c->rbytes -= tocopy;
                break;
            }

            /*  now try reading from the socket */
            res = recv(c->sfd, c->rbuf, c->rsize > c->sbytes ? c->sbytes : c->rsize, 0);
#ifdef WIN32
            error = WSAGetLastError();
#else
            error = errno;
#endif

            if (res > 0) {
                cb_mutex_enter(&c->thread->stats.mutex);
                c->thread->stats.bytes_read += res;
                cb_mutex_exit(&c->thread->stats.mutex);
                add_bytes_read(c, res);
                c->sbytes -= res;
                break;
            }
            if (res == 0) { /* end of stream */
                conn_set_state(c, conn_closing);
                break;
            }
            if (res == -1 && is_blocking(error)) {
                if (!update_event(c, EV_READ | EV_PERSIST)) {
                    if (settings.verbose > 0)
                        moxi_log_write("Couldn't update event\n");
                    conn_set_state(c, conn_closing);
                    break;
                }
                stop = true;
                break;
            }
            /* otherwise we have a real error, on which we close the connection */
            if (!is_closed_conn(error) && settings.verbose > 0) {
                moxi_log_write("Failed to read, and not due to blocking\n");
            }
            conn_set_state(c, conn_closing);
            break;

        case conn_write:
            /*
             * We want to write out a simple response. If we haven't already,
             * assemble it into a msgbuf list (this will be a single-entry
             * list for TCP or a two-entry list for UDP).
             */
            if (c->iovused == 0 || (IS_UDP(c->transport) && c->iovused == 1)) {
                if (add_iov(c, c->wcurr, c->wbytes) != 0) {
                    if (settings.verbose > 0)
                        moxi_log_write("Couldn't build response\n");
                    conn_set_state(c, conn_closing);
                    break;
                }
            }

            /* fall through... */

        case conn_mwrite:
          if (IS_UDP(c->transport) && c->msgcurr == 0 && build_udp_headers(c) != 0) {
            if (settings.verbose > 0)
              moxi_log_write("Failed to build UDP headers\n");
            conn_set_state(c, conn_closing);
            break;
          }
            switch (transmit(c)) {
            case TRANSMIT_COMPLETE:
                if (c->state == conn_mwrite) {
                    while (c->ileft > 0) {
                        item *it = *(c->icurr);
                        cb_assert((it->it_flags & ITEM_SLABBED) == 0);
                        item_remove(it);
                        c->icurr++;
                        c->ileft--;
                    }
                    while (c->suffixleft > 0) {
                        char *suffix = *(c->suffixcurr);
                        cache_free(c->thread->suffix_cache, suffix);
                        c->suffixcurr++;
                        c->suffixleft--;
                    }
                    conn_set_state(c, c->write_and_go);
                } else if (c->state == conn_write) {
                    if (c->write_and_free) {
                        free(c->write_and_free);
                        c->write_and_free = 0;
                    }
                    conn_set_state(c, c->write_and_go);
                } else {
                    if (settings.verbose > 0)
                        moxi_log_write("Unexpected state %d\n", c->state);
                    conn_set_state(c, conn_closing);
                }
                break;

            case TRANSMIT_INCOMPLETE:
            case TRANSMIT_HARD_ERROR:
                break;                   /* Continue in state machine. */

            case TRANSMIT_SOFT_ERROR:
                stop = true;
                break;
            }
            break;

        case conn_pause:
            /* In case whoever put us into conn_pause didn't clear out */
            /* libevent registration, do so now. */

            update_event(c, 0);

            if (c->funcs->conn_pause != NULL)
                c->funcs->conn_pause(c);

            stop = true;
            break;

        case conn_closing:
            if (c->funcs->conn_close != NULL)
                c->funcs->conn_close(c);

            if (IS_UDP(c->transport))
                conn_cleanup(c);
            else
                conn_close(c);
            stop = true;
            break;

        case conn_max_state:
            cb_assert(false);
            break;
        }
    }

    return;
}

void add_bytes_read(conn *c, int bytes_read) {
    cb_assert(c != NULL);
    cb_mutex_enter(&c->thread->stats.mutex);
    c->thread->stats.bytes_read += bytes_read;
    cb_mutex_exit(&c->thread->stats.mutex);
}

static void event_handler(evutil_socket_t fd, short which, void *arg) {
    conn *c;

    c = (conn *)arg;
    cb_assert(c != NULL);

    c->which = which;

    /* sanity */
    if (fd != c->sfd) {
        if (settings.verbose > 0)
            moxi_log_write("Catastrophic: event fd doesn't match conn fd!\n");
        conn_close(c);
        return;
    }

    c->update_diag = "working";

    drive_machine(c);

    /* wait for next event */
    return;
}

static SOCKET new_socket(struct addrinfo *ai) {
    SOCKET sfd;

    if ((sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)) == INVALID_SOCKET) {
        return INVALID_SOCKET;
    }

    if (evutil_make_socket_nonblocking(sfd) == -1) {
        perror("setting O_NONBLOCK");
        closesocket(sfd);
        return INVALID_SOCKET;
    }
    return sfd;
}


/*
 * Sets a socket's send buffer size to the maximum allowed by the system.
 */
static void maximize_sndbuf(const SOCKET sfd) {
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int min, max, avg;
    int old_size;

    /* Start with the default size. */
    if (getsockopt(sfd, SOL_SOCKET, SO_SNDBUF, (void*)&old_size, &intsize) != 0) {
        if (settings.verbose > 0)
            perror("getsockopt(SO_SNDBUF)");
        return;
    }

    /* Binary-search for the real maximum. */
    min = old_size;
    max = MAX_SENDBUF_SIZE;

    while (min <= max) {
        avg = ((unsigned int)(min + max)) / 2;
        if (setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, (void *)&avg, intsize) == 0) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }

    if (settings.verbose > 1)
        moxi_log_write("<%d send buffer was %d, now %d\n", sfd, old_size, last_good);
}

/**
 * Create a socket and bind it to a specific port number
 * @param port the port number to bind to
 * @param transport the transport protocol (TCP / UDP)
 * @param portnumber_file A filepointer to write the port numbers to
 *        when they are successfully added to the list of ports we
 *        listen on.
 */
int server_socket(int port, enum network_transport transport,
                  FILE *portnumber_file) {
    SOCKET sfd;
    struct linger ling = {0, 0};
    struct addrinfo *ai;
    struct addrinfo *next;
    struct addrinfo hints = { .ai_flags = AI_PASSIVE,
                              .ai_family = AF_UNSPEC };
    char port_buf[NI_MAXSERV];
    int error;
    int success = 0;
    int flags =1;

    hints.ai_socktype = IS_UDP(transport) ? SOCK_DGRAM : SOCK_STREAM;

    if (port == EPHEMERAL) {
        port = 0;
    }
    snprintf(port_buf, sizeof(port_buf), "%d", port);
    error= getaddrinfo(settings.inter, port_buf, &hints, &ai);
    if (error != 0) {
#ifdef WIN32
        char* win_msg = NULL;
        DWORD err = WSAGetLastError();
        FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                       FORMAT_MESSAGE_FROM_SYSTEM |
                       FORMAT_MESSAGE_IGNORE_INSERTS,
                       NULL, err,
                       MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                       (LPTSTR)&win_msg,
                       0, NULL);
        moxi_log_write("getaddrinfo(): %s\n", win_msg);
        LocalFree(win_msg);
#else
        if (error != EAI_SYSTEM)
          moxi_log_write("getaddrinfo(): %s\n", gai_strerror(error));
        else
          perror("getaddrinfo()");
#endif
        return 1;
    }

    for (next= ai; next; next= next->ai_next) {
        conn *listen_conn_add;
        if ((sfd = new_socket(next)) == INVALID_SOCKET) {
            /* getaddrinfo can return "junk" addresses,
             * we make sure at least one works before erroring.
             */
            continue;
        }

#ifdef IPV6_V6ONLY
        if (next->ai_family == AF_INET6) {
            error = setsockopt(sfd, IPPROTO_IPV6, IPV6_V6ONLY, (char *) &flags, sizeof(flags));
            if (error != 0) {
                perror("setsockopt");
                closesocket(sfd);
                continue;
            }
        }
#endif

        setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
        if (IS_UDP(transport)) {
            maximize_sndbuf(sfd);
        } else {
            error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
            if (error != 0)
                perror("setsockopt");

            error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
            if (error != 0)
                perror("setsockopt");

            error = setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));
            if (error != 0)
                perror("setsockopt");
        }

        if (bind(sfd, next->ai_addr, next->ai_addrlen) == SOCKET_ERROR) {
#ifdef WIN32
            DWORD error = WSAGetLastError();
#else
            int error = errno;
#endif
            if (!is_addrinuse(error)) {
                perror("bind()");
                closesocket(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            closesocket(sfd);
            continue;
        } else {
            success++;
            if (!IS_UDP(transport) && listen(sfd, settings.backlog) == SOCKET_ERROR) {
                perror("listen()");
                closesocket(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            if (portnumber_file != NULL &&
                (next->ai_addr->sa_family == AF_INET ||
                 next->ai_addr->sa_family == AF_INET6)) {
                union {
                    struct sockaddr_in in;
                    struct sockaddr_in6 in6;
                } my_sockaddr;
                socklen_t len = sizeof(my_sockaddr);
                if (getsockname(sfd, (struct sockaddr*)&my_sockaddr, &len)==0) {
                    if (next->ai_addr->sa_family == AF_INET) {
                        fprintf(portnumber_file, "%s INET: %u\n",
                                IS_UDP(transport) ? "UDP" : "TCP",
                                ntohs(my_sockaddr.in.sin_port));
                    } else {
                        fprintf(portnumber_file, "%s INET6: %u\n",
                                IS_UDP(transport) ? "UDP" : "TCP",
                                ntohs(my_sockaddr.in6.sin6_port));
                    }
                }
            }
        }

        if (IS_UDP(transport)) {
            int c;

            for (c = 1; c < settings.num_threads; c++) {
                /* this is guaranteed to hit all threads because we round-robin */
                dispatch_conn_new(sfd, conn_read, EV_READ | EV_PERSIST,
                                  UDP_READ_BUFFER_SIZE, settings.binding_protocol, transport,
                                  NULL, NULL);
            }
        } else {
            if (!(listen_conn_add = conn_new(sfd, conn_listening,
                                             EV_READ | EV_PERSIST, 1,
                                             transport,
                                             main_base, NULL, NULL))) {
                moxi_log_write("ERROR: failed to create listening connection\n");
                exit(EXIT_FAILURE);
            }
            listen_conn_add->next = listen_conn;
            listen_conn = listen_conn_add;
        }
    }

    freeaddrinfo(ai);

    /* Return zero iff we detected no errors in starting up connections */
    return success == 0;
}

static SOCKET new_socket_unix(void) {
    SOCKET sfd;

    if ((sfd = socket(AF_UNIX, SOCK_STREAM, 0)) == INVALID_SOCKET) {
        perror("socket()");
        return INVALID_SOCKET;
    }

    if (evutil_make_socket_nonblocking(sfd) == -1) {
        perror("setting O_NONBLOCK");
        closesocket(sfd);
        return INVALID_SOCKET;
    }

    return sfd;
}

#ifdef HAVE_SYS_UN_H
int server_socket_unix(const char *path, int access_mask) {
    int sfd;
    struct linger ling = {0, 0};
    struct sockaddr_un addr;
    struct stat tstat;
    int flags =1;
    int old_umask;

    if (!path) {
        return 1;
    }

    if ((sfd = new_socket_unix()) == -1) {
        return 1;
    }

    /*
     * Clean up a previous socket file if we left it around
     */
    if (lstat(path, &tstat) == 0) {
        if (S_ISSOCK(tstat.st_mode))
            unlink(path);
    }

    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
    setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
    setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));

    /*
     * the memset call clears nonstandard fields in some impementations
     * that otherwise mess things up.
     */
    memset(&addr, 0, sizeof(addr));

    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    cb_assert(strcmp(addr.sun_path, path) == 0);
    old_umask = umask( ~(access_mask&0777));
    if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) == SOCKET_ERROR) {
        perror("bind()");
        closesocket(sfd);
        umask(old_umask);
        return 1;
    }
    umask(old_umask);
    if (listen(sfd, settings.backlog) == SOCKET_ERROR) {
        perror("listen()");
        closesocket(sfd);
        return 1;
    }
    if (!(listen_conn = conn_new(sfd, conn_listening,
                                 EV_READ | EV_PERSIST, 1,
                                 local_transport, main_base,
                                 NULL, NULL))) {
        moxi_log_write("failed to create listening connection\n");
        exit(EXIT_FAILURE);
    }

    return 0;
}
#endif

/*
 * We keep the current time of day in a global variable that's updated by a
 * timer event. This saves us a bunch of time() system calls (we really only
 * need to get the time once a second, whereas there can be tens of thousands
 * of requests a second) and allows us to use server-start-relative timestamps
 * rather than absolute UNIX timestamps, a space savings on systems where
 * sizeof(time_t) > sizeof(unsigned int).
 */
volatile rel_time_t current_time;
static struct event clockevent;

/* time-sensitive callers can call it by hand with this, outside the normal ever-1-second timer */
static void set_current_time(void) {
    struct timeval timer;

    gettimeofday(&timer, NULL);
    current_time = (rel_time_t) (timer.tv_sec - process_started);
}

static void clock_handler(evutil_socket_t fd, const short which, void *arg) {
    struct timeval t = {.tv_sec = 1, .tv_usec = 0};
    static bool initialized = false;

    (void)fd;
    (void)which;
    (void)arg;

    if (initialized) {
        /* only delete the event if it's actually there. */
        evtimer_del(&clockevent);
    } else {
        initialized = true;
    }

    evtimer_set(&clockevent, clock_handler, 0);
    event_base_set(main_base, &clockevent);
    evtimer_add(&clockevent, &t);

    set_current_time();
}

static void usage(char **argv) {
    (void) argv;

    char *m = "moxi";

    proxy_behavior *b = &behavior_default_g;

    printf(PACKAGE " " VERSION "\n");
    printf("\n");
    printf("Usage:\n");
    printf("  %s [FLAGS] URL1[,URL2[,URLn]]\n", m);
    printf("  %s [FLAGS] -z url=URL1[,URL2[,URLn]]\n", m);
    printf("  %s [FLAGS] -z LOCAL_PORT=MCHOST[:MCPORT][,MCHOST2[:MCPORT2][,*]]\n", m);
    printf("\n");
    printf("The -z parameter specifies a 'cluster configuration' that tells\n");
    printf("moxi which servers to communicate with.\n");
    printf("\n");
    printf("The URL approach allows you to specify the cluster configuration\n"
           "via dynamic HTTP/REST-based lookup.  Example:\n"
           "  %s http://127.0.0.1:8091/pools/default/bucketsStreaming/default\n", m);
    printf("\n"
           "You can also specify multiple cluster configuration URL's for higher\n"
           "availability, using comma-separated URL's (no whitespace).  A URL list\n"
           "as the last parameter is also assumed to be a -z cluster configuration.\n");
    printf("\n");
    printf("The MCHOST:MCPORT approach allows you to specify the cluster\n"
           "configuration for libmemcached/ketama hashing, where moxi listens\n"
           "on the given LOCAL_PORT and forwards to downstream memcached servers\n"
           "running at MCHOST:MCPORT.  More than one MCHOST:MCPORT can be listed,\n"
           "separated by commas.  Example:\n"
           "  %s -z 11211=mc_server1:11211,mc_server2:11211\n", m);
    printf("\n");
    printf("The -z cluster configuration can be also specified in a config file:\n");
    printf("  %s [FLAGS] -z /path/to/absolute/configFile\n", m);
    printf("  %s [FLAGS] -z ./path/to/relative/configFile\n", m);
    printf("\n");
    printf("The optional FLAGS are...\n\n");
    printf("-Z <key=val*> optional 'proxy configuration'.  See more below.\n");
#ifdef HAVE_GETPWNAM
    printf("-u <username> assume identity of <username> (only when run as root)\n");
#endif
    printf("-c <num>      max simultaneous connections (default: 1024)\n"
           "-v            verbose (print errors/warnings while in event loop)\n"
           "-vv           very verbose (also print client commands/reponses)\n"
           "-vvv          extremely verbose (also print internal state transitions)\n"
           "-h            print this help and exit\n"
           "-i            print moxi, memcached and libevent license\n"
           "-a <mask>     access mask for UNIX socket, in octal (default: 0700)\n"
           "-l <addr>     interface to listen on\n"
           "-d            run as a daemon\n"
           "-P <file>     save PID in <file>, only used with -d option\n");
    printf("-t <num>      number of threads to use (default: 4)\n");
    printf("-R            maximum number of requests per event, limits the number of\n"
           "              requests process for a given connection to prevent \n"
           "              starvation (default: 20)\n");
    printf("-b            set the backlog queue limit (default: 1024)\n");
    printf("-B            binding protocol - one of ascii, binary, or auto (default)\n");
    printf("-Y <y|n>      exit when stdin closes (default: n)\n");
#ifdef HAVE_SYS_UN_H
    printf("-s <file>     UNIX socket path to listen on (disables network support)\n");
#endif
    printf("-p <num>      (deprecated) TCP port number (default: 0 (off)) where moxi can\n"
           "              listen and run as a memcached server.  To specify\n"
           "              a TCP port number that moxi will listen as a proxy,\n"
           "              instead, use: -Z port_listen=PORT_NUM\n"
           "-U <num>      (deprecated) UDP port number (default: 0 (off)) where moxi can\n"
           "              listen can run as a memcached server\n"
           "-k            (deprecated) lock down all paged memory.  Note that there is a\n"
           "              limit on how much memory you may lock.  Trying to\n"
           "              allocate more than that would fail, so be sure you\n"
           "              set the limit correctly for the user you started\n"
           "              the daemon with (not for -u <username> user;\n"
           "              under sh this is done with 'ulimit -S -l NUM_KB').\n"
           "-m <num>      (deprecated) max memory for items in megabytes (default: 64 MB)\n"
           "-M            (deprecated) return error on memory exhausted\n"
           "-r            Raise the core file size limit to the maximum allowable.\n"
           "              Also suppresses 'chdir(\"/\")' at startup when running as a daemon\n"
           "              to allow control over core dump location.\n"
           "-f <factor>   (deprecated) chunk size growth factor (default: 1.25)\n"
           "-n <bytes>    (deprecated) minimum allocated for key+value+flags (default: 48)\n"
#if defined(HAVE_GETPAGESIZES) && defined(HAVE_MEMCNTL)
           "-L            (deprecated) try to use large memory pages (if available). Increasing\n"
           "              the memory page size could reduce the number of TLB misses\n"
           "              and improve the performance. In order to get large pages\n"
           "              from the OS, memcached will allocate the total item-cache\n"
           "              in one large chunk.\n"
#endif
           "-D <char>     (deprecated) use <char> as delimiter for key prefixes and IDs.\n"
           "              This is used for per-prefix stats reporting. The default is\n"
           "              \":\" (colon). If this option is specified, stats collection\n"
           "              is turned on automatically; if not, then it may be turned on\n"
           "              by sending the \"stats detail on\" command to the server.\n"
           "-C            (deprecated) disable use of CAS\n"
           "-O <log path> moxi log file name.\n"
           "-X            enable mcmux compatibility; disables libvbucket & libmemcached\n");
    printf("\n"
           "The proxy configuration flag, -Z, is a comma-separated list of key=value\n"
           "pairs, which specify additional proxy behavior.  The more useful proxy\n"
           "configuration keys include the following, with their default values...\n");
    printf("  port_listen=%d\n", b->port_listen);
    printf("      Port number that moxi will listen on, if not otherwise specified.\n");
    printf("  usr=<USR, none by default>\n");
    printf("  pwd=<PWD, none by default>\n");
    printf("      User/password that moxi will use for SASL plain or HTTP basic auth.\n");
    printf("      You can also define these using the MOXI_SASL_PLAIN_USR\n"
           "      and MOXI_SASL_PLAIN_PWD environment variables.\n");
    printf("  default_bucket_name=<use the first bucket>\n"
           "      When unspecified, moxi will treat the first bucket it sees as\n"
           "      the default bucket.  This is the bucket that new clients will\n"
           "      use when they first connect.\n");
    printf("  concurrency=%d\n", b->downstream_max);
    printf("      Number of requests that moxi will process concurrently\n"
           "      per worker thread and per bucket.  Requests will otherwise go\n"
           "      onto the tail of a wait queue.  0 means no limit to concurrency.\n"
           "      This parameter is also (confusingly) known as 'downstream_max'\n");
    printf("  wait_queue_timeout=%ld\n",
           b->wait_queue_timeout.tv_sec * 1000 +
           b->wait_queue_timeout.tv_usec / 1000);
    printf("      Millisecs before moxi will timeout requests that have stayed\n"
           "      too long on the wait queue.  0 means no timeout.\n");
    printf("  connect_timeout=%ld\n",
           b->connect_timeout.tv_sec * 1000 +
           b->connect_timeout.tv_usec / 1000);
    printf("      Millisecs before moxi will timeout a connect() attempt.\n"
           "      0 means no timeout.\n");
    printf("  auth_timeout=%ld\n",
           b->auth_timeout.tv_sec * 1000 +
           b->auth_timeout.tv_usec / 1000);
    printf("      Millisecs before moxi will timeout a SASL auth attempt.\n"
           "      0 means no timeout.\n");
    printf("  connect_max_errors=%d\n", b->connect_max_errors);
    printf("      Max number of errors per host:port:bucket per worker thread\n"
           "      before moxi blacklists an unresponsive host:port:bucket.\n"
           "      0 means blacklisting is disabled.\n");
    printf("  connect_retry_interval=%d\n", b->connect_retry_interval);
    printf("      Millisecs that a host:port:bucket will be blacklisted\n"
           "      before moxi tries again to contact the host:port:bucket.\n"
           "      0 means blacklisting is disabled.\n");
    printf("  downstream_conn_max=%d\n", b->downstream_conn_max);
    printf("      Max number of downstream conns moxi will open per worker thread\n"
           "      to a host:port:bucket.  If downstream_conn_max is reached,\n"
           "      requests go onto the tail of a downstream conn queue.\n"
           "      0 means no limit.\n");
    printf("  downstream_conn_queue_timeout=%ld\n",
           b->downstream_conn_queue_timeout.tv_sec * 1000 +
           b->downstream_conn_queue_timeout.tv_usec / 1000);
    printf("      Millisecs before moxi will timeout a request that has been\n"
           "      waiting too long in a downstream conn queue.\n"
           "      0 means no timeout.\n");
    printf("  downstream_timeout=%ld\n",
           b->downstream_timeout.tv_sec * 1000 +
           b->downstream_timeout.tv_usec / 1000);
    printf("      Millisecs that moxi will allow a request to run after necessary\n"
           "      downstream conns have been allocated to the request (such as\n"
           "      when the request reaches the head of the downstream conn queue).\n"
           "      0 means no timeout.\n");
    printf("  cycle=%d\n", b->cycle);
    printf("      Millisec clock quantum for moxi.\n");
    printf("  mcs_opts=<initialization options for the mcs layer>\n");
    printf("      The mcs layer abstracts away the libmemcached and libvbucket\n"
           "      hashing libraries, and mcs_opts allows 'pass-thru' initialization\n"
           "      parameters.\n"
           "      There are currently no extra libvbucket initialization options.\n"
           "      The libmemcached-only initialization options are...\n"
           "        distribution:ketama\n"
           "            use KETAMA distribution behavior;\n"
           "            this is the default.\n"
           "        distribution:ketama-weighted\n"
           "            use KETAMA_WEIGHTED distribution behavior; usually\n"
           "            use this when you have >1 client language libraries\n"
           "            as it provides compatibility with default libketama\n"
           "            hashing.\n"
           "        distribution:modula\n"
           "            use mod hashing (non-ketama) distribution behavior.\n");
    printf("\n"
           "Example of a 'gateway' moxi:\n");
    printf("  ./moxi -Z usr=Administrator,pwd=password,port_listen=11311,"
           "concurrency=1024,"
           "wait_queue_timeout=200,"
           "connect_timeout=400,"
           "connect_max_errors=3,"
           "connect_retry_interval=30000,"
           "auth_timeout=100,"
           "downstream_conn_max=16,"
           "downstream_timeout=5000,"
           "cycle=200,"
           "default_bucket_name=default"
           " http://127.0.0.1:8091/pools/default/saslBucketsStreaming\n");

    printf("\n"
           "Example of a 'per-bucket' moxi, for the 'default' bucket:\n");
    printf("  ./moxi -Z usr=Administrator,pwd=password,port_listen=11411,"
           "concurrency=1024,"
           "wait_queue_timeout=200,"
           "connect_timeout=400,"
           "connect_max_errors=3,"
           "connect_retry_interval=30000,"
           "auth_timeout=100,"
           "downstream_conn_max=16,"
           "downstream_timeout=5000,"
           "cycle=200"
           " http://127.0.0.1:8091/pools/default/bucketsStreaming/default\n");
}

static void usage_license(void) {
    printf(PACKAGE " " VERSION "\n\n");
    printf(
    "Copyright (c) 2003, Danga Interactive, Inc. <http://www.danga.com/>\n"
    "All rights reserved.\n"
    "\n"
    "Redistribution and use in source and binary forms, with or without\n"
    "modification, are permitted provided that the following conditions are\n"
    "met:\n"
    "\n"
    "    * Redistributions of source code must retain the above copyright\n"
    "notice, this list of conditions and the following disclaimer.\n"
    "\n"
    "    * Redistributions in binary form must reproduce the above\n"
    "copyright notice, this list of conditions and the following disclaimer\n"
    "in the documentation and/or other materials provided with the\n"
    "distribution.\n"
    "\n"
    "    * Neither the name of the Danga Interactive nor the names of its\n"
    "contributors may be used to endorse or promote products derived from\n"
    "this software without specific prior written permission.\n"
    "\n"
    "THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS\n"
    "\"AS IS\" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT\n"
    "LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR\n"
    "A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT\n"
    "OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,\n"
    "SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT\n"
    "LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
    "DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n"
    "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
    "(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE\n"
    "OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n"
    "\n"
    "\n"
    "This product includes software developed by Niels Provos.\n"
    "\n"
    "[ libevent ]\n"
    "\n"
    "Copyright 2000-2003 Niels Provos <provos@citi.umich.edu>\n"
    "All rights reserved.\n"
    "\n"
    "Redistribution and use in source and binary forms, with or without\n"
    "modification, are permitted provided that the following conditions\n"
    "are met:\n"
    "1. Redistributions of source code must retain the above copyright\n"
    "   notice, this list of conditions and the following disclaimer.\n"
    "2. Redistributions in binary form must reproduce the above copyright\n"
    "   notice, this list of conditions and the following disclaimer in the\n"
    "   documentation and/or other materials provided with the distribution.\n"
    "3. All advertising materials mentioning features or use of this software\n"
    "   must display the following acknowledgement:\n"
    "      This product includes software developed by Niels Provos.\n"
    "4. The name of the author may not be used to endorse or promote products\n"
    "   derived from this software without specific prior written permission.\n"
    "\n"
    "THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR\n"
    "IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES\n"
    "OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.\n"
    "IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,\n"
    "INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT\n"
    "NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
    "DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n"
    "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
    "(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF\n"
    "THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n"
    );

    return;
}

static void save_pid(const pid_t pid, const char *pid_file) {
    FILE *fp;
    if (pid_file == NULL)
        return;

    if ((fp = fopen(pid_file, "w")) == NULL) {
        moxi_log_write("Could not open the pid file %s for writing\n", pid_file);
        return;
    }

    fprintf(fp,"%ld\n", (long)pid);
    if (fclose(fp) == -1) {
        moxi_log_write("Could not close the pid file %s.\n", pid_file);
        return;
    }
}

static void remove_pidfile(const char *pid_file) {
  if (pid_file == NULL)
      return;

  if (remove(pid_file) != 0) {
      moxi_log_write("Could not remove the pid file %s.\n", pid_file);
  }

}

static void sig_handler(const int sig) {
    switch (sig) {
#ifndef WIN32
        case SIGHUP :
            log_error_cycle(ml);
            break;
#endif
        default :
            printf("SIGINT handled.\n");
            exit(EXIT_SUCCESS);
    }
}

#if !defined(WIN32) && !defined(HAVE_SIGIGNORE)
static int sigignore(int sig) {
    struct sigaction sa = { .sa_handler = SIG_IGN, .sa_flags = 0 };

    if (sigemptyset(&sa.sa_mask) == -1 || sigaction(sig, &sa, 0) == -1) {
        return -1;
    }
    return 0;
}
#endif


#if defined(HAVE_GETPAGESIZES) && defined(HAVE_MEMCNTL)
/*
 * On systems that supports multiple page sizes we may reduce the
 * number of TLB-misses by using the biggest available page size
 */
static int enable_large_pages(void) {
    int ret = -1;
    size_t sizes[32];
    int avail = getpagesizes(sizes, 32);
    if (avail != -1) {
        size_t max = sizes[0];
        struct memcntl_mha arg = {0};
        int ii;

        for (ii = 1; ii < avail; ++ii) {
            if (max < sizes[ii]) {
                max = sizes[ii];
            }
        }

        arg.mha_flags   = 0;
        arg.mha_pagesize = max;
        arg.mha_cmd = MHA_MAPSIZE_BSSBRK;

        if (memcntl(0, 0, MC_HAT_ADVISE, (caddr_t)&arg, 0, 0) == -1) {
            moxi_log_write("Failed to set large pages: %s\n",
                    strerror(errno));
            moxi_log_write("Will use default page size\n");
        } else {
            ret = 0;
        }
    } else {
        moxi_log_write("Failed to get supported pagesizes: %s\n",
                strerror(errno));
        moxi_log_write("Will use default page size\n");
    }

    return ret;
}
#endif

#ifdef MAIN_CHECK
#define main main_memcached
#endif

int main(int argc, char **argv);

#ifdef MAIN_CHECK

static
work_collect main_completion;

struct main_data {
    int argc;
    char **argv;
};

static
void *main_trampoline(void *_data)
{
    struct main_data *data = _data;
    int rv = main_memcached(data->argc, data->argv);
    printf("Unexpected exit from main with rv = %d\n", rv);
    abort();
}

#include <stdarg.h>

void start_main(char *arg0, ...)
{
    pthread_t main_thread;
    va_list args;
    int rv;
    char *argv[256] = {0};
    int argc = 0;
    char *current_arg = arg0;

    va_start(args, arg0);
    while (current_arg != NULL) {
        argv[argc++] = current_arg;
        current_arg = va_arg(args, char *);
    }
    va_end(args);

    work_collect_init(&main_completion, 1, NULL);
    struct main_data data = {.argc = argc, .argv = argv};
    rv = pthread_create(&main_thread, NULL, main_trampoline, &data);
    if (rv) {
        perror("pthread_create");
        abort();
    }
    work_collect_wait(&main_completion);
}
#endif

int main (int argc, char **argv) {
    int c;
    bool lock_memory = false;
    bool do_daemonize = false;
    bool preallocate = false;
    bool check_stdin = false;
    int maxcore = 0;
    char *username = NULL;
    char *pid_file = NULL;
    char *cproxy_cfg = NULL;
    char *cproxy_behavior = NULL;
#ifndef MAIN_CHECK
#ifndef _MSC_VER
    struct passwd *pw;
#endif
    char *log_file = NULL;
    int log_mode = DEFAULT_ERRORLOG;
    int log_level = 0;
#endif
#ifndef WIN32
    struct rlimit rlim;
#endif
    /* listening sockets */
    static int *l_socket = NULL;

    /* udp socket */
    static int *u_socket = NULL;

    // MB-14649 log() crash on windows on some CPU's
#ifdef _WIN64
    _set_FMA3_enable (0);
#endif

    /* handle SIGINT */
    signal(SIGINT, sig_handler);

    cb_initialize_sockets();
    initialize_conn_lock();

    /* init settings */
    settings_init();

    /* set stderr non-buffering (for running under, say, daemontools) */
    setbuf(stderr, NULL);

    /* process arguments */
    while (-1 != (c = getopt(argc, argv,
          "a:"  /* access mask for unix socket */
          "p:"  /* TCP port number to listen on */
#ifdef HAVE_SYS_UN_H
          "s:"  /* unix socket path to listen on */
#endif
          "U:"  /* UDP port number to listen on */
          "m:"  /* max memory to use for items in megabytes */
          "M"   /* return error on memory exhausted */
          "c:"  /* max simultaneous connections */
          "k"   /* lock down all paged memory */
          "hi"  /* help, licence info */
          "r"   /* maximize core file limit */
          "v"   /* verbose */
          "d"   /* daemon mode */
          "l:"  /* interface to listen on */
#ifdef HAVE_GETPWNAM
          "u:"  /* user identity to run as */
#endif
          "P:"  /* save PID in file */
          "f:"  /* factor? */
          "n:"  /* minimum space allocated for key+value+flags */
          "t:"  /* threads */
          "D:"  /* prefix delimiter? */
          "L"   /* Large memory pages */
          "R:"  /* max requests per event */
          "C"   /* Disable use of CAS */
          "b:"  /* backlog queue limit */
          "z:"  /* cproxy configuration */
          "Z:"  /* cproxy behavior */
          "B:"  /* Binding protocol */
          "Y:"  /* exit when stdin closes, for windows compatibility */
          "O:"  /* log file name */
          "X"   /* run in mcmux compatiblity mode */
        ))) {
        switch (c) {
        case 'a':
            /* access for unix domain socket, as octal mask (like chmod)*/
            settings.access= strtol(optarg,NULL,8);
            break;

        case 'U':
            settings.udpport = atoi(optarg);
            break;
        case 'p':
            settings.port = atoi(optarg);
            break;
        case 's':
            settings.socketpath = optarg;
            break;
        case 'm':
            settings.maxbytes = ((size_t)atoi(optarg)) * 1024 * 1024;
            break;
        case 'M':
            settings.evict_to_free = 0;
            break;
        case 'c':
            settings.maxconns = atoi(optarg);
            break;
        case 'h':
            usage(argv);
            exit(EXIT_SUCCESS);
        case 'i':
            usage_license();
            exit(EXIT_SUCCESS);
        case 'k':
            lock_memory = true;
            break;
        case 'v':
            settings.verbose++;
            break;
        case 'l':
            settings.inter = strdup(optarg);
            break;
        case 'd':
            do_daemonize = true;
            break;
        case 'r':
            maxcore = 1;
            break;
        case 'R':
            settings.reqs_per_event = atoi(optarg);
            if (settings.reqs_per_event == 0) {
                fprintf(stderr, "Number of requests per event must be greater than 0\n");
                return 1;
            }
            break;
        case 'u':
            username = optarg;
            break;
        case 'P':
            pid_file = optarg;
            break;
        case 'f':
            settings.factor = atof(optarg);
            if (settings.factor <= 1.0) {
                fprintf(stderr, "Factor must be greater than 1\n");
                return 1;
            }
            break;
        case 'n':
            settings.chunk_size = atoi(optarg);
            if (settings.chunk_size == 0) {
                fprintf(stderr, "Chunk size must be greater than 0\n");
                return 1;
            }
            break;
        case 't':
            settings.num_threads = atoi(optarg) + 1; /* Extra dispatch thread */
            if (settings.num_threads <= 1) {
                fprintf(stderr, "Number of threads must be greater than 0\n");
                return 1;
            }
            break;
        case 'D':
            if (! optarg || ! optarg[0]) {
                fprintf(stderr, "No delimiter specified\n");
                return 1;
            }
            settings.prefix_delimiter = optarg[0];
            settings.detail_enabled = 1;
            break;
        case 'L' :
#if defined(HAVE_GETPAGESIZES) && defined(HAVE_MEMCNTL)
            if (enable_large_pages() == 0) {
                preallocate = true;
            }
#endif
            break;
        case 'C' :
            settings.use_cas = false;
            break;
        case 'b' :
            settings.backlog = atoi(optarg);
            break;
        case 'z' :
            cproxy_cfg = strdup(optarg);
            break;
        case 'Z' :
            cproxy_behavior = strdup(optarg);
            break;
        case 'Y' :
            check_stdin = (optarg[0] == 'y');
            break;
        case 'B':
            if (strcmp(optarg, "auto") == 0) {
                settings.binding_protocol = negotiating_prot;
            } else if (strcmp(optarg, "binary") == 0) {
                settings.binding_protocol = binary_prot;
            } else if (strcmp(optarg, "ascii") == 0) {
                settings.binding_protocol = ascii_prot;
            } else {
                fprintf(stderr, "Invalid value for binding protocol: %s\n"
                        " -- should be one of auto, binary, or ascii\n", optarg);
                exit(EX_USAGE);
            }
            break;
        case 'O' :
#ifndef MAIN_CHECK
            if (!optarg) {
                fprintf(stderr, "Log File path not provided, resorting to syslog");
            } else {
                if (optarg[0] == '\0' ||
                    strcmp(optarg, "stderr") == 0) {
                    log_mode = ERRORLOG_STDERR;
                } else {
                    log_mode = ERRORLOG_FILE;
                    log_file = strdup(optarg);
                }
                break;
            }
#endif
            break;
        case 'X' :
            settings.enable_mcmux_mode = true;
            break;

        default:
            fprintf(stderr, "Illegal argument \"%c\"\n", c);
            return 1;
        }
    }

#ifndef MAIN_CHECK
    /*
     * initalize log file
     */
    ml = (struct moxi_log *)calloc(1, sizeof(struct moxi_log));
    ml->log_mode = log_mode;
    ml->log_file = log_file;
    ml->log_ident = "moxi";
    ml->log_level = log_level ? log_level : 5;

    log_error_open(ml);

    /*
     * logger initialized, from now on we should use moxi_log_write
     * instead of fprintfs/perror
     */
    if (settings.verbose > 0) {
        moxi_log_write("moxi log, mode=%d, file=%s\n", ml->log_mode, ml->log_file);
    }

#ifndef WIN32
    if (ml->log_mode == ERRORLOG_FILE) {
        /*
         * install the signal handler for SIGHUP to handle
         * cycling of the error log file
         */
         signal(SIGHUP, sig_handler);
    }
#endif
#endif

    if (cproxy_cfg
        && settings.port == UNSPECIFIED
        && settings.udpport == UNSPECIFIED) {
        /* Default behavior when we're a proxy is to also */
        /* behave as a memcached on port 11210. */

        settings.port = MEMCACHED_DEFAULT_LISTEN_PORT;
    }

#ifdef HAVE_GETRLIMIT
    if (maxcore != 0) {
        struct rlimit rlim_new;
        /*
         * First try raising to infinity; if that fails, try bringing
         * the soft limit to the hard.
         */
        if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
            rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;
            if (setrlimit(RLIMIT_CORE, &rlim_new)!= 0) {
                /* failed. try raising just to the old max */
                rlim_new.rlim_cur = rlim_new.rlim_max = rlim.rlim_max;
                (void)setrlimit(RLIMIT_CORE, &rlim_new);
            }
        }
        /*
         * getrlimit again to see what we ended up with. Only fail if
         * the soft limit ends up 0, because then no core files will be
         * created at all.
         */

        if ((getrlimit(RLIMIT_CORE, &rlim) != 0) || rlim.rlim_cur == 0) {
            moxi_log_write("failed to ensure corefile creation\n");
            exit(EX_OSERR);
        }
    }

    /*
     * If needed, increase rlimits to allow as many connections
     * as needed.
     */

    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        moxi_log_write("failed to getrlimit number of files\n");
        exit(EX_OSERR);
    } else {
        int maxfiles = settings.maxconns;
        if ((int)rlim.rlim_cur < maxfiles)
            rlim.rlim_cur = maxfiles;
        if (rlim.rlim_max < rlim.rlim_cur)
            rlim.rlim_max = rlim.rlim_cur;
        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            moxi_log_write("failed to set rlimit for open files. Try running as root or requesting smaller maxconns value.\n");
            exit(EX_OSERR);
        }
    }
#endif
    /* lose root privileges if we have them */
#ifndef MAIN_CHECK
#ifdef HAVE_GETPWNAM
    if (getuid() == 0 || geteuid() == 0) {
        if (username != NULL) {
            if ((pw = getpwnam(username)) == 0) {
                moxi_log_write("can't find the user %s to switch to\n", username);
                exit(EX_NOUSER);
            }
            if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
                moxi_log_write("failed to assume identity of user %s\n", username);
                exit(EX_OSERR);
            }
        }
    }
#endif
#endif

#ifndef WIN32
    /* daemonize if requested */
    /* if we want to ensure our ability to dump core, don't chdir to / */
    if (do_daemonize) {
        if (sigignore(SIGHUP) == -1) {
            perror("Failed to ignore SIGHUP");
        }
        if (daemonize(maxcore, settings.verbose) == -1) {
            moxi_log_write("failed to daemon() in order to daemonize\n");
            exit(EXIT_FAILURE);
        }
    }
#endif

    /* lock paged memory if needed */
    if (lock_memory) {
#ifdef HAVE_MLOCKALL
        int res = mlockall(MCL_CURRENT | MCL_FUTURE);
        if (res != 0) {
            moxi_log_write("warning: -k invalid, mlockall() failed: %s\n",
                    strerror(errno));
        }
#else
        moxi_log_write("warning: -k invalid, mlockall() not supported on this platform.  proceeding without.\n");
#endif
    }

    /* initialize main thread libevent instance */
    main_base = event_init();

    /* initialize other stuff */
    item_init();
    stats_init();
    assoc_init();
    conn_init();
    slabs_init(settings.maxbytes, settings.factor, preallocate);

#ifdef HAVE_SIGPIPE
    /*
     * ignore SIGPIPE signals; we can use errno == EPIPE if we
     * need that information
     */
    if (sigignore(SIGPIPE) == -1) {
        perror("failed to ignore SIGPIPE; sigaction");
        exit(EX_OSERR);
    }
#endif

    /* start up worker threads if MT mode */
    thread_init(settings.num_threads, main_base);
    /* save the PID in if we're a daemon, do this after thread_init due to
       a file descriptor handling bug somewhere in libevent */

    if (start_assoc_maintenance_thread() == -1) {
        exit(EXIT_FAILURE);
    }

#ifndef WIN32
    if (do_daemonize)
        save_pid(getpid(), pid_file);
#endif

    /* initialise clock event */
    clock_handler(0, 0, 0);

#ifdef HAVE_SYS_UN_H
    /* create unix mode sockets after dropping privileges */
    if ((settings.socketpath != NULL) && (false == settings.enable_mcmux_mode)) {
        errno = 0;
        if (server_socket_unix(settings.socketpath,settings.access)) {
            moxi_log_write("failed to listen on UNIX socket: %s: %s",
                settings.socketpath, strerror(errno));
            exit(EX_OSERR);
        }
    }
#endif

    /* create the listening socket, bind it, and init */
    if (settings.socketpath == NULL) {
        const char *portnumber_filename = getenv("MEMCACHED_PORT_FILENAME");
        char temp_portnumber_filename[PATH_MAX];
        FILE *portnumber_file = NULL;

        if (portnumber_filename != NULL) {
            snprintf(temp_portnumber_filename,
                     sizeof(temp_portnumber_filename),
                     "%s.lck", portnumber_filename);

            portnumber_file = fopen(temp_portnumber_filename, "a");
            if (portnumber_file == NULL) {
                moxi_log_write("Failed to open \"%s\": %s\n",
                        temp_portnumber_filename, strerror(errno));
            }
        }

        errno = 0;
        if ((settings.port >= 0) && (false == settings.enable_mcmux_mode) &&
                (server_socket(settings.port, tcp_transport, portnumber_file))) {
            moxi_log_write("failed to listen on TCP port %d: %s",
                           settings.port, strerror(errno));
            if (ml->log_mode != ERRORLOG_STDERR) {
                fprintf(stderr, "failed to listen on TCP port %d: %s",
                        settings.port, strerror(errno));
            }
            exit(EX_OSERR);
        }

        /*
         * initialization order: first create the listening sockets
         * (may need root on low ports), then drop root if needed,
         * then daemonise if needed, then init libevent (in some cases
         * descriptors created by libevent wouldn't survive forking).
         */

        /* create the UDP listening socket and bind it */
        errno = 0;
        if (settings.udpport >= 0 && server_socket(settings.udpport,
                                                   udp_transport,
                                                   portnumber_file)) {
            moxi_log_write("failed to listen on UDP port %d: %s",
                           settings.udpport, strerror(errno));
            if (ml->log_mode != ERRORLOG_STDERR) {
                fprintf(stderr, "failed to listen on UDP port %d: %s",
                        settings.udpport, strerror(errno));
            }
            exit(EX_OSERR);
        }

        if (portnumber_file) {
            fclose(portnumber_file);
            rename(temp_portnumber_filename, portnumber_filename);
        }
    }

    /* Do cproxy_init after we create normal memcached sockets, because
     * we can be a proxy to ourselves for testing.
     */
    if (cproxy_cfg) {
        cproxy_init(cproxy_cfg,
                    cproxy_behavior,
                    settings.num_threads,
                    main_base);
        free(cproxy_cfg);
    } else if (settings.enable_mcmux_mode) {
        cproxy_init(NULL, cproxy_behavior, settings.num_threads, main_base);
    } else {
        int i = argc - 1;
        if (i > 0 &&
            (strncmp(argv[i], "apikey=", 7) == 0 ||
             strncmp(argv[i], "http://", 7) == 0 ||
             strchr(argv[i], '@') != NULL)) {
            cproxy_init(argv[i], cproxy_behavior,
                        settings.num_threads,
                        main_base);
        } else {
#ifndef MAIN_CHECK
            if (settings.port == UNSPECIFIED &&
                settings.udpport == UNSPECIFIED) {
                moxi_log_write("ERROR: need cluster configuration. See usage (-h).\n");
                if (ml->log_mode != ERRORLOG_STDERR) {
                    fprintf(stderr, "ERROR: need cluster configuration. See usage (-h).\n");
                }
                exit(EXIT_FAILURE);
            }
#endif
        }
    }

#ifndef MAIN_CHECK
    if (check_stdin) {
      stdin_check();
    }
#endif

    if (cproxy_behavior) {
        free(cproxy_behavior);
    }

    /* Drop privileges no longer needed */
    drop_privileges();

#ifdef MAIN_CHECK
    work_collect_one(&main_completion);
#endif

    /* enter the event loop */
    event_base_loop(main_base, 0);

    stop_assoc_maintenance_thread();

    /* remove the PID file if we're a daemon */
    if (do_daemonize)
        remove_pidfile(pid_file);
    /* Clean up strdup() call for bind() address */
    if (settings.inter)
      free(settings.inter);
    if (l_socket)
      free(l_socket);
    if (u_socket)
      free(u_socket);

    return EXIT_SUCCESS;
}

/* Internal data structures that should be opaque to outside viewers */
#ifndef CONFLATE_INTERNAL_H
#define CONFLATE_INTERNAL_H 1

#include <platform/platform.h>

#ifdef CONFLATE_USE_XMPP
#include <strophe.h>
#else
#define xmpp_ctx_t void
#define xmpp_conn_t void
#endif

struct _conflate_handle {

    xmpp_ctx_t *ctx;
    xmpp_conn_t *conn;

    conflate_config_t *conf;

    cb_thread_t thread;

    char *url; /* Current URL for debuggability. */
};

void conflate_init_commands(void);

#endif /* CONFLATE_INTERNAL_H */

/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include "memcached.h"
#include "cproxy.h"
#include "mcs.h"
#include "log.h"

// TODO: This timeout is inherited from zstored, but use it where?
//
#define DOWNSTREAM_DEFAULT_LINGER 1000

// The lvb stands for libvbucket.
//
mcs_st  *lvb_create(mcs_st *ptr, const char *config,
                    const char *default_usr,
                    const char *default_pwd,
                    const char *opts);
void     lvb_free_data(mcs_st *ptr);
bool     lvb_stable_update(mcs_st *curr_version, mcs_st *next_version);
uint32_t lvb_key_hash(mcs_st *ptr, const char *key, size_t key_length,
                      int *vbucket);
void     lvb_server_invalid_vbucket(mcs_st *ptr, int server_index,
                                    int vbucket);

// The lmc stands for libmemcached.
//
mcs_st  *lmc_create(mcs_st *ptr, const char *config,
                    const char *default_usr,
                    const char *default_pwd,
                    const char *opts);
void     lmc_free_data(mcs_st *ptr);
uint32_t lmc_key_hash(mcs_st *ptr, const char *key, size_t key_length,
                      int *vbucket);

// ----------------------------------------------------------------------

mcs_st *mcs_create(mcs_st *ptr, const char *config,
                   const char *default_usr,
                   const char *default_pwd,
                   const char *opts) {
#ifdef MOXI_USE_LIBVBUCKET
    if (config[0] == '{') {
        if (settings.verbose > 2) {
            moxi_log_write("mcs_create using libvbucket\n");
        }
        return lvb_create(ptr, config, default_usr, default_pwd, opts);
    }
#endif
#ifdef MOXI_USE_LIBMEMCACHED
    if (config[0] != '{') {
        if (settings.verbose > 2) {
            moxi_log_write("mcs_create using libmemcached\n");
        }
        return lmc_create(ptr, config, default_usr, default_pwd, opts);
    }
#endif
    moxi_log_write("ERROR: unconfigured hash library\n");
    exit(1);

    return NULL;
}

void mcs_free(mcs_st *ptr) {
#ifdef MOXI_USE_LIBVBUCKET
    if (ptr->kind == MCS_KIND_LIBVBUCKET) {
        lvb_free_data(ptr);
    }
#endif
#ifdef MOXI_USE_LIBMEMCACHED
    if (ptr->kind == MCS_KIND_LIBMEMCACHED) {
        lmc_free_data(ptr);
    }
#endif
    ptr->kind = MCS_KIND_UNKNOWN;

    if (ptr->servers) {
        for (int i = 0; i < ptr->nservers; i++) {
            if (ptr->servers[i].usr != NULL) {
                free(ptr->servers[i].usr);
            }
            if (ptr->servers[i].pwd != NULL) {
                free(ptr->servers[i].pwd);
            }
        }
        free(ptr->servers);
    }

    memset(ptr, 0, sizeof(*ptr));
}

bool mcs_stable_update(mcs_st *curr_version, mcs_st *next_version) {
#ifdef MOXI_USE_LIBVBUCKET
    if (curr_version->kind == MCS_KIND_LIBVBUCKET) {
        return lvb_stable_update(curr_version, next_version);
    }
#endif

    // TODO: MCS_KIND_LIBMEMCACHED impl for stable update.

    return false;
}

uint32_t mcs_server_count(mcs_st *ptr) {
    return (uint32_t) ptr->nservers;
}

mcs_server_st *mcs_server_index(mcs_st *ptr, int i) {
    return &ptr->servers[i];
}

uint32_t mcs_key_hash(mcs_st *ptr, const char *key, size_t key_length,
                      int *vbucket) {
#ifdef MOXI_USE_LIBVBUCKET
    if (ptr->kind == MCS_KIND_LIBVBUCKET) {
        return lvb_key_hash(ptr, key, key_length, vbucket);
    }
#endif
#ifdef MOXI_USE_LIBMEMCACHED
    if (ptr->kind == MCS_KIND_LIBMEMCACHED) {
        return lmc_key_hash(ptr, key, key_length, vbucket);
    }
#endif
    return 0;
}

void mcs_server_invalid_vbucket(mcs_st *ptr, int server_index,
                                int vbucket) {
#ifdef MOXI_USE_LIBVBUCKET
    if (ptr->kind == MCS_KIND_LIBVBUCKET) {
        lvb_server_invalid_vbucket(ptr, server_index, vbucket);
    }
#endif
}

// ----------------------------------------------------------------------

#ifdef MOXI_USE_LIBVBUCKET

mcs_st *lvb_create(mcs_st *ptr, const char *config,
                   const char *default_usr,
                   const char *default_pwd,
                   const char *opts) {
    (void) opts;

    assert(ptr);
    memset(ptr, 0, sizeof(*ptr));
    ptr->kind = MCS_KIND_LIBVBUCKET;

    VBUCKET_CONFIG_HANDLE vch = vbucket_config_parse_string(config);
    if (vch != NULL) {
        ptr->data     = vch;
        ptr->nservers = vbucket_config_get_num_servers(vch);
        if (ptr->nservers > 0) {
            ptr->servers = calloc(sizeof(mcs_server_st), ptr->nservers);
            if (ptr->servers != NULL) {
                for (int i = 0; i < ptr->nservers; i++) {
                    ptr->servers[i].fd = -1;
                }

                int j = 0;
                for (; j < ptr->nservers; j++) {
                    const char *hostport = vbucket_config_get_server(vch, j);
                    if (hostport != NULL &&
                        strlen(hostport) > 0 &&
                        strlen(hostport) < sizeof(ptr->servers[j].hostname) - 1) {
                        strncpy(ptr->servers[j].hostname,
                                hostport,
                                sizeof(ptr->servers[j].hostname) - 1);
                        char *colon = strchr(ptr->servers[j].hostname, ':');
                        if (colon != NULL) {
                            *colon = '\0';
                            ptr->servers[j].port = atoi(colon + 1);
                            if (ptr->servers[j].port <= 0) {
                                moxi_log_write("mcs_create failed, could not parse port: %s\n",
                                        config);
                                break;
                            }
                        } else {
                            moxi_log_write("mcs_create failed, missing port: %s\n",
                                    config);
                            break;
                        }
                    } else {
                        moxi_log_write("mcs_create failed, unknown server: %s\n",
                                config);
                        break;
                    }

                    const char *user = vbucket_config_get_user(vch);
                    if (user != NULL) {
                        ptr->servers[j].usr = strdup(user);
                    } else if (default_usr != NULL) {
                        ptr->servers[j].usr = strdup(default_usr);
                    }

                    const char *password = vbucket_config_get_password(vch);
                    if (password != NULL) {
                        ptr->servers[j].pwd = strdup(password);
                    } else if (default_pwd != NULL) {
                        ptr->servers[j].pwd = strdup(default_pwd);
                    }
                }

                if (j >= ptr->nservers) {
                    return ptr;
                }
            }
        }
    } else {
        moxi_log_write("mcs_create failed, vbucket_config_parse_string: %s\n",
                       config);
    }

    mcs_free(ptr);

    return NULL;
}

void lvb_free_data(mcs_st *ptr) {
    assert(ptr->kind == MCS_KIND_LIBVBUCKET);

    if (ptr->data != NULL) {
        vbucket_config_destroy((VBUCKET_CONFIG_HANDLE) ptr->data);
    }

    ptr->data = NULL;
}

/* Returns true if curr_version could be updated with next_version in
 * a low-impact stable manner (server-list is the same), allowing the
 * same connections to be reused.  Or returns false if the delta was
 * too large for an in-place updating of curr_version with information
 * from next_version.
 *
 * The next_version may be destroyed in this call, and the caller
 * should afterwards only call mcs_free() on the next_version.
 */
bool lvb_stable_update(mcs_st *curr_version, mcs_st *next_version) {
    assert(curr_version->kind == MCS_KIND_LIBVBUCKET);
    assert(curr_version->data != NULL);
    assert(next_version->kind == MCS_KIND_LIBVBUCKET);
    assert(next_version->data != NULL);

    bool rv = false;

    VBUCKET_CONFIG_DIFF *diff =
        vbucket_compare((VBUCKET_CONFIG_HANDLE) curr_version->data,
                        (VBUCKET_CONFIG_HANDLE) next_version->data);
    if (diff != NULL) {
        if (!diff->sequence_changed) {
            vbucket_config_destroy((VBUCKET_CONFIG_HANDLE) curr_version->data);
            curr_version->data = next_version->data;
            next_version->data = 0;

            rv = true;
        }

        vbucket_free_diff(diff);
    }

    return rv;
}

uint32_t lvb_key_hash(mcs_st *ptr, const char *key, size_t key_length,
                      int *vbucket) {
    assert(ptr->kind == MCS_KIND_LIBVBUCKET);
    assert(ptr->data != NULL);

    VBUCKET_CONFIG_HANDLE vch = (VBUCKET_CONFIG_HANDLE) ptr->data;

    int v = vbucket_get_vbucket_by_key(vch, key, key_length);
    if (vbucket != NULL) {
        *vbucket = v;
    }

    return (uint32_t) vbucket_get_master(vch, v);
}

void lvb_server_invalid_vbucket(mcs_st *ptr, int server_index,
                                int vbucket) {
    assert(ptr->kind == MCS_KIND_LIBVBUCKET);
    assert(ptr->data != NULL);

    VBUCKET_CONFIG_HANDLE vch = (VBUCKET_CONFIG_HANDLE) ptr->data;

    vbucket_found_incorrect_master(vch, vbucket, server_index);
}

#endif // MOXI_USE_LIBVBUCKET

// ----------------------------------------------------------------------

#ifdef MOXI_USE_LIBMEMCACHED

mcs_st *lmc_create(mcs_st *ptr, const char *config,
                   const char *default_usr,
                   const char *default_pwd,
                   const char *opts) {
    assert(ptr);
    memset(ptr, 0, sizeof(*ptr));
    ptr->kind = MCS_KIND_LIBMEMCACHED;

    memcached_st *mst = memcached_create(NULL);
    if (mst != NULL) {
        memcached_behavior_t b = MEMCACHED_BEHAVIOR_KETAMA_WEIGHTED;
        uint64_t             v = 1;

        if (opts != NULL) {
            if (strstr(opts, "distribution:ketama-weighted") != NULL) {
                b = MEMCACHED_BEHAVIOR_KETAMA_WEIGHTED;
                v = 1;
            } else if (strstr(opts, "distribution:ketama") != NULL) {
                b = MEMCACHED_BEHAVIOR_KETAMA;
                v = 1;
            } else if (strstr(opts, "distribution:modula") != NULL) {
                b = MEMCACHED_BEHAVIOR_KETAMA;
                v = 0;
            }
        }

        memcached_behavior_set(mst, b, v);
        memcached_behavior_set(mst, MEMCACHED_BEHAVIOR_NO_BLOCK, 1);
        memcached_behavior_set(mst, MEMCACHED_BEHAVIOR_TCP_NODELAY, 1);

        memcached_server_st *mservers;

        mservers = memcached_servers_parse(config);
        if (mservers != NULL) {
            memcached_server_push(mst, mservers);

            ptr->data     = mst;
            ptr->nservers = (int) memcached_server_list_count(mservers);
            if (ptr->nservers > 0) {
                ptr->servers = calloc(sizeof(mcs_server_st), ptr->nservers);
                if (ptr->servers != NULL) {
                    for (int i = 0; i < ptr->nservers; i++) {
                        ptr->servers[i].fd = -1;
                    }

                    int j = 0;
                    for (; j < ptr->nservers; j++) {
                        strncpy(ptr->servers[j].hostname,
                                memcached_server_name(mservers + j),
                                sizeof(ptr->servers[j].hostname) - 1);
                        ptr->servers[j].port =
                            (int) memcached_server_port(mservers + j);
                        if (ptr->servers[j].port <= 0) {
                            moxi_log_write("lmc_create failed, could not parse port: %s\n",
                                           config);
                            break;
                        }

                        if (default_usr != NULL) {
                            ptr->servers[j].usr = strdup(default_usr);
                        }

                        if (default_pwd != NULL) {
                            ptr->servers[j].pwd = strdup(default_pwd);
                        }
                    }

                    if (j >= ptr->nservers) {
                        memcached_server_list_free(mservers);

                        return ptr;
                    }
                }
            }

            memcached_server_list_free(mservers);
        }
    }

    mcs_free(ptr);

    return NULL;
}

void lmc_free_data(mcs_st *ptr) {
    assert(ptr->kind == MCS_KIND_LIBMEMCACHED);

    if (ptr->data != NULL) {
        memcached_free((memcached_st *) ptr->data);
    }

    ptr->data = NULL;
}

uint32_t lmc_key_hash(mcs_st *ptr, const char *key, size_t key_length, int *vbucket) {
    assert(ptr->kind == MCS_KIND_LIBMEMCACHED);
    assert(ptr->data != NULL);

    if (vbucket != NULL) {
        *vbucket = -1;
    }

    return memcached_generate_hash((memcached_st *) ptr->data, key, key_length);
}

#endif // MOXI_USE_LIBMEMCACHED

// ----------------------------------------------------------------------

void mcs_server_st_quit(mcs_server_st *ptr, uint8_t io_death) {
    (void) io_death;

    // TODO: Should send QUIT cmd.
    //
    if (ptr->fd != -1) {
        close(ptr->fd);
    }
    ptr->fd = -1;
}

mcs_return mcs_server_st_connect(mcs_server_st *ptr, int *errno_out, bool blocking) {
    if (ptr->fd != -1) {
        if (errno_out != NULL) {
            *errno_out = 0;
        }

        return MCS_SUCCESS;
    }

    if (errno_out != NULL) {
        *errno_out = -1;
    }

    ptr->fd = mcs_connect(ptr->hostname, ptr->port, errno_out, blocking);
    if (ptr->fd != -1) {
        return MCS_SUCCESS;
    }

    return MCS_FAILURE;
}

int mcs_connect(const char *hostname, int portnum,
                int *errno_out, bool blocking) {
    if (errno_out != NULL) {
        *errno_out = -1;
    }

    int ret = -1;

    struct addrinfo *ai   = NULL;
    struct addrinfo *next = NULL;
    struct addrinfo hints = { .ai_flags = AI_PASSIVE,
                              .ai_socktype = SOCK_STREAM,
                              .ai_family = AF_UNSPEC };

    char port[50];
    snprintf(port, sizeof(port), "%d", portnum);

    int error = getaddrinfo(hostname, port, &hints, &ai);
    if (error != 0) {
        if (error != EAI_SYSTEM) {
            // settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            //                                 "getaddrinfo(): %s\n", gai_strerror(error));
        } else {
            // settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            //                                 "getaddrinfo(): %s\n", strerror(error));
        }

        return -1;
    }

    for (next = ai; next; next = next->ai_next) {
        int sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (sock == -1) {
            // settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            //                                 "Failed to create socket: %s\n",
            //                                 strerror(errno));
            continue;
        }

        // If the caller wants non-blocking, set the sock options
        // now so even the connect() becomes non-blocking.
        //
        if (!blocking && (mcs_set_sock_opt(sock) != MCS_SUCCESS)) {
            close(sock);
            continue;
        }

        if (connect(sock, ai->ai_addr, ai->ai_addrlen) == -1) {
            int errno_last;
#ifdef WIN32
            errno_last = WSAGetLastError();
#else
            errno_last = errno;
#endif
            if (errno_out != NULL) {
                *errno_out = errno_last;
            }

            if (!blocking && (errno_last == EINPROGRESS ||
                              errno_last == EWOULDBLOCK)) {
                ret = sock;
                break;
            }

            // settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
            //                                 "Failed to connect socket: %s\n",
            //                                 strerror(errno));
            close(sock);
            continue;
        }

        if (mcs_set_sock_opt(sock) == MCS_SUCCESS) {
            ret = sock;
            break;
        }

        close(sock);
    }

    freeaddrinfo(ai);

    return ret;
}

mcs_return mcs_set_sock_opt(int sock) {
    /* jsh: todo
       TODO: from zstored set_socket_options()...

    if (fd type == MEMCACHED_CONNECTION_UDP)
       return true;

#ifdef HAVE_SNDTIMEO
    if (ptr->root->snd_timeout) {
        int error;
        struct timeval waittime;

        waittime.tv_sec = 0;
        waittime.tv_usec = ptr->root->snd_timeout;

        error = setsockopt(ptr->fd, SOL_SOCKET, SO_SNDTIMEO,
                           &waittime, (socklen_t)sizeof(struct timeval));
        WATCHPOINT_ASSERT(error == 0);
    }
#endif

#ifdef HAVE_RCVTIMEO
    if (ptr->root->rcv_timeout) {
        int error;
        struct timeval waittime;

        waittime.tv_sec = 0;
        waittime.tv_usec = ptr->root->rcv_timeout;

        error= setsockopt(ptr->fd, SOL_SOCKET, SO_RCVTIMEO,
                          &waittime, (socklen_t)sizeof(struct timeval));
        WATCHPOINT_ASSERT(error == 0);
    }
#endif

  {
    int error;
    struct linger linger;

    linger.l_onoff = 1;
    linger.l_linger = DOWNSTREAM_DEFAULT_LINGER;
    error = setsockopt(fd, SOL_SOCKET, SO_LINGER,
                       &linger, (socklen_t)sizeof(struct linger));
  }

  if (ptr->root->send_size) {
    int error;

    error= setsockopt(ptr->fd, SOL_SOCKET, SO_SNDBUF,
                      &ptr->root->send_size, (socklen_t)sizeof(int));
    WATCHPOINT_ASSERT(error == 0);
  }

  if (ptr->root->recv_size) {
    int error;

    error= setsockopt(ptr->fd, SOL_SOCKET, SO_RCVBUF,
                      &ptr->root->recv_size, (socklen_t)sizeof(int));
    WATCHPOINT_ASSERT(error == 0);
  }
  */

    int flags = fcntl(sock, F_GETFL, 0);
    if (flags < 0 ||
        fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
        return MCS_FAILURE;
    }

    flags = 1;

    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY,
               &flags, (socklen_t) sizeof(flags));

    return MCS_SUCCESS;
}

ssize_t mcs_io_write(int fd, const void *buffer, size_t length) {
    assert(fd != -1);

    return write(fd, buffer, length);
}

mcs_return mcs_io_read(int fd, void *dta, size_t size, struct timeval *timeout_in) {
    struct timeval my_timeout; // Linux select() modifies its timeout param.
    struct timeval *timeout = NULL;

    if (timeout_in != NULL &&
        (timeout_in->tv_sec != 0 ||
         timeout_in->tv_usec != 0)) {
        my_timeout = *timeout_in;
        timeout = &my_timeout;
    }

    char *data = dta;
    size_t done = 0;

    while (done < size) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(fd, &readfds);

        fd_set errfds;
        FD_ZERO(&errfds);
        FD_SET(fd, &errfds);

        int s = select(fd + 1, &readfds, NULL, &errfds, timeout);
        if (s == 0) {
            return MCS_TIMEOUT;
        }

        if (s != 1 || FD_ISSET(fd, &errfds) || !FD_ISSET(fd, &readfds)) {
            return MCS_FAILURE;
        }

        ssize_t n = read(fd, data + done, 1);
        if (n == -1 || n == 0) {
            return MCS_FAILURE;
        }

        done += (size_t) n;
    }

    return MCS_SUCCESS;
}

void mcs_io_reset(int fd) {
    (void) fd;

    // TODO: memcached_io_reset(ptr);
}

const char *mcs_server_st_hostname(mcs_server_st *ptr) {
    return ptr->hostname;
}

int mcs_server_st_port(mcs_server_st *ptr) {
    return ptr->port;
}

int mcs_server_st_fd(mcs_server_st *ptr) {
    return ptr->fd;
}

const char *mcs_server_st_usr(mcs_server_st *ptr) {
    return ptr->usr;
}

const char *mcs_server_st_pwd(mcs_server_st *ptr) {
    return ptr->pwd;
}

char *mcs_server_st_ident(mcs_server_st *msst, bool is_ascii) {
    assert(msst != NULL);

    char *buf = is_ascii ? msst->ident_a : msst->ident_b;
    if (buf[0] == '\0') {
        const char *usr = mcs_server_st_usr(msst);
        const char *pwd = mcs_server_st_pwd(msst);

        snprintf(buf, MCS_IDENT_SIZE,
                 "%s:%d:%s:%s:%d",
                 mcs_server_st_hostname(msst),
                 mcs_server_st_port(msst),
                 usr, pwd,
                 is_ascii);
    }

    return buf;
}


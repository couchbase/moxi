#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>

#ifdef WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#define strdup _strdup
#define sleep(a) Sleep(a * 1000)
#else
#include <unistd.h>
#include <sys/socket.h>
#endif

#include <string.h>
#include <curl/curl.h>

#include <libconflate/conflate.h>
#include "rest.h"
#include "conflate_internal.h"

long curl_init_flags = CURL_GLOBAL_ALL;

static int g_tot_process_new_configs = 0;

struct response_buffer {
    char *data;
    size_t bytes_used;
    size_t buffer_size;
    struct response_buffer *next;
};

struct response_buffer *response_buffer_head = NULL;
struct response_buffer *cur_response_buffer = NULL;

static struct response_buffer *mk_response_buffer(size_t size) {
    struct response_buffer *r =
      (struct response_buffer *) calloc(1, sizeof(struct response_buffer));
    assert(r);
    r->data = malloc(size);
    assert(r->data);
    r->bytes_used = 0;
    r->buffer_size = size;
    r->next = NULL;
    return r;
}

static void free_response(struct response_buffer *response) {
    if (!response) {
        return;
    }
    if (response->next) {
        free_response(response->next);
    }
    if (response->data) {
        free(response->data);
    }
    free(response);
}

static struct response_buffer *write_data_to_buffer(struct response_buffer *buffer,
                                                    const char *data, size_t len) {
    size_t bytes_written = 0;
    while (bytes_written < len) {
        size_t bytes_to_write = (len - bytes_written);
        size_t space = buffer->buffer_size - buffer->bytes_used;
        if (space == 0) {
            struct response_buffer *new_buffer = mk_response_buffer(buffer->buffer_size);
            buffer->next = new_buffer;
            buffer = new_buffer;
        } else {
            char *d;
            if (bytes_to_write > space) {
                bytes_to_write = space;
            }
            d = buffer->data;
            d = &d[buffer->bytes_used];
            memcpy(d,&data[bytes_written], bytes_to_write);
            bytes_written += bytes_to_write;
            buffer->bytes_used += bytes_to_write;
        }
    }
    return buffer;
}

static char *assemble_complete_response(struct response_buffer *response_head) {
    struct response_buffer *cur_buffer = response_head;
    size_t response_size = 0;
    char *response = NULL;
    char *ptr;

    if (response_head == NULL) {
        return NULL;
    }

    /* figure out how big the message is */
    while (cur_buffer) {
        response_size += cur_buffer->bytes_used;
        cur_buffer = cur_buffer->next;
    }

    /* create buffer */
    response = malloc(response_size + 1);
    assert(response);

    /* populate buffer */
    cur_buffer = response_head;
    ptr = response;
    while (cur_buffer) {
        memcpy(ptr, cur_buffer->data, cur_buffer->bytes_used);
        ptr += cur_buffer->bytes_used;
        cur_buffer = cur_buffer->next;
    }

    response[response_size] = '\0';

    return response;
}

static bool pattern_ends_with(const char *pattern, const char *target, size_t target_size) {
    size_t pattern_size;
    assert(target);
    assert(pattern);

    pattern_size = strlen(pattern);
    if (target_size < pattern_size) {
        return false;
    }
    return memcmp(&target[target_size - pattern_size], pattern, pattern_size) == 0;
}

static conflate_result process_new_config(long http_code, conflate_handle_t *conf_handle) {
    char *values[2];
    kvpair_t *kv;
    conflate_result (*call_back)(void *, kvpair_t *);
    conflate_result r;
    char buf[4];

    g_tot_process_new_configs++;

    snprintf(buf, 4, "%ld", http_code),
    values[0] = buf;
    values[1] = NULL;

    kv = mk_kvpair(HTTP_CODE_KEY, values);

    /* construct the new config from its components */
    values[0] = assemble_complete_response(response_buffer_head);

    free_response(response_buffer_head);
    response_buffer_head = NULL;
    cur_response_buffer = NULL;

    if (values[0] == NULL) {
        fprintf(stderr, "ERROR: invalid response from REST server\n");
        return CONFLATE_ERROR;
    }

    kv->next = mk_kvpair(CONFIG_KEY, values);

    if (conf_handle->url != NULL) {
        char *url[2];
        url[0] = conf_handle->url;
        url[1] = NULL;
        kv->next->next = mk_kvpair("url", url);
    }

    /* execute the provided call back */
    call_back = conf_handle->conf->new_config;
    r = call_back(conf_handle->conf->userdata, kv);

    /* clean up */
    free_kvpair(kv);
    free(values[0]);

    response_buffer_head = mk_response_buffer(RESPONSE_BUFFER_SIZE);
    cur_response_buffer = response_buffer_head;

    return r;
}

static size_t handle_response(void *data, size_t s, size_t num, void *cb) {
    conflate_handle_t *c_handle = (conflate_handle_t *) cb;
    size_t size = s * num;
    bool end_of_message = pattern_ends_with(END_OF_CONFIG, data, size);
    cur_response_buffer = write_data_to_buffer(cur_response_buffer, data, size);
    if (end_of_message) {
        process_new_config(200, c_handle);
    }
    return size;
}

static int setup_curl_sock(void *clientp,
                           curl_socket_t curlfd,
                           curlsocktype purpose) {
  int       optval = 1;
  socklen_t optlen = sizeof(optval);
  setsockopt(curlfd, SOL_SOCKET, SO_KEEPALIVE, (void *) &optval, optlen);
  (void) clientp;
  (void) purpose;
  return 0;
}

static void setup_handle(CURL *handle, char *url, char *userpass,
                         conflate_handle_t *chandle,
                         size_t (response_handler)(void *, size_t, size_t, void *)) {
    if (url != NULL) {

        CURLcode c;

        c = curl_easy_setopt(handle, CURLOPT_SOCKOPTFUNCTION, setup_curl_sock);
        assert(c == CURLE_OK);
        c = curl_easy_setopt(handle, CURLOPT_WRITEDATA, chandle);
        assert(c == CURLE_OK);
        c = curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, response_handler);
        assert(c == CURLE_OK);
        c = curl_easy_setopt(handle, CURLOPT_URL, url);
        assert(c == CURLE_OK);

        if (userpass != NULL) {
            c = curl_easy_setopt(handle, CURLOPT_HTTPAUTH, CURLAUTH_BASIC);
            assert(c == CURLE_OK);
            c = curl_easy_setopt(handle, CURLOPT_USERPWD, userpass);
            assert(c == CURLE_OK);
        }

        c = curl_easy_setopt(handle, CURLOPT_HTTPGET, 1);
        assert(c == CURLE_OK);
    }
}

#ifdef WIN32
/*
 * NOTE!!! we are only using "|" as the pattern, so this code will _NOT_
 * work if you change that to include more than a single character!
 */
static char *strsep(char **stringp, char *pattern) {
   char *ptr = *stringp;
   *stringp = strchr(*stringp, pattern[0]);
   if (*stringp != NULL) {
      **stringp = '\0';
      *stringp = (*stringp) + 1;
   }

   return ptr;
}
#endif

void run_rest_conflate(void *arg) {
    conflate_handle_t *handle = (conflate_handle_t *) arg;
    char curl_error_string[CURL_ERROR_SIZE];
    kvpair_t *conf;
    CURLcode c;
    CURL *curl_handle;
    bool always_retry = true;



    /* prep the buffers used to hold the config */
    response_buffer_head = mk_response_buffer(RESPONSE_BUFFER_SIZE);
    cur_response_buffer = response_buffer_head;

    /* Before connecting and all that, load the stored config */
    conf = load_kvpairs(handle, handle->conf->save_path);
    if (conf) {
        handle->conf->new_config(handle->conf->userdata, conf);
        free_kvpair(conf);
    }

    /* init curl */
    c = curl_global_init(curl_init_flags);
    assert(c == CURLE_OK);

    curl_handle = curl_easy_init();
    assert(curl_handle);

    curl_easy_setopt(curl_handle, CURLOPT_ERRORBUFFER, &curl_error_string);

    while (true) {
        int start_tot_process_new_configs = g_tot_process_new_configs;
        bool succeeding = true;

        while (succeeding) {
            char *urls = strdup(handle->conf->host);  /* Might be a '|' delimited list of url's. */
            char *next = urls;
            char *userpass = NULL;
            succeeding = false;

            if (handle->conf->jid && strlen(handle->conf->jid)) {
                size_t buff_size = strlen(handle->conf->jid) + strlen(handle->conf->pass) + 2;
                userpass = (char *) malloc(buff_size);
                assert(userpass);
                snprintf(userpass, buff_size, "%s:%s", handle->conf->jid, handle->conf->pass);
                userpass[buff_size - 1] = '\0';
            }

            while (next != NULL) {
                char *url = strsep(&next, "|");

                handle->url = url;

                setup_handle(curl_handle,
                             url,  /* The full URL. */
                             userpass, /* The auth user and password. */
                             handle, handle_response);

                if (curl_easy_perform(curl_handle) == 0) {
                    long http_code = 0;
                    if (curl_easy_getinfo(curl_handle, CURLINFO_RESPONSE_CODE, &http_code) != CURLE_OK) {
                        http_code = 0;
                    }
                    /* We reach here if the REST server didn't provide a
                       streaming JSON response and so we need to process
                       the just-one-JSON response */
                    conflate_result r = process_new_config(http_code, handle);
                    if (r == CONFLATE_SUCCESS ||
                        r == CONFLATE_ERROR) {
                      /* Restart at the beginning of the urls list */
                      /* on either a success or a 'local' error. */
                      /* In contrast, if the callback returned a */
                      /* value of CONFLATE_ERROR_BAD_SOURCE, then */
                      /* we should try the next url on the list. */
                      succeeding = true;
                      next = NULL;
                    }
                } else {
                    fprintf(stderr, "WARNING: curl error: %s from: %s\n",
                            curl_error_string, url);
                }
            }

            sleep(1);  /* Don't overload the REST servers with tons of retries. */

            free(urls);
            free(userpass);
        }

        if (start_tot_process_new_configs == g_tot_process_new_configs) {
            if (start_tot_process_new_configs == 0) {
                fprintf(stderr, "WARNING: could not contact REST server(s): %s;"
                        " perhaps they are unavailable or still initializing\n",
                        handle->conf->host);
            } else {
                fprintf(stderr, "ERROR: could not contact REST server(s): %s\n",
                        handle->conf->host);
            }

            if (always_retry == false) {
              /* If we went through all our URL's and didn't see any new */
              /* configs, then stop trying. */

              break;
            }
        }
    }

    free_response(response_buffer_head);
    response_buffer_head = NULL;
    cur_response_buffer = NULL;

    curl_easy_cleanup(curl_handle);

    exit(1);
}

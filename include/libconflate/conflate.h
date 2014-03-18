#ifndef LIBCONFLATE_CONFLATE_H
#define LIBCONFLATE_CONFLATE_H 1

#include <platform/platform.h>
#include <stdbool.h>
#include <sys/types.h>

#ifdef __cpluscplus
extern "C" {
#endif

/*! \mainpage libconflate
 *
 * \section intro_sec Introduction
 *
 * libconflate is a library that helps you configure large
 * installations of applications easily.
 *
 * It's designed to be easy to use, safe, and autonomous while still
 * leveraging centralized resources to allow you to have your clusters
 * easily adapt to change.
 *
 * \section docs_sec API Documentation
 *
 * Jump right into <a href="modules.html">the modules docs</a> to get started.
 */

/* Deal with an ICC annoyance.  It tries hard to pretend to be GCC<
   but doesn't understand its attributes properly. */
#ifndef __libconflate_gcc_attribute__
#if __GNUC__ && !(defined (__ICC) || defined (__SUNPRO_C))
# define __libconflate_gcc_attribute__ __attribute__
#else
# define __libconflate_gcc_attribute__(x)
#endif
#endif

#ifdef BUILDING_LIBCONFLATE

#if defined (__SUNPRO_C) && (__SUNPRO_C >= 0x550)
#define LIBCONFLATE_PUBLIC_API __global
#elif defined __GNUC__
#define LIBCONFLATE_PUBLIC_API __attribute__ ((visibility("default")))
#elif defined(_MSC_VER)
#define LIBCONFLATE_PUBLIC_API extern __declspec(dllexport)
#else
/* unknown compiler */
#define LIBCONFLATE_PUBLIC_API
#endif

#else

#if defined(_MSC_VER)
#define LIBCONFLATE_PUBLIC_API extern __declspec(dllimport)
#else
#define LIBCONFLATE_PUBLIC_API
#endif

#endif

/* Forward declaration */
typedef struct _conflate_handle conflate_handle_t;

/**
 * \defgroup Core Core Functionality
 * \defgroup Extending Adding Management Commands
 * \defgroup kvpairs Simple lisp-style Associative Lists
 * \defgroup Logging Logging Facilities
 * \defgroup Persistence Long-Term Persistence API
 */

/**
* \addtogroup kvpairs
 * @{
 */

/**
 * A linked list of keys each which may have zero or more values.
 */
typedef struct kvpair {
    /**
     * The key in this kv pair.
     */
    char*  key;
    /**
     * A NULL-terminated list of values.
     */
    char** values;

    /** \private */
    int    allocated_values;
    /** \private */
    int    used_values;

    /**
     * The next kv pair in this list.  NULL if this is the last.
     */
    struct kvpair* next;
} kvpair_t;

/**
 * Visitor callback for walking a kvpair_t.
 *
 * @param opaque opaque value passed into walk
 * @param key the kvpair_t key
 * @param values the kvpair_t values
 *
 * @return true if traversal should continue
 */
typedef bool (*kvpair_visitor_t)(void *opaque,
                                 const char *name,
                                 const char **values);

/**
 * Create a kvpair_t.
 *
 * When building out a kvpair chain, one would presumably set the
 * `next' param to the first item in your existing chain.
 *
 * @param k the key for this kvpair
 * @param v (optional) the list of values for this key.
 * @return a newly allocated kvpair_t
 */
LIBCONFLATE_PUBLIC_API
kvpair_t* mk_kvpair(const char* k, char** v)
    __libconflate_gcc_attribute__ ((warn_unused_result, nonnull (1)));

/**
 * Add a value to a kvpair_t.
 *
 * @param kvpair the current kvpair that needs a new value
 * @param value the new value
 */
LIBCONFLATE_PUBLIC_API
void add_kvpair_value(kvpair_t* kvpair, const char* value)
    __libconflate_gcc_attribute__ ((nonnull (1, 2)));

/**
 * Find a kvpair with the given key.
 *
 * @param pair start of a pair chain
 * @param key the desired key
 *
 * @return the pair with the given key, or NULL if no such pair is found
 */
LIBCONFLATE_PUBLIC_API
kvpair_t* find_kvpair(kvpair_t* pair, const char* key)
    __libconflate_gcc_attribute__ ((warn_unused_result, nonnull (2)));

/**
 * Find a simple value from a kvpair list.
 *
 * If a kvpair_t is found with the given key, return the first value.
 * This does not duplicate the value, so you should neither free it,
 * nor free the kvpair containing it as long as you're intending to
 * use it.
 *
 * @param pair the pair to search
 * @param key the key to find
 * @return a pointer to the first value found, or NULL if none was found
 */
LIBCONFLATE_PUBLIC_API
char *get_simple_kvpair_val(kvpair_t *pair, const char *key)
    __libconflate_gcc_attribute__ ((warn_unused_result, nonnull (2)));

/**
 * Copy a chain of kvpairs.
 *
 * @param pair the pair to duplicate (recursively)
 *
 * @return a complete deep copy of the kvpair structure
 */
LIBCONFLATE_PUBLIC_API
kvpair_t *dup_kvpair(kvpair_t *pair)
    __libconflate_gcc_attribute__ ((warn_unused_result, nonnull (1)));

/**
 * Walk a kvpair.
 *
 * @param pair the kvpair to walk
 * @param opaque a value to be transparently passed to the visitor
 * @param visitor the visitor to call for each kvpair_t
 */
LIBCONFLATE_PUBLIC_API
void walk_kvpair(kvpair_t *pair, void *opaque, kvpair_visitor_t visitor)
    __libconflate_gcc_attribute__ ((nonnull(1, 3)));

/**
 * Free a chain of kvpairs.
 *
 * @param pair the pair to free (recursively)
 */
LIBCONFLATE_PUBLIC_API
void free_kvpair(kvpair_t* pair);

/**
 * @}
 */

/**
 * \addtogroup Extending
 * @{
 *
 * Extension functions allow libconflate consumers to easily provide
 * new management functionality specific to their own applications.
 *
 * Register your callbacks with ::conflate_register_mgmt_cb and
 * libconflate will take over from there.
 */

/**
 * Callback response form builder.
 *
 * \sa ::conflate_add_field
 * \sa ::conflate_add_field_multi
 * \sa ::conflate_next_fieldset
 */
typedef struct _conflate_form_result conflate_form_result;

/**
 * Add a single k/v pair in a response form.
 *
 * @param r the form as handed to the callback
 * @param k a form key (may not be NULL)
 * @param v a form value (may not be NULL)
 */
LIBCONFLATE_PUBLIC_API
void conflate_add_field(conflate_form_result *r, const char *k, const char *v)
    __libconflate_gcc_attribute__ ((nonnull (1, 2, 3)));

/**
 * Add a multi-valued key to a response form.
 *
 * @param r the form as handed to the callback
 * @param k the form key (may not be NULL)
 * @param v the response values (may not be NULL -- NULL terminated)
 */
LIBCONFLATE_PUBLIC_API
void conflate_add_field_multi(conflate_form_result *r, const char *k,
                              const char **v)
    __libconflate_gcc_attribute__ ((nonnull (1, 2, 3)));

/**
 * Create a response container for a multi-set response.
 *
 * This is used for callbacks that want to build tabular results
 * (i.e. a list of k/v pairs).
 *
 * @param r the form as handed to the callback
 */
LIBCONFLATE_PUBLIC_API
void conflate_next_fieldset(conflate_form_result *r)
    __libconflate_gcc_attribute__ ((nonnull (1)));

/**
 * Initialize the result form if it's not already initialized.
 *
 * This is useful for the case where an empty form may be desirable.
 *
 * If a callback is issued that returns without adding fields or
 * calling conflate_init_form, no form will be returned.
 *
 * If conflate_init_form is called and no fields are added, an
 * \e empty form will be returned.
 */
LIBCONFLATE_PUBLIC_API
void conflate_init_form(conflate_form_result *r);

/**
 * Callback return types indicating status and result type of a
 * management callback.
 */
enum conflate_mgmt_cb_result {
    RV_OK,     /**< Invocation worked as expected */
    RV_ERROR,  /**< Invocation failed. */
    RV_BADARG  /**< Bad/incomplete arguments */
};

/**
 * Callback invoked to process a management command.
 *
 * @param opaque registered opaque value
 * @param handle the conflate handle
 * @param cmd the name of the command being executed
 * @param direct if true, this is a directed command (else issued via pubsub)
 * @param pair the form sent with this command (may be NULL)
 * @param r the result form being built
 */
typedef enum conflate_mgmt_cb_result (*conflate_mgmt_cb_t)(void *opaque,
                                                           conflate_handle_t *handle,
                                                           const char *cmd,
                                                           bool direct,
                                                           kvpair_t *pair,
                                                           conflate_form_result *r);

/**
 * Register a management command handler.
 *
 * Callbacks allow applications using libconflate to register
 * arbitrary callbacks to extend the capabilities of the management
 * layer.
 *
 * There are three major forms of success results:
 *
 * - Empty (no specific results necessary)
 * - A simple key/multi-value list.
 * - A list of key/multi-value lists.
 *
 * See the definition of ::conflate_mgmt_cb_t for more information on
 * result types.
 *
 * @param cmd the node name of the command
 * @param desc short description of the command
 * @param cb the callback to issue when this command is invoked
 */
LIBCONFLATE_PUBLIC_API
void conflate_register_mgmt_cb(const char *cmd, const char *desc,
                               conflate_mgmt_cb_t cb)
    __libconflate_gcc_attribute__ ((nonnull (1, 2, 3)));

/**
 * @}
 */

/**
 * \addtogroup Logging
 * @{
 */

#ifdef ENABLE_LIBCONFLATE_DEPRECATED_LOGGING
#define DEBUG LOG_LVL_DEBUG
#define INFO LOG_LVL_INFO
#define WARN LOG_LVL_WARN
#define ERROR LOG_LVL_ERROR
#define FATAL LOG_LVL_FATAL
#endif

/**
 * Log levels.
 */
enum conflate_log_level {
    LOG_LVL_DEBUG, /**< Various debug messages (for example xmpp stanzas) */
    LOG_LVL_INFO,  /**< Info messages that might be helpful */
    LOG_LVL_WARN,  /**< Warnings of stuff going bad */
    LOG_LVL_ERROR, /**< Noteworthy error conditions */
    LOG_LVL_FATAL  /**< The rapture is upon us */
};

/**
 * Logging implementation that logs to syslog.
 *
 * This is the default logger.
 */
LIBCONFLATE_PUBLIC_API
void conflate_syslog_logger(void *, enum conflate_log_level,
                            const char *, ...);

/**
 * Logging implementation that logs to stderr.
 *
 * The stderr logger is somewhat primitive in that it does not have
 * any sort of level filter, but useful for running apps on console
 * with verbosity.
 */
LIBCONFLATE_PUBLIC_API
void conflate_stderr_logger(void *, enum conflate_log_level,
                            const char *, ...);

/**
 * @}
 */

/**
 * \addtogroup Core
 *
 * Core consists of two simple functions.  ::init_conflate is used to
 * provide default values to a configuration and ::start_conflate
 * launches the communication loop.
 *
 * \example examples/bot.c
 */

/**
 * Conflate result codes.
 */
typedef enum {
    CONFLATE_SUCCESS,
    CONFLATE_ERROR,
    CONFLATE_ERROR_BAD_SOURCE
} conflate_result;

/**
 * Configuration for a conflatee.
 */
typedef struct {

    /** The XMPP JID (typically excludes a resource definition) */
    char *jid;

    /** The XMPP Password */
    char *pass;

    /**
     * The XMPP server address (may be NULL).
     *
     * This is optional -- setting this to NULL will allow for normal
     * XMPP SRV lookups to locate the server.
     */
    char *host;

    /**
     * Name of the software using libconflate.
     *
     * For example:  awesomeproxy
     */
    char *software;

    /**
     * Version number of the software running libconflate.
     *
     * For example:  1.3.3-8-gee0c3d5
     */
    char *version;

    /**
     * Path to persist configuration for faster/more reliable restarts.
     */
    char *save_path;

    /**
     * User defined data to be passed to callbacks.
     */
    void *userdata;

    /**
     * Logging callback (optional).
     *
     * If you do not specify a logger, one will be suplied for you
     * that sends a lot of stuff to stderr.
     *
     * @param udata The client's custom user data
     * @param level log level (see ::conflate_log_level)
     * @param msg the message to log
     */
    void (*log)(void *udata, enum conflate_log_level level, const char *msg, ...)
        __libconflate_gcc_attribute__ ((format (printf, 3, 4)));

    /**
     * Callback issued when a new configuration is to be activated.
     *
     * This callback will receive the caller's userdata and a kvpair_t
     * of all of the keys and values received when things changed.
     *
     * The new config *may* be the same as the previous config.  It's
     * up to the client to detect and decide what to do in this case.
     *
     * The callback should return CONFLATE_SUCCESS on success.
     */
    conflate_result (*new_config)(void*, kvpair_t*);

    /** \private */
    void *initialization_marker;

} conflate_config_t;

/**
 * @}
 */

/**
 * \addtogroup Core
 * @{
 */

/**
 * Initialize the given configuration to defaults.
 *
 * This should be called *before* you fill in your config.
 *
 * @param conf configuration to be initialized
 */
LIBCONFLATE_PUBLIC_API
void init_conflate(conflate_config_t *conf) __libconflate_gcc_attribute__ ((nonnull (1)));

/**
 * The main entry point for starting a conflate agent.
 *
 * @param conf configuration for libconflate
 *
 * @return true if libconflate was able to properly initialize itself
 */
LIBCONFLATE_PUBLIC_API
bool start_conflate(conflate_config_t conf) __libconflate_gcc_attribute__ ((warn_unused_result));

/**
 * @}
 */

/* Misc */
LIBCONFLATE_PUBLIC_API
char* safe_strdup(const char*);
LIBCONFLATE_PUBLIC_API
void free_string_list(char **);

/**
 * Create a copy of a config.
 *
 * @param c the configuration to duplicate
 *
 * @return a deep copy of the configuration
 */
LIBCONFLATE_PUBLIC_API
conflate_config_t* dup_conf(conflate_config_t c);

/**
 * \addtogroup Persistence
 * @{
 */

/**
 * Load the key/value pairs from the file at the given path.
 *
 * @param handle the conflate handle (for logging contexts and stuff)
 * @param filename the path from which the config should be read
 *
 * @return the config, or NULL if the config could not be read for any reason
 */
LIBCONFLATE_PUBLIC_API
kvpair_t* load_kvpairs(conflate_handle_t *handle, const char *filename)
    __libconflate_gcc_attribute__ ((warn_unused_result, nonnull(1, 2)));

/**
 * Save a config at the given path.
 *
 * @param handle the conflate handle (for logging contexts and stuff)
 * @param pairs the kvpairs to store
 * @param filename the path to which the config should be written
 *
 * @return false if the configuration could not be saved for any reason
 */
LIBCONFLATE_PUBLIC_API
bool save_kvpairs(conflate_handle_t *handle, kvpair_t* pairs, const char *filename)
    __libconflate_gcc_attribute__ ((warn_unused_result, nonnull(1, 2, 3)));

/**
 * Save some instance-private data.
 *
 * @param handle the conflate handle (for logging contexts and stuff)
 * @param k the key to store
 * @param v the value to store
 * @param filename the path to which the data should be written
 *
 * @return false if the data could not be saved for any reason
 */
LIBCONFLATE_PUBLIC_API
bool conflate_save_private(conflate_handle_t *handle,
                           const char *k, const char *v, const char *filename)
    __libconflate_gcc_attribute__ ((warn_unused_result, nonnull(1, 2, 3, 4)));

/**
 * Delete some saved instance-private data.
 *
 * @param handle the conflate handle (for logging contexts and stuff)
 * @param k the key to delete
 * @param filename the path from which the data should be removed
 *
 * @return false if the data could not be deleted for any reason
 */
LIBCONFLATE_PUBLIC_API
bool conflate_delete_private(conflate_handle_t *handle,
                             const char *k, const char *filename)
    __libconflate_gcc_attribute__ ((warn_unused_result, nonnull(1, 2, 3)));

/**
 * Get some saved instance-private data.
 *
 * @param handle the conflate handle (for logging contexts and stuff)
 * @param k the key to look up
 * @param filename the path from which the data should be retrieved
 *
 * @return an allocated value or NULL if one could not be retrieved
 */
LIBCONFLATE_PUBLIC_API
char *conflate_get_private(conflate_handle_t *handle,
                           const char *k, const char *filename)
    __libconflate_gcc_attribute__ ((warn_unused_result, nonnull(1, 2, 3)));

/**
 * @}
 */
#ifdef __cplusplus
}
#endif

#endif /* CONFLATE_H */

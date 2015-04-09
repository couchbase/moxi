/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <strings.h>

#include <libvbucket/vbucket.h>
#include <platform/cbassert.h>

#include "macros.h"

struct key_st {
    char *key;
    int vbucket;
};

static const struct key_st keys[] =
{
    { "hello", 0 },
    { "doctor", 0 },
    { "name", 3 },
    { "continue", 3 },
    { "yesterday", 0 },
    { "tomorrow", 1 },
    { "another key", 2 },
    { NULL, -1 }
};

static const char *servers[] = { "server1:11211",
                                 "server2:11210",
                                 "server3:11211" };

struct vb_st {
    int master;
    int replicas[2];
};

static const struct vb_st vbuckets[] =
{
    { 0, { 1, 2 } },
    { 1, { 2, 0 } },
    { 2, { 1, -1 } },
    { 1, { 2, 0 } }
};

static char *configPath(const char *fname) {
    static char buffer[FILENAME_MAX];
    const char *root = getenv("CMAKE_CURRENT_SOURCE_DIR");
    struct stat st;

    if (root == NULL) {
        root = ".";
    }

    snprintf(buffer, FILENAME_MAX, "%s/tests/vbucket/config/testapp-%s",
             root, fname);
    if (stat(buffer, &st) == -1) {
        snprintf(buffer, FILENAME_MAX, "%s/tests/vbucket/config/%s",
                 root, fname);
        if (stat(buffer, &st) == -1) {
            fprintf(stderr, "cannot find config %s\n", fname);
            abort();
        }
    }

    return buffer;
}

static void testConfig(const char *fname) {
    int whoops = 0;
    const struct key_st *k;
    int i = 0;

    VBUCKET_CONFIG_HANDLE vb = vbucket_config_parse_file(configPath(fname));
    if (vb == NULL) {
        fprintf(stderr, "vbucket_config_parse_file error: %s\n",
                vbucket_get_error());
        abort();
    }

    while ((k = &keys[i++])->key != NULL) {
        int id = vbucket_get_vbucket_by_key(vb, k->key, strlen(k->key));
        if (id != k->vbucket) {
            fprintf(stderr, "Expected vbucket %d for key '%s' but got %d\n",
                    k->vbucket, k->key, id);
            whoops = 1;
        }
    }

    if (whoops) {
        abort();
    }

    cb_assert(vbucket_config_get_num_servers(vb) == 3 || vbucket_config_get_num_servers(vb) == 4);
    cb_assert(vbucket_config_get_num_replicas(vb) == 2);

    for (i = 0; i < 3; ++i) {
        cb_assert(strcmp(vbucket_config_get_server(vb, i), servers[i]) == 0);
    }

    for (i = 0; i < 4; ++i) {
        cb_assert(vbucket_get_master(vb, i) == vbuckets[i].master);
        cb_assert(vbucket_get_replica(vb, i, 0) == vbuckets[i].replicas[0]);
        cb_assert(vbucket_get_replica(vb, i, 1) == vbuckets[i].replicas[1]);
    }

    cb_assert(vbucket_config_get_user(vb) == NULL);
    cb_assert(vbucket_config_get_password(vb) == NULL);

    vbucket_config_destroy(vb);
}


static void testWrongServer(const char *fname) {
    VBUCKET_CONFIG_HANDLE vb = vbucket_config_parse_file(configPath(fname));
    if (vb == NULL) {
        fprintf(stderr, "vbucket_config_parse_file error: %s\n",
                vbucket_get_error());
        abort();
    }

    /* Starts at 0 */
    cb_assert(vbucket_get_master(vb, 0) == 0);
    /* Does not change when I told it I found the wrong thing */
    cb_assert(vbucket_found_incorrect_master(vb, 0, 1) == 0);
    cb_assert(vbucket_get_master(vb, 0) == 0);
    /* Does change if I tell it I got the right thing and it was wrong. */
    cb_assert(vbucket_found_incorrect_master(vb, 0, 0) == 1);
    cb_assert(vbucket_get_master(vb, 0) == 1);
    /* ...and again */
    cb_assert(vbucket_found_incorrect_master(vb, 0, 1) == 2);
    cb_assert(vbucket_get_master(vb, 0) == 2);
    /* ...and then wraps */
    cb_assert(vbucket_found_incorrect_master(vb, 0, 2) == 0);
    cb_assert(vbucket_get_master(vb, 0) == 0);

    vbucket_config_destroy(vb);
}

static void testWrongNumVbuckets(const char *fname) {
    VBUCKET_CONFIG_HANDLE vb = vbucket_config_create();
    cb_assert(vb != NULL);
    cb_assert(vbucket_config_parse(vb, LIBVBUCKET_SOURCE_FILE, configPath(fname)) != 0);
    cb_assert(strcmp(vbucket_get_error_message(vb),
                  "Number of vBuckets must be a power of two > 0 and <= 65536") == 0);
    vbucket_config_destroy(vb);
}

static void testZeroNumVbuckets(const char *fname) {
    VBUCKET_CONFIG_HANDLE vb = vbucket_config_create();
    cb_assert(vb != NULL);
    cb_assert(vbucket_config_parse(vb, LIBVBUCKET_SOURCE_FILE, configPath(fname)) != 0);
    cb_assert(strcmp(vbucket_get_error_message(vb),
                  "No vBuckets available; service maybe still initializing") == 0);
    vbucket_config_destroy(vb);
}

static void testWrongServerFFT(const char *fname) {
    VBUCKET_CONFIG_HANDLE vb = vbucket_config_parse_file(configPath(fname));
    int rv = 0;
    int nvb = 0;
    int i = 0;

    if (vb == NULL) {
        fprintf(stderr, "vbucket_config_parse_file error: %s\n",
                vbucket_get_error());
        abort();
    }

    /* found incorrect master should not be the same as get master now */
    nvb = vbucket_config_get_num_vbuckets(vb);
    for (i = 0; i < nvb; i++) {
        rv = vbucket_get_master(vb, i);
        cb_assert(rv != vbucket_found_incorrect_master(vb, i, rv));
    }
    /* the ideal test case should be that we check that the vbucket */
    /* and the fvbucket map are identical at this point. TODO untill */
    /* we have a vbucketlib function that diffs vbuckets and fvbuckets */
    vbucket_config_destroy(vb);
}

static void testConfigDiff(void) {
    VBUCKET_CONFIG_HANDLE vb1 = vbucket_config_parse_file(configPath("config-diff1"));
    VBUCKET_CONFIG_HANDLE vb2 = vbucket_config_parse_file(configPath("config-diff2"));
    VBUCKET_CONFIG_DIFF *diff;
    cb_assert(vb2);

    diff = vbucket_compare(vb1, vb2);
    cb_assert(vb1);
    cb_assert(diff);

    cb_assert(diff->sequence_changed);
    cb_assert(diff->n_vb_changes == 1);
    cb_assert(strcmp(diff->servers_added[0], "server4:11211") == 0);
    cb_assert(diff->servers_added[1] == NULL);
    cb_assert(strcmp(diff->servers_removed[0], "server3:11211") == 0);
    cb_assert(diff->servers_removed[1] == NULL);

    vbucket_free_diff(diff);
    vbucket_config_destroy(vb2);

    vb2 = vbucket_config_parse_file(configPath("config-diff3"));
    cb_assert(vb2);

    diff = vbucket_compare(vb1, vb2);
    cb_assert(diff);

    cb_assert(diff->sequence_changed);
    cb_assert(diff->n_vb_changes == -1);
    cb_assert(diff->servers_added[0] == NULL);
    cb_assert(strcmp(diff->servers_removed[0], "server3:11211") == 0);
    cb_assert(diff->servers_removed[1] == NULL);
}

static void testConfigDiffSame(void) {
    VBUCKET_CONFIG_HANDLE vb1 = vbucket_config_parse_file(configPath("config"));
    VBUCKET_CONFIG_HANDLE vb2 = vbucket_config_parse_file(configPath("config"));
    VBUCKET_CONFIG_DIFF *diff;
    cb_assert(vb1);
    cb_assert(vb2);
    diff = vbucket_compare(vb1, vb2);
    cb_assert(diff);

    cb_assert(diff->sequence_changed == 0);
    cb_assert(diff->n_vb_changes == 0);
    cb_assert(diff->servers_added[0] == NULL);
    cb_assert(diff->servers_removed[0] == NULL);

    vbucket_free_diff(diff);
    vbucket_config_destroy(vb1);
    vbucket_config_destroy(vb2);
}

static void testConfigDiffKetamaSame(void) {
    VBUCKET_CONFIG_HANDLE vb1 = vbucket_config_parse_file(configPath("ketama-eight-nodes"));
    VBUCKET_CONFIG_HANDLE vb2 = vbucket_config_parse_file(configPath("ketama-ordered-eight-nodes"));
    VBUCKET_CONFIG_DIFF *diff;
    cb_assert(vb1);
    cb_assert(vb2);
    diff = vbucket_compare(vb1, vb2);
    cb_assert(diff);

    cb_assert(diff->sequence_changed == 0);
    cb_assert(diff->n_vb_changes == 0);
    cb_assert(diff->servers_added[0] == NULL);
    cb_assert(diff->servers_removed[0] == NULL);

    vbucket_free_diff(diff);
    vbucket_config_destroy(vb1);
    vbucket_config_destroy(vb2);
}

static void testConfigUserPassword(void) {
    VBUCKET_CONFIG_HANDLE vb1;
    VBUCKET_CONFIG_HANDLE vb2;
    VBUCKET_CONFIG_DIFF *diff;

    vb1 = vbucket_config_parse_file(configPath("config-user-password1"));
    cb_assert(vb1);
    cb_assert(strcmp(vbucket_config_get_user(vb1), "theUser") == 0);
    cb_assert(strcmp(vbucket_config_get_password(vb1), "thePassword") == 0);

    vb2 = vbucket_config_parse_file(configPath("config-user-password2"));
    cb_assert(vb2);
    cb_assert(strcmp(vbucket_config_get_user(vb2), "theUserIsDifferent") == 0);
    cb_assert(strcmp(vbucket_config_get_password(vb2), "thePasswordIsDifferent") == 0);

    diff = vbucket_compare(vb1, vb2);
    cb_assert(diff);

    cb_assert(diff->sequence_changed);
    cb_assert(diff->n_vb_changes == 0);
    cb_assert(diff->servers_added[0] == NULL);
    cb_assert(diff->servers_removed[0] == NULL);

    vbucket_free_diff(diff);

    diff = vbucket_compare(vb1, vb1);
    cb_assert(diff);

    cb_assert(diff->sequence_changed == 0);
    cb_assert(diff->n_vb_changes == 0);
    cb_assert(diff->servers_added[0] == NULL);
    cb_assert(diff->servers_removed[0] == NULL);

    vbucket_free_diff(diff);

    vbucket_config_destroy(vb1);
    vbucket_config_destroy(vb2);
}

static void testConfigCouchApiBase(void)
{
    VBUCKET_CONFIG_HANDLE vb = vbucket_config_parse_file(configPath("config-couch-api-base"));
    cb_assert(vb);
    cb_assert(strcmp(vbucket_config_get_couch_api_base(vb, 0), "http://192.168.2.123:9500/default") == 0);
    cb_assert(strcmp(vbucket_config_get_couch_api_base(vb, 1), "http://192.168.2.123:9501/default") == 0);
    cb_assert(strcmp(vbucket_config_get_couch_api_base(vb, 2), "http://192.168.2.123:9502/default") == 0);
    cb_assert(strcmp(vbucket_config_get_rest_api_server(vb, 0), "192.168.2.123:9000") == 0);
    cb_assert(strcmp(vbucket_config_get_rest_api_server(vb, 1), "192.168.2.123:9001") == 0);
    cb_assert(strcmp(vbucket_config_get_rest_api_server(vb, 2), "192.168.2.123:9002") == 0);
    cb_assert(strcmp(vbucket_config_get_server(vb, 0), "192.168.2.123:12000") == 0);
    cb_assert(strcmp(vbucket_config_get_server(vb, 1), "192.168.2.123:12002") == 0);
    cb_assert(strcmp(vbucket_config_get_server(vb, 2), "192.168.2.123:12004") == 0);
}

int main(int argc, char **argv)
{
    char buffer[1024];
    if (argc > 1 && getenv("CMAKE_CURRENT_SOURCE_DIR") == NULL) {
        snprintf(buffer, sizeof(buffer), "CMAKE_CURRENT_SOURCE_DIR=%s",
                 argv[1]);
        putenv(buffer);
    }
  testConfig("config");
  testConfig("config-flat");
  testConfig("config-in-envelope");
  testConfig("config-in-envelope2");
  testConfig("config-in-envelope-fft");
  testWrongServer("config");
  testWrongServerFFT("config-in-envelope-fft");
  testWrongNumVbuckets("config-wrong-num-vbuckets");
  testZeroNumVbuckets("config-zero-num-vbuckets");
  testConfigDiff();
  testConfigDiffSame();
  testConfigUserPassword();
  testConfigCouchApiBase();
  testConfigDiffKetamaSame();
  exit(EXIT_SUCCESS);
}

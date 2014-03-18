#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <libconflate/conflate.h>
#include "conflate_internal.h"

/** \private */
struct command_def {
    char *name;
    char *description;
    conflate_mgmt_cb_t cb;
    struct command_def *next;
};

struct command_def *commands = NULL;

void* run_conflate(void *arg);

void conflate_init_form(conflate_form_result *r)
{
    (void)r;
}

void conflate_next_fieldset(conflate_form_result *r) {
    (void)r;
    assert(0);
}

void conflate_add_field(conflate_form_result *r, const char *k, const char *v) {
    (void)r;
    (void)k;
    (void)v;
    assert(0);
}

void conflate_add_field_multi(conflate_form_result *r, const char *k,
                              const char **v) {
    (void)r;
    (void)k;
    (void)v;
    assert(0);
}

void* run_conflate(void *arg) {
    (void)arg;
    assert(0);
    return NULL;
}

/* ------------------------------------------------------------------------ */

void conflate_register_mgmt_cb(const char *cmd, const char *desc,
                               conflate_mgmt_cb_t cb)
{
    struct command_def *c = calloc(1, sizeof(struct command_def));
    assert(c);

    c->name = safe_strdup(cmd);
    c->description = safe_strdup(desc);
    c->cb = cb;
    c->next = commands;

    commands = c;
}

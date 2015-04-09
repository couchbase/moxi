/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "src/config.h"
#include <platform/cbassert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <src/htgram.h>

static void testSimple(void) {
    HTGRAM_HANDLE h0;
    int i;
    int64_t start;
    int64_t width;
    uint64_t count;

    h0 = htgram_mk(0, 5, 1.0, 3, NULL);
    cb_assert(h0 != NULL);
    cb_assert(htgram_get_bin_start(h0) == 0);
    cb_assert(htgram_get_bin_start_width(h0) == 5);
    cb_assert(htgram_get_bin_width_growth(h0) == 1.0);
    cb_assert(htgram_get_num_bins(h0) == 3);

    start = width = count = 123;
    cb_assert(htgram_get_bin_data(h0, -1, &start, &width, &count) == false);
    cb_assert(start == 123);
    cb_assert(width == 123);
    cb_assert(count == 0);

    for (i = 0; i < (int) htgram_get_num_bins(h0); i++) {
        start = width = count = 123;
        cb_assert(htgram_get_bin_data(h0, i, &start, &width, &count) == true);
        cb_assert(start == i * 5);
        cb_assert(width == 5);
        cb_assert(count == 0);

        htgram_incr(h0, (i * 5) + 0, 1);
        htgram_incr(h0, (i * 5) + 1, 2);
        htgram_incr(h0, (i * 5) + 2, 3);
        htgram_incr(h0, (i * 5) + 5, 0);

        start = width = count = 123;
        cb_assert(htgram_get_bin_data(h0, i, &start, &width, &count) == true);
        cb_assert(start == i * 5);
        cb_assert(width == 5);
        cb_assert(count == 6);
    }

    start = width = count = 123;
    cb_assert(htgram_get_bin_data(h0, i, &start, &width, &count) == false);
    cb_assert(start == 123);
    cb_assert(width == 123);
    cb_assert(count == 0);

    htgram_reset(h0);

    start = width = count = 123;
    cb_assert(htgram_get_bin_data(h0, -1, &start, &width, &count) == false);
    cb_assert(start == 123);
    cb_assert(width == 123);
    cb_assert(count == 0);

    for (i = 0; i < (int) htgram_get_num_bins(h0); i++) {
        start = width = count = 123;
        cb_assert(htgram_get_bin_data(h0, i, &start, &width, &count) == true);
        cb_assert(start == i * 5);
        cb_assert(width == 5);
        cb_assert(count == 0);
    }

    start = width = count = 123;
    cb_assert(htgram_get_bin_data(h0, i, &start, &width, &count) == false);
    cb_assert(start == 123);
    cb_assert(width == 123);
    cb_assert(count == 0);

    htgram_incr(h0, -10000, 111);
    htgram_incr(h0, 10000, 222);

    start = width = count = 123;
    cb_assert(htgram_get_bin_data(h0, -1, &start, &width, &count) == false);
    cb_assert(start == 123);
    cb_assert(width == 123);
    cb_assert(count == 111);

    start = width = count = 123;
    cb_assert(htgram_get_bin_data(h0, i, &start, &width, &count) == false);
    cb_assert(start == 123);
    cb_assert(width == 123);
    cb_assert(count == 222);

    htgram_destroy(h0);
}

static void testChained(void) {
    HTGRAM_HANDLE h0, h1;

    int64_t start;
    int64_t width;
    uint64_t count;
    int i;

    /* Have 200 bins from [0 to 2000), with bin widths of 10. */
    /* Have 36 bins from 2000 onwards, with bin width growing at 1.5, chained. */
    h1 = htgram_mk(2000, 10, 1.5, 36, NULL);
    h0 = htgram_mk(0, 10, 1.0, 200, h1);

    for (i = 0; i < (int) (htgram_get_num_bins(h0) + htgram_get_num_bins(h1)); i++) {
        cb_assert(htgram_get_bin_data(h0, i, &start, &width, &count) == true);
        /* printf("%d %d %d %d\n", i, start, width, count); */
    }

    htgram_incr(h0, 28000000, 111);

    cb_assert(htgram_get_bin_data(h0, 200 + 36 - 1, &start, &width, &count) == true);
    cb_assert(start == 27692301);
    cb_assert(width == 13845150);
    cb_assert(count == 111);

    htgram_reset(h0);

    cb_assert(htgram_get_bin_data(h0, 200 + 36 - 1, &start, &width, &count) == true);
    cb_assert(start == 27692301);
    cb_assert(width == 13845150);
    cb_assert(count == 0);
}

int main(void) {
    testSimple();
    testChained();

    return 0;
}

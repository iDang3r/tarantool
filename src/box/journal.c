/*
 * Copyright 2010-2017, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "journal.h"
#include <small/region.h>
#include <diag.h>

struct journal *current_journal = NULL;

struct journal_queue journal_queue = {
	.max_size = INT64_MAX,
	.size = 0,
	.max_len = INT64_MAX,
	.len = 0,
	.waiters = RLIST_HEAD_INITIALIZER(journal_queue.waiters),
	.is_awake = false,
	.is_ready = false,
};

struct journal_entry *
journal_entry_new(size_t n_rows, struct region *region,
		  journal_write_async_f write_async_cb,
		  void *complete_data)
{
	struct journal_entry *entry;

	size_t size = (sizeof(struct journal_entry) +
		       sizeof(entry->rows[0]) * n_rows);

	entry = region_aligned_alloc(region, size,
				     alignof(struct journal_entry));
	if (entry == NULL) {
		diag_set(OutOfMemory, size, "region", "struct journal_entry");
		return NULL;
	}

	journal_entry_create(entry, n_rows, 0, write_async_cb,
			     complete_data);
	return entry;
}

struct journal_queue_entry {
	/** The fiber waiting for queue space to free. */
	struct fiber *fiber;
	/** A link in all waiting fibers list. */
	struct rlist in_queue;
};

/**
 * Wake up the first waiter in the journal queue.
 */
static inline void
journal_queue_wakeup_first(void)
{
	struct journal_queue_entry *e;
	if (rlist_empty(&journal_queue.waiters))
		goto out;
	/*
	 * When the queue isn't forcefully emptied, no need to wake everyone
	 * else up until there's some free space.
	 */
	if (!journal_queue.is_ready && journal_queue_is_full())
		goto out;
	e = rlist_entry(rlist_first(&journal_queue.waiters), typeof(*e),
			in_queue);
	fiber_wakeup(e->fiber);
	return;
out:
	journal_queue.is_awake = false;
	journal_queue.is_ready = false;
}

void
journal_queue_wakeup(bool force_ready)
{
	assert(!rlist_empty(&journal_queue.waiters));
	if (journal_queue.is_awake)
		return;
	journal_queue.is_awake = true;
	journal_queue.is_ready = force_ready;
	journal_queue_wakeup_first();
}

void
journal_wait_queue(void)
{
	struct journal_queue_entry entry = {
		.fiber = fiber(),
	};
	rlist_add_tail_entry(&journal_queue.waiters, &entry, in_queue);
	/*
	 * Will be waken up by either queue emptying or a synchronous write.
	 */
	while (journal_queue_is_full() && !journal_queue.is_ready)
		fiber_yield();

	assert(&entry.in_queue == rlist_first(&journal_queue.waiters));
	rlist_del(&entry.in_queue);

	journal_queue_wakeup_first();
}

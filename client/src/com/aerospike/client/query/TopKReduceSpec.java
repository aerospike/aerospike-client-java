/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.query;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Client-side merge of a Top-K query's per-node results into the global top k, created by
 * {@link Statement#createTopK()} for each run.
 * <p>
 * Maintains a size-k heap keyed by the order keys, deduplicated by record digest (a record may
 * reach the client from two nodes while its partition migrates, keeping the copy with the later
 * last-update time) with ties broken by digest ascending for stable, deterministic results.
 */
final class TopKReduceSpec {
	private final OrderBy[] keys;
	private final int limit;

	// Key equality is namespace and digest, and a query has one namespace, so the key itself
	// identifies a record without deriving anything from the digest.
	private final Map<Key, OrderKey> byKey;

	/** Worst-first heap: head is evicted first when the heap exceeds {@code limit}. */
	private final PriorityQueue<OrderKey> heap;

	/**
	 * Create an empty combiner that ranks by {@code keys}, most significant first.
	 */
	TopKReduceSpec(OrderBy[] keys, int limit) {
		this.keys = keys;
		this.limit = limit;
		this.byKey = new HashMap<>((limit + 1) * 4 / 3 + 1);
		this.heap = new PriorityQueue<>(limit + 1, TopKReduceSpec::compareWorstFirst);
	}

	/**
	 * Merge one record from a node into the ranking. Called concurrently by the node threads.
	 *
	 * @param record			record as received
	 * @param key				record key; used for digest-based deduplication and tie-breaking
	 * @param lastUpdateTime	record's last-update time as sent by the server, or zero when
	 *							the server did not send one; decides which of two copies of a
	 *							record is kept
	 */
	void acceptPartial(Record record, Key key, long lastUpdateTime) {
		// Read the order-key values before taking the lock, so node threads contend only for
		// the heap update.
		OrderKey candidate = new OrderKey(record, keys, key, lastUpdateTime);

		synchronized (this) {
			// Dedup: at most one row per digest (partition migration may re-scan a record).
			OrderKey existing = byKey.get(key);

			if (existing != null) {
				if (! replaces(candidate, existing)) {
					return;
				}
				heap.remove(existing);
			}
			else if (heap.size() == limit && candidate.compareTo(heap.peek()) >= 0) {
				return; // would be evicted at once.
			}

			heap.offer(candidate);
			byKey.put(key, candidate);

			if (heap.size() > limit) {
				byKey.remove(heap.poll().key);
			}
		}
	}

	/**
	 * Return the kept key/record pairs, best first. Used by the query executor to fill the
	 * {@link RecordSet}.
	 */
	synchronized KeyRecord[] getResultKeyRecords() {
		OrderKey[] sorted = heap.toArray(new OrderKey[0]);
		Arrays.sort(sorted);

		KeyRecord[] out = new KeyRecord[sorted.length];

		for (int i = 0; i < sorted.length; i++) {
			out[i] = new KeyRecord(sorted[i].key, sorted[i].record);
		}
		return out;
	}

	/**
	 * Return whether {@code candidate} should replace {@code existing}, another copy of the same
	 * record: the later last-update time wins, even when it ranks worse. Equal times, including
	 * none sent by the server, fall back to the better rank so the result does not depend on
	 * which node answers first.
	 */
	private static boolean replaces(OrderKey candidate, OrderKey existing) {
		if (candidate.lastUpdateTime != existing.lastUpdateTime) {
			return candidate.lastUpdateTime > existing.lastUpdateTime;
		}
		return candidate.compareTo(existing) < 0;
	}

	/** Evict the candidate with the worst sort key first (heap head = worst). */
	private static int compareWorstFirst(OrderKey a, OrderKey b) {
		return b.compareTo(a);
	}
}

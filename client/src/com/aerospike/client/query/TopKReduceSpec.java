/*
 * Copyright 2012-2025 Aerospike, Inc.
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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Ordered LIMIT k reduce combiner backing {@link Reduce#topK(String, BinDataType, Order, int)}
 * and the composed split form {@code setReduce(Reduce.orderBy(...), Reduce.limit(...))}.
 * <p>
 * Maintains a size-k heap keyed by the order keys, deduplicated by record digest (a record may
 * reach the client from two nodes while its partition migrates, keeping the copy with the later
 * last-update time) with ties broken by digest ascending for stable, deterministic results. Not
 * part of the public API surface beyond the {@link Reduce#topK} factory method.
 */
final class TopKReduceSpec implements ReduceSpec<Record, Record> {
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
	 * Create an empty combiner with the same order keys and limit.
	 */
	TopKReduceSpec emptyCopy() {
		return new TopKReduceSpec(keys, limit);
	}

	/**
	 * Compose an {@link OrderByReduceSpec} and a {@link LimitReduceSpec} on the same bin into a
	 * single {@link TopKReduceSpec} combiner. Used by {@link Reduce#topK} and by
	 * {@link Statement#resolveReduce()} when the two specs are set separately.
	 *
	 * @throws AerospikeException.InvalidArgument	if the specs are not exactly one orderBy + one
	 *								limit, or their bins do not match
	 */
	static TopKReduceSpec compose(ReduceSpec<Record, Record> orderBy, ReduceSpec<Record, Record> limit) {
		if (!(orderBy instanceof OrderByReduceSpec) || !(limit instanceof LimitReduceSpec)) {
			throw new AerospikeException.InvalidArgument("topK requires one orderBy spec and one limit spec");
		}
		OrderByReduceSpec ob = (OrderByReduceSpec)orderBy;
		LimitReduceSpec lim = (LimitReduceSpec)limit;

		if (!ob.key.binName.equals(lim.bin)) {
			throw new AerospikeException.InvalidArgument(
				"orderBy bin (" + ob.key.binName + ") must match limit bin (" + lim.bin + ")");
		}
		return new TopKReduceSpec(new OrderBy[] {ob.key}, lim.limit);
	}

	@Override
	public synchronized void acceptPartial(Record record, Key key, long lastUpdateTime) {
		OrderKey candidate = new OrderKey(record, keys, key, lastUpdateTime);

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

	@Override
	public synchronized Record getScalarResult() {
		Record[] all = getResult();

		if (all.length == 0) {
			throw new IllegalStateException(
				"getScalarResult() called before any records were accepted via acceptPartial()");
		}
		return all[0];
	}

	@Override
	public synchronized Record[] getResult() {
		OrderKey[] sorted = sorted();
		Record[] out = new Record[sorted.length];

		for (int i = 0; i < sorted.length; i++) {
			out[i] = sorted[i].record;
		}
		return out;
	}

	/**
	 * Return the key/record pairs of {@link #getResult()}, in the same order. Used by the query
	 * executor to fill the {@link RecordSet}.
	 */
	synchronized KeyRecord[] getResultKeyRecords() {
		OrderKey[] sorted = sorted();
		KeyRecord[] out = new KeyRecord[sorted.length];

		for (int i = 0; i < sorted.length; i++) {
			out[i] = new KeyRecord(sorted[i].key, sorted[i].record);
		}
		return out;
	}

	/** Return the kept candidates, best first. */
	private OrderKey[] sorted() {
		OrderKey[] sorted = heap.toArray(new OrderKey[0]);
		Arrays.sort(sorted);
		return sorted;
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

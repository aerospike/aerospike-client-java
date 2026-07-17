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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.Buffer;

/**
 * Ordered LIMIT k reduce combiner backing {@link Reduce#topK(String, BinDataType, Order, OrderByFlags, int)}
 * and the composed split form {@code setReduce(Reduce.orderBy(...), Reduce.limit(...))}.
 * <p>
 * Maintains a size-k heap keyed by the order bin, deduplicated by record digest (a record may
 * be re-scanned across partition migrations) with ties broken by digest ascending for stable,
 * deterministic results.
 */
final class TopKReduceSpec implements ReduceSpec<Record, Record> {
	private final String bin;
	private final BinDataType type;
	private final Order order;
	private final OrderByFlags flags;
	private final int limit;

	/** digest (hex) -> (key, record), for dedup and result reconstruction. */
	private final Map<String, Entry> byDigest = new HashMap<>();

	/** Worst-first heap: head is evicted first when the heap exceeds {@code limit}. */
	private final PriorityQueue<OrderKey> heap;

	private static final class Entry {
		final Key key;
		final Record record;
		final OrderKey orderKey;

		Entry(Key key, Record record, OrderKey orderKey) {
			this.key = key;
			this.record = record;
			this.orderKey = orderKey;
		}
	}

	private TopKReduceSpec(String bin, BinDataType type, Order order, OrderByFlags flags, int limit) {
		this.bin = bin;
		this.type = type;
		this.order = order;
		this.flags = flags;
		this.limit = limit;
		this.heap = new PriorityQueue<>(limit + 1, TopKReduceSpec::compareWorstFirst);
	}

	/**
	 * Compose an {@link OrderByReduceSpec} and a {@link LimitReduceSpec} on the same bin into a
	 * single {@link TopKReduceSpec} combiner. Used by {@link Reduce#topK} and by
	 * {@link Statement#resolveReduce()} when the two specs are set separately.
	 *
	 * @throws IllegalArgumentException if the specs are not exactly one orderBy + one limit, or
	 *                                   their bins do not match
	 */
	static TopKReduceSpec compose(ReduceSpec<Record, Record> orderBy, ReduceSpec<Record, Record> limit) {
		if (!(orderBy instanceof OrderByReduceSpec) || !(limit instanceof LimitReduceSpec)) {
			throw new IllegalArgumentException("topK requires one orderBy spec and one limit spec");
		}
		OrderByReduceSpec ob = (OrderByReduceSpec)orderBy;
		LimitReduceSpec lim = (LimitReduceSpec)limit;

		if (!ob.bin.equals(lim.bin)) {
			throw new IllegalArgumentException(
				"orderBy bin (" + ob.bin + ") must match limit bin (" + lim.bin + ")");
		}
		return new TopKReduceSpec(ob.bin, ob.type, ob.order, ob.flags, lim.limit);
	}

	@Override
	public void acceptPartial(Record record, Key key) {
		byte[] digest = key.digest;
		String digestKey = digestKey(digest);
		OrderKey candidate = new OrderKey(record, bin, type, order, flags, digest);

		// Dedup: at most one row per digest (partition migration may re-scan a record).
		Entry existingEntry = byDigest.get(digestKey);

		if (existingEntry != null) {
			if (candidate.compareTo(existingEntry.orderKey) >= 0) {
				return; // existing entry is at least as good.
			}
			heap.remove(existingEntry.orderKey);
			byDigest.remove(digestKey);
		}

		heap.offer(candidate);
		byDigest.put(digestKey, new Entry(key, record, candidate));

		if (heap.size() > limit) {
			OrderKey evicted = heap.poll();
			byDigest.remove(digestKey(evicted.digest));
		}
	}

	@Override
	public Record getScalarResult() {
		Record[] all = getResult();

		if (all.length == 0) {
			throw new IllegalStateException("No partials accepted");
		}
		return all[0];
	}

	@Override
	public Record[] getResult() {
		List<OrderKey> list = new ArrayList<>(heap);
		list.sort(Comparator.naturalOrder()); // best first.

		Record[] out = new Record[list.size()];

		for (int i = 0; i < list.size(); i++) {
			out[i] = byDigest.get(digestKey(list.get(i).digest)).record;
		}
		return out;
	}

	/**
	 * Return the record keys corresponding to {@link #getResult()}, in the same order.
	 * Used by the query executor to reconstruct key/record pairs for {@link RecordSet}.
	 */
	Key[] getResultKeys() {
		List<OrderKey> list = new ArrayList<>(heap);
		list.sort(Comparator.naturalOrder()); // best first.

		Key[] out = new Key[list.size()];

		for (int i = 0; i < list.size(); i++) {
			out[i] = byDigest.get(digestKey(list.get(i).digest)).key;
		}
		return out;
	}

	/** Evict the candidate with the worst sort key first (heap head = worst). */
	private static int compareWorstFirst(OrderKey a, OrderKey b) {
		return b.compareTo(a);
	}

	private static String digestKey(byte[] digest) {
		return Buffer.bytesToHexString(digest);
	}
}

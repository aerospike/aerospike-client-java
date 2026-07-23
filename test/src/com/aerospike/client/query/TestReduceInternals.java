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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * White-box unit tests for package-private reduce internals in {@code com.aerospike.client.query}
 * that are not reachable from the public {@link Reduce} / {@link ReduceSpec} API. Pure client-side
 * (no cluster required); registered in {@code SuiteUnit}.
 */
public class TestReduceInternals {
	private static final String NS = "test";
	private static final String SET = "reduceset";
	private static final String BIN = "val";

	private static Key key(String userKey) {
		return new Key(NS, SET, userKey);
	}

	private static Record record(long value) {
		Map<String, Object> bins = new HashMap<>();
		bins.put(BIN, value);
		return new Record(bins, 1, 0);
	}

	@Test
	public void minMaxCollectsTiedRecords() {
		MinMaxReduceSpec spec = new MinMaxReduceSpec(BIN, BinDataType.INTEGER, Order.ASC);

		spec.acceptPartial(record(5L), key("k1"));
		spec.acceptPartial(record(5L), key("k2")); // tie at current best
		spec.acceptPartial(record(9L), key("k3")); // worse, ignored for ties
		spec.acceptPartial(record(5L), key("k4")); // tie at current best

		assertEquals(5L, spec.getScalarResult().longValue());
		assertEquals(3, spec.getTiedRecords().length);
	}

	@Test
	public void minMaxNewBestClearsPreviousTies() {
		MinMaxReduceSpec spec = new MinMaxReduceSpec(BIN, BinDataType.INTEGER, Order.ASC);

		spec.acceptPartial(record(5L), key("k1"));
		spec.acceptPartial(record(5L), key("k2"));
		spec.acceptPartial(record(2L), key("k3")); // new minimum, clears prior ties

		assertEquals(2L, spec.getScalarResult().longValue());
		assertEquals(1, spec.getTiedRecords().length);
	}

	@Test
	public void minMaxTiesDedupedByDigest() {
		MinMaxReduceSpec spec = new MinMaxReduceSpec(BIN, BinDataType.INTEGER, Order.DESC);

		Key k1 = key("k1");
		spec.acceptPartial(record(7L), k1);
		spec.acceptPartial(record(7L), k1); // same digest re-scan, must not double-count

		assertEquals(7L, spec.getScalarResult().longValue());
		assertEquals(1, spec.getTiedRecords().length);
	}
}

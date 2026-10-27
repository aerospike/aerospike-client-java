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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Candidate held by {@link TopKReduceSpec}: the record and its key, ranked by one value per
 * order key, then the digest.
 * <p>
 * Missing, mismatched, and collection values are NIL and sort last for their order key, so a
 * later order key still ranks records that are NIL on an earlier one.
 */
final class OrderKey implements Comparable<OrderKey> {
	// A query has at most two order keys; fields instead of an array avoid an allocation per
	// candidate.
	final Object first;
	final Object second;
	final OrderBy[] keys;
	final Key key;
	final Record record;
	final long lastUpdateTime;

	OrderKey(Record record, OrderBy[] keys, Key key, long lastUpdateTime) {
		this.first = readValue(record, keys[0].binName, keys[0].type);
		this.second = keys.length > 1 ? readValue(record, keys[1].binName, keys[1].type) : null;
		this.keys = keys;
		this.key = key;
		this.record = record;
		this.lastUpdateTime = lastUpdateTime;
	}

	@Override
	public int compareTo(OrderKey other) {
		int cmp = compareKey(keys[0], first, other.first);

		if (cmp == 0 && keys.length > 1) {
			cmp = compareKey(keys[1], second, other.second);
		}

		if (cmp != 0) {
			return cmp;
		}
		// Tie-break: digest ascending (stable, deterministic ordering).
		return compareDigest(key.digest, other.key.digest);
	}

	private static int compareKey(OrderBy key, Object a, Object b) {
		if (a == null || b == null) {
			if (a == null && b == null) {
				return 0;
			}
			// NIL sorts last in either direction.
			return a == null ? 1 : -1;
		}

		int cmp = compareValues(a, b, key.type, key.flags);
		return key.order == Order.ASC ? cmp : -cmp;
	}

	/**
	 * Extract the scalar order-by value, or {@code null} for NIL.
	 */
	static Object readValue(Record record, String bin, BinDataType type) {
		Object value = record.getValue(bin);

		if (value == null || value instanceof List || value instanceof Map) {
			return null;
		}

		switch (type) {
			case INTEGER:
				return (value instanceof Long) ? value : null;
			case DOUBLE:
				return (value instanceof Double) ? value : null;
			case STRING:
				return (value instanceof String) ? value : null;
			case BYTES:
				return (value instanceof byte[]) ? value : null;
			default:
				throw new IllegalArgumentException("Unsupported BinDataType: " + type);
		}
	}

	@SuppressWarnings("unchecked")
	private static int compareValues(Object a, Object b, BinDataType type, OrderByFlags flags) {
		if (type == BinDataType.STRING) {
			return compareBytes(
				((String)a).getBytes(StandardCharsets.UTF_8),
				((String)b).getBytes(StandardCharsets.UTF_8),
				flags == OrderByFlags.CASE_INSENSITIVE);
		}

		if (type == BinDataType.BYTES) {
			return compareBytes((byte[])a, (byte[])b);
		}

		if (type == BinDataType.DOUBLE) {
			double x = (Double)a;
			double y = (Double)b;
			// The server compares with < and >, so -0.0 equals 0.0 and the digest decides.
			return x == y ? 0 : Double.compare(x, y);
		}
		return ((Comparable<Object>)a).compareTo(b);
	}

	private static int compareBytes(byte[] a, byte[] b) {
		return compareBytes(a, b, false);
	}

	private static int compareBytes(byte[] a, byte[] b, boolean foldAscii) {
		int len = Math.min(a.length, b.length);

		for (int i = 0; i < len; i++) {
			int ai = a[i] & 0xff;
			int bi = b[i] & 0xff;

			if (foldAscii) {
				if (ai >= 'A' && ai <= 'Z') {
					ai += 'a' - 'A';
				}

				if (bi >= 'A' && bi <= 'Z') {
					bi += 'a' - 'A';
				}
			}

			int cmp = ai - bi;

			if (cmp != 0) {
				return cmp;
			}
		}
		return a.length - b.length;
	}

	private static int compareDigest(byte[] a, byte[] b) {
		int len = Math.min(a.length, b.length);

		for (int i = 0; i < len; i++) {
			int cmp = (a[i] & 0xff) - (b[i] & 0xff);

			if (cmp != 0) {
				return cmp;
			}
		}
		return a.length - b.length;
	}
}

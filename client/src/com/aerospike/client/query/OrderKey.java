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

import java.util.List;
import java.util.Map;

import com.aerospike.client.Record;

/**
 * Sort key used internally by {@link TopKReduceSpec}.
 * <p>
 * Missing, mismatched, and collection values are NIL and sort last.
 */
final class OrderKey implements Comparable<OrderKey> {
	final Object value;
	final BinDataType type;
	final Order order;
	final OrderByFlags flags;
	final byte[] digest;

	OrderKey(Record record, String bin, BinDataType type, Order order, OrderByFlags flags, byte[] digest) {
		this.value = readValue(record, bin, type);
		this.type = type;
		this.order = order;
		this.flags = flags;
		this.digest = digest;
	}

	boolean isNil() {
		return value == null;
	}

	@Override
	public int compareTo(OrderKey other) {
		boolean nilA = isNil();
		boolean nilB = other.isNil();

		if (nilA || nilB) {
			if (nilA && nilB) {
				// Break NIL ties by digest.
				return compareDigest(digest, other.digest);
			}
			// NIL sorts last in either direction.
			return nilA ? 1 : -1;
		}

		int cmp = compareValues(value, other.value, type, flags);

		if (cmp != 0) {
			return order == Order.ASC ? cmp : -cmp;
		}
		// Tie-break: digest ascending (stable, deterministic ordering).
		return compareDigest(digest, other.digest);
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
		if (type == BinDataType.STRING && flags == OrderByFlags.CASE_INSENSITIVE) {
			return ((String)a).compareToIgnoreCase((String)b);
		}

		if (type == BinDataType.BYTES) {
			return compareBytes((byte[])a, (byte[])b);
		}
		return ((Comparable<Object>)a).compareTo(b);
	}

	private static int compareBytes(byte[] a, byte[] b) {
		int len = Math.min(a.length, b.length);

		for (int i = 0; i < len; i++) {
			int cmp = (a[i] & 0xff) - (b[i] & 0xff);

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

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

import java.util.LinkedHashMap;
import java.util.Map;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.Buffer;

/**
 * Scalar MIN/MAX reduce combiner. Backs both {@link Reduce#min(String, BinDataType)} and
 * {@link Reduce#max(String, BinDataType)} — direction is selected via {@link Order}
 * ({@code ASC} picks the minimum, {@code DESC} picks the maximum).
 * <p>
 * Merge is commutative: the order in which {@link #acceptPartial(Record, Key)} is called
 * across nodes does not affect the result. Not part of the public API surface beyond the
 * {@link Reduce#min} / {@link Reduce#max} factory methods.
 */
final class MinMaxReduceSpec implements ReduceSpec<Record, Number> {
	private final String bin;
	private final BinDataType type;
	private final Order order;

	private Number best;
	private final Map<String, Record> tiesByDigest = new LinkedHashMap<>();

	MinMaxReduceSpec(String bin, BinDataType type, Order order) {
		if (type != BinDataType.INTEGER && type != BinDataType.DOUBLE) {
			throw new IllegalArgumentException("min/max requires BinDataType.INTEGER or BinDataType.DOUBLE, got: " + type);
		}
		this.bin = bin;
		this.type = type;
		this.order = order;
	}

	@Override
	public void acceptPartial(Record record, Key key) {
		Number value = readNumber(record);

		if (value == null) {
			return;
		}

		int cmp = (best == null) ? 1 : compareNumber(value, best);
		boolean better = (order == Order.ASC) ? cmp < 0 : cmp > 0;

		if (best == null || better) {
			best = value;
			tiesByDigest.clear();
			tiesByDigest.put(digestKey(key), record);
		}
		else if (cmp == 0) {
			tiesByDigest.putIfAbsent(digestKey(key), record);
		}
	}

	@Override
	public Number getScalarResult() {
		if (best == null) {
			throw new IllegalStateException(
				"getScalarResult() called before any records were accepted via acceptPartial()");
		}
		return best;
	}

	@Override
	public Number[] getResult() {
		return new Number[] { getScalarResult() };
	}

	/**
	 * Records tied at the current MIN/MAX value, in the order they were first seen.
	 */
	Record[] getTiedRecords() {
		return tiesByDigest.values().toArray(new Record[0]);
	}

	private Number readNumber(Record record) {
		return type == BinDataType.DOUBLE ? record.getDouble(bin) : record.getLong(bin);
	}

	private static int compareNumber(Number a, Number b) {
		return Double.compare(a.doubleValue(), b.doubleValue());
	}

	static String digestKey(Key key) {
		return Buffer.bytesToHexString(key.digest);
	}
}

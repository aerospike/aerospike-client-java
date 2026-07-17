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

import com.aerospike.client.Record;

/**
 * Factory for {@link ReduceSpec} instances used with {@link Statement#setReduce(ReduceSpec...)}.
 * <p>
 * Two reduce shapes are supported:
 * <ul>
 *   <li><b>Ordered LIMIT k</b> ({@link #topK}, or {@link #orderBy} + {@link #limit} passed
 *       together) — returns up to k full records, globally ordered.</li>
 *   <li><b>Scalar</b> ({@link #sum}, {@link #count}, {@link #min}, {@link #max}) — returns a
 *       single commutative aggregate value.</li>
 * </ul>
 */
public final class Reduce {

	private Reduce() {
	}

	/**
	 * Ordered LIMIT k reduce; returns up to {@code k} full records in global sort order
	 * (e.g. vector search Top-K, {@code ORDER BY bin LIMIT k}).
	 *
	 * @param bin	bin name to order by; must be a physical bin or a projected bin from
	 *              {@link Statement#setOperations(com.aerospike.client.Operation[])}
	 * @param type	scalar type of {@code bin}
	 * @param order	sort direction
	 * @param flags	comparison options ({@link OrderByFlags#CASE_INSENSITIVE} for
	 *              {@link BinDataType#STRING} only)
	 * @param k		maximum number of records to return, in {@code [1, 1000]}
	 */
	public static ReduceSpec<Record, Record> topK(String bin, BinDataType type, Order order, OrderByFlags flags, int k) {
		return TopKReduceSpec.compose(orderBy(bin, type, order, flags), limit(bin, k));
	}

	/**
	 * Sort-order building block for {@code topK}. Must be paired with {@link #limit(String, int)}
	 * on the same bin via {@link Statement#setReduce(ReduceSpec...)}; using it alone throws
	 * {@link UnsupportedOperationException}.
	 */
	public static ReduceSpec<Record, Record> orderBy(String bin, BinDataType type, Order order, OrderByFlags flags) {
		return new OrderByReduceSpec(bin, type, order, flags);
	}

	/**
	 * Cap building block for {@code topK}. Must be paired with
	 * {@link #orderBy(String, BinDataType, Order, OrderByFlags)} on the same bin via
	 * {@link Statement#setReduce(ReduceSpec...)}; using it alone throws
	 * {@link UnsupportedOperationException}.
	 *
	 * @param bin	bin name; must match the paired {@code orderBy} bin
	 * @param limit	maximum number of records to return, in {@code [1, 1000]}
	 */
	public static ReduceSpec<Record, Record> limit(String bin, int limit) {
		return new LimitReduceSpec(bin, limit);
	}

	/**
	 * Sum of {@code bin} across all matching records. {@code bin} must be an integer bin
	 * (or a projected bin from {@link Statement#setOperations(com.aerospike.client.Operation[])}
	 * that evaluates to an integer).
	 */
	public static ReduceSpec<Record, Long> sum(String bin) {
		return new SumReduceSpec(bin);
	}

	/**
	 * Count of records matching the query filter and record predicate.
	 */
	public static ReduceSpec<Record, Long> count() {
		return new CountReduceSpec();
	}

	/**
	 * Minimum value of {@code bin} across all matching records.
	 * {@link ReduceSpec#getScalarResult()} returns the minimum value.
	 *
	 * @param bin	bin name; must be {@link BinDataType#INTEGER} or {@link BinDataType#DOUBLE}
	 * @param type	scalar type of {@code bin}
	 */
	public static ReduceSpec<Record, Number> min(String bin, BinDataType type) {
		return new MinMaxReduceSpec(bin, type, Order.ASC);
	}

	/**
	 * Maximum value of {@code bin} across all matching records.
	 *
	 * @param bin	bin name; must be {@link BinDataType#INTEGER} or {@link BinDataType#DOUBLE}
	 * @param type	scalar type of {@code bin}
	 */
	public static ReduceSpec<Record, Number> max(String bin, BinDataType type) {
		return new MinMaxReduceSpec(bin, type, Order.DESC);
	}
}

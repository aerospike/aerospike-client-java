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

import com.aerospike.client.Key;

/**
 * Client-side global reduce combiner for query results.
 * <p>
 * Each cluster node produces a partial result for a query (e.g. a locally bounded Top-K
 * heap, or a partial scalar aggregate). The query executor feeds every node's partial
 * results into {@link #acceptPartial(Object, Key)} in any order (nodes complete
 * concurrently), then reads the merged result via {@link #getScalarResult()} or
 * {@link #getResult()}.
 * <p>
 * Instances are created via {@link Reduce} factory methods and passed to
 * {@link Statement#setReduce(ReduceSpec...)}.
 *
 * @param <TInput>  type of each partial accepted by {@link #acceptPartial(Object, Key)}
 *                  (usually {@link com.aerospike.client.Record})
 * @param <TResult> element type returned by {@link #getResult()}
 */
public interface ReduceSpec<TInput, TResult> {

	/**
	 * Merge one partial result from a node into this combiner.
	 *
	 * @param record	partial result (usually a {@link com.aerospike.client.Record})
	 * @param key		record key; used for digest-based deduplication and tie-breaking
	 */
	void acceptPartial(TInput record, Key key);

	/**
	 * Return a single scalar view of the merged result (e.g. MAX value, SUM, or the
	 * best-ranked record for a Top-K reduce).
	 */
	TResult getScalarResult();

	/**
	 * Return the full merged result set (e.g. all Top-K records in order, or all records
	 * tied at a MIN/MAX value).
	 */
	TResult[] getResult();
}

/*
 * Copyright 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.metrics;

import java.util.Arrays;

/**
 * Immutable latency histogram snapshot.
 */
public final class LatencySnapshot {
	/**
	 * Operation category measured by this histogram.
	 */
	public final LatencyType type;
	private final long[] bucketCounts;

	/**
	 * Observed minimum, or null while collection does not provide this summary.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final Long min;

	/**
	 * Observed maximum, or null while collection does not provide this summary.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final Long max;

	/**
	 * Sum of observed values, or null while collection does not provide this summary.
	 */
	// TODO: Populate when the corresponding client collection support is implemented.
	public final Double sum;

	/**
	 * Cumulative number of observations represented by the histogram buckets.
	 */
	public final long count;

	/**
	 * Initialize an immutable latency histogram snapshot.
	 */
	public LatencySnapshot(
		LatencyType type,
		long[] bucketCounts,
		Long min,
		Long max,
		Double sum,
		long count
	) {
		this.type = type;
		this.bucketCounts = bucketCounts == null ? new long[0] : bucketCounts.clone();
		this.min = min;
		this.max = max;
		this.sum = sum;
		this.count = count;
	}

	public long[] getBucketCounts() {
		return bucketCounts.clone();
	}

	@Override
	public String toString() {
		return "LatencySnapshot{"
			+ "type=" + type
			+ ", bucketCounts=" + Arrays.toString(bucketCounts)
			+ ", min=" + min
			+ ", max=" + max
			+ ", sum=" + sum
			+ ", count=" + count
			+ '}';
	}
}

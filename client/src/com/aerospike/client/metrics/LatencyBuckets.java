/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.client.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Latency buckets for a command group (See {@link com.aerospike.client.metrics.LatencyType}).
 * Latency bucket counts are cumulative and not reset on each metrics snapshot interval.
 */
public final class LatencyBuckets {
	private static final long NS_TO_MS = 1000000;

	private final AtomicLong[] buckets;
	private final int latencyShift;

	/**
	 * Initialize latency buckets.
	 *
	 * @param latencyColumns	number of latency buckets
	 * @param latencyShift		power of 2 multiple between each range bucket in latency histograms starting at bucket 3.
	 * 							The first 2 buckets are "&lt;=1ms" and "&gt;1ms".
	 */
	public LatencyBuckets(int latencyColumns, int latencyShift) {
		this.latencyShift = latencyShift;
		buckets = new AtomicLong[latencyColumns];

		for (int i = 0; i < buckets.length; i++) {
			buckets[i] = new AtomicLong();
		}
	}

	/**
	 * Return number of buckets.
	 */
	public int getMax() {
		return buckets.length;
	}

	/**
	 * Return cumulative count of a bucket.
	 */
	public long getBucket(int i) {
		return buckets[i].get();
	}

	/**
	 * Increment count of bucket corresponding to the elapsed time in nanoseconds.
	 */
	public void add(long elapsed) {
		int index = getIndex(elapsed);
		buckets[index].getAndIncrement();
	}

	private int getIndex(long elapsedNanos) {
		// Convert nanoseconds to milliseconds.
		long elapsed = elapsedNanos / NS_TO_MS;

		// Round up elapsed to nearest millisecond.
		if ((elapsedNanos - (elapsed * NS_TO_MS)) > 0) {
			elapsed++;
		}

		int lastBucket = buckets.length - 1;
		long limit = 1;

		for (int i = 0; i < lastBucket; i++) {
			if (elapsed <= limit) {
				return i;
			}
			limit <<= latencyShift;
		}
		return lastBucket;
	}
}

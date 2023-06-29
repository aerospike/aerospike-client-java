/*
 * Copyright 2012-2023 Aerospike, Inc.
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

import com.aerospike.client.cluster.LatencyType;

/**
 * Optional extended node metrics. Used when extended metrics is enabled
 * (See {@link com.aerospike.client.AerospikeClient#enableMetrics(MetricsPolicy)}).
 */
public final class NodeMetrics {
	private final LatencyBuckets[] latency;
	private final AtomicLong errors = new AtomicLong();
	private final AtomicLong timeouts = new AtomicLong();
	private final AtomicLong retries = new AtomicLong();

	/**
	 * Initialize extended node metrics.
	 */
	public NodeMetrics(MetricsPolicy policy) {
		int latencyColumns = policy.latencyColumns;
		int latencyShift = policy.latencyShift;
		int max = LatencyType.getMax();

		latency = new LatencyBuckets[max];

		for (int i = 0; i < max; i++) {
			latency[i] = new LatencyBuckets(latencyColumns, latencyShift);
		}
	}

	/**
	 * Get latency buckets given type and increment count of the bucket corresponding to the
	 * elapsed time in milliseconds.
	 */
	public void addLatency(LatencyType type, long elapsed) {
		latency[type.ordinal()].add(elapsed);
	}

	/**
	 * Return latency buckets given type.
	 */
	public LatencyBuckets getLatencyBuckets(int type) {
		return latency[type];
	}

	/**
	 * Increment transaction error count. If the error is retryable, multiple errors per
	 * transaction may occur.
	 */
	public void addError() {
		errors.getAndIncrement();
	}

	/**
	 * Increment transaction timeout count. If the timeout is retryable (ie socketTimeout),
	 * multiple timeouts per transaction may occur.
	 */
	public void addTimeout() {
		timeouts.getAndIncrement();
	}

	/**
	 * Increment transaction retry count.
	 */
	public void addRetry() {
		retries.getAndIncrement();
	}

	/**
	 * Return transaction error count. The value is cumulative and not reset per metrics interval.
	 */
	public long getErrors() {
		return errors.get();
	}

	/**
	 * Return transaction timeout count. The value is cumulative and not reset per metrics interval.
	 */
	public long getTimeouts() {
		return timeouts.get();
	}

	/**
	 * Return transaction retry count. The value is cumulative and not reset per metrics interval.
	 */
	public long getRetries() {
		return retries.get();
	}
}

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
package com.aerospike.client.cluster;

import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.policy.StatsPolicy;

public final class NodeMetrics {
	private final LatencyBuckets[] latency;
	private final AtomicInteger errors = new AtomicInteger();
	private final AtomicInteger timeouts = new AtomicInteger();

	public NodeMetrics(StatsPolicy policy) {
		int latencyColumns = policy.latencyColumns;
		int latencyShift = policy.latencyShift;
		int max = LatencyType.getMax();

		latency = new LatencyBuckets[max];

		for (int i = 0; i < max; i++) {
			latency[i] = new LatencyBuckets(latencyColumns, latencyShift);
		}
	}

	public void addLatency(LatencyType type, long elapsed) {
		latency[type.ordinal()].add(elapsed);
	}

	public LatencyBuckets get(int type) {
		return latency[type];
	}

	public void addError() {
		errors.getAndIncrement();
	}

	public void addTimeout() {
		timeouts.getAndIncrement();
	}

	public int resetError() {
		return errors.getAndSet(0);
	}

	public int resetTimeout() {
		return timeouts.getAndSet(0);
	}
}

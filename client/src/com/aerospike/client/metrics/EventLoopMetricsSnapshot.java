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

/**
 * Immutable Java event-loop measurements.
 */
public final class EventLoopMetricsSnapshot {
	/**
	 * Current number of commands being processed by this Java event loop.
	 */
	public final int processSize;

	/**
	 * Current number of commands waiting in this Java event-loop queue.
	 */
	public final int queueSize;

	/**
	 * Initialize an immutable Java event-loop snapshot.
	 */
	public EventLoopMetricsSnapshot(int processSize, int queueSize) {
		this.processSize = processSize;
		this.queueSize = queueSize;
	}

	@Override
	public String toString() {
		return "EventLoopMetricsSnapshot{"
			+ "processSize=" + processSize
			+ ", queueSize=" + queueSize
			+ '}';
	}
}

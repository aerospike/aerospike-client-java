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
 * Immutable Java connection-pool snapshot.
 */
public final class ConnectionMetricsSnapshot {
	/**
	 * Active connections currently executing commands.
	 */
	public final int inUse;

	/**
	 * Initialized pooled connections that are currently idle.
	 */
	public final int inPool;

	/**
	 * Cumulative connections opened since node creation.
	 */
	public final int opened;

	/**
	 * Cumulative connections closed since node creation.
	 */
	public final int closed;

	/**
	 * Initialize an immutable connection-pool snapshot.
	 */
	public ConnectionMetricsSnapshot(int inUse, int inPool, int opened, int closed) {
		this.inUse = inUse;
		this.inPool = inPool;
		this.opened = opened;
		this.closed = closed;
	}

	@Override
	public String toString() {
		return "ConnectionMetricsSnapshot{"
			+ "inUse=" + inUse
			+ ", inPool=" + inPool
			+ ", opened=" + opened
			+ ", closed=" + closed
			+ '}';
	}
}

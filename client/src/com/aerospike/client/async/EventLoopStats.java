/*
 * Copyright 2012-2021 Aerospike, Inc.
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
package com.aerospike.client.async;

/**
 * Snapshot of a single event loop's load (process and queue sizes).
 * <p>
 * Used in {@link com.aerospike.client.cluster.ClusterStats#eventLoops}; obtain via {@link com.aerospike.client.AerospikeClient#getClusterStats()}.
 *
 * @see com.aerospike.client.cluster.ClusterStats
 * @see EventLoop#getProcessSize
 * @see EventLoop#getQueueSize
 */
public final class EventLoopStats {
	/** Approximate number of commands actively being processed on the event loop. */
	public final int processSize;

	/** Approximate number of commands on the event loop's delay queue not yet started. */
	public final int queueSize;

	/**
	 * Builds stats from the given event loop (values are snapshots at construction time).
	 * @param eventLoop event loop (must not be null)
	 */
	public EventLoopStats(EventLoop eventLoop) {
		this.processSize = eventLoop.getProcessSize();
		this.queueSize = eventLoop.getQueueSize();
	}

	/** @return processSize and queueSize as a comma-separated string */
	@Override
	public String toString() {
		return "" + processSize + ',' + queueSize;
	}
}

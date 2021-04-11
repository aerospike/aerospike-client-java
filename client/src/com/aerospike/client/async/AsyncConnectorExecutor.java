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

import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.util.Util;

public final class AsyncConnectorExecutor implements AsyncConnector.Listener {
	private final Monitor monitor;
	private final AtomicInteger eventLoopCount;
	private final int maxConnections;
	private final int maxConcurrent;
	private int countConnections;
	private boolean done;

	public AsyncConnectorExecutor
	(
		EventLoop eventLoop,
		Cluster cluster,
		Node node,
		int maxConnections,
		int maxConcurrent,
		Monitor monitor,
		AtomicInteger eventLoopCount
	) {
		this.monitor = monitor;
		this.eventLoopCount = eventLoopCount;
		this.maxConnections = maxConnections;
		this.maxConcurrent = (maxConnections >= maxConcurrent)? maxConcurrent : maxConnections;

		try {
			for (int i = 0; i < this.maxConcurrent; i++) {
				AsyncConnector ac = eventLoop.createConnector(cluster, node, this);

				if (! ac.execute()) {
					complete();
					break;
				}
			}
		}
		catch (Exception e) {
			complete();
			throw e;
		}
	}

	public void onSuccess(AsyncConnector ac) {
		countConnections++;

		if (countConnections < maxConnections) {
			int next = countConnections + maxConcurrent - 1;

			// Determine if a new command needs to be started.
			if (next < maxConnections && ! done) {
				// Start new command.
				if (! ac.execute()) {
					complete();
				}
			}
		}
		else {
			complete();
		}
	}

	public void onFailure(AerospikeException e)
	{
		// Connection failed.  Highly unlikely other connections will succeed.
		// Abort the process.
		if (Log.warnEnabled()) {
			Log.warn("Async min connections failed: " + Util.getErrorMessage(e));
		}
		complete();
	}

	public void onFailure()
	{
		complete();
	}

	private void complete() {
		// Ensure executor succeeds or fails exactly once.
		if (done) {
			return;
		}
		done = true;

		if (monitor != null) {
			eventLoopComplete(monitor, eventLoopCount);
		}
	}

	public static void eventLoopComplete(Monitor monitor, AtomicInteger eventLoopCount) {
		// Check if all event loops are done.
		int count = eventLoopCount.decrementAndGet();

		if (count == 0) {
			monitor.notifyComplete();
		}
	}
}

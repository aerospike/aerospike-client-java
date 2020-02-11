/*
 * Copyright 2012-2020 Aerospike, Inc.
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

import com.aerospike.client.async.EventLoopStats;

/**
 * Cluster statistics.
 */
public final class ClusterStats {
	/**
	 * Statistics for each node.
	 */
	public final NodeStats[] nodes;

	/**
	 * Statistics for each event loop.
	 * This value will be null if event loops are not defined.
	 */
	public final EventLoopStats[] eventLoops;

	/**
	 * Number of active threads executing sync batch/scan/query commands.
	 */
	public final int threadsInUse;

	/**
	 * Number of connections residing in sync connection shutdown queue.
	 */
	public final int recoverQueueSize;

	/**
	 * Cluster statistics constructor.
	 */
	public ClusterStats(NodeStats[] nodes, EventLoopStats[] eventLoops, int threadsInUse, int recoverQueueSize) {
		this.nodes = nodes;
		this.eventLoops = eventLoops;
		this.threadsInUse = threadsInUse;
		this.recoverQueueSize = recoverQueueSize;
	}

	/**
	 * Convert statistics to string.
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder(1024);

		sb.append("nodes(inUse,inPool,opened,closed):");
		sb.append(System.lineSeparator());

		for (NodeStats stat : nodes) {
			sb.append(stat);
			sb.append(System.lineSeparator());
		}

		if (eventLoops != null) {
			sb.append("eventLoops(processSize,queueSize): ");

			for (int i = 0; i < eventLoops.length; i++) {
				EventLoopStats stat = eventLoops[i];

				if (i > 0) {
					sb.append(',');
				}
				sb.append('(');
				sb.append(stat);
				sb.append(')');
			}
			sb.append(System.lineSeparator());
		}

		sb.append("threadsInUse: " + threadsInUse);
		sb.append(System.lineSeparator());
		sb.append("recoverQueueSize: " + recoverQueueSize);
		sb.append(System.lineSeparator());
		return sb.toString();
	}
}

/*
 * Copyright 2012-2018 Aerospike, Inc.
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

/**
 * Cluster statistics.
 */
public final class ClusterStats {
	/**
	 * Statistics for each node.
	 */
	public final NodeStats[] nodes;
	
	/**
	 * Number of active threads executing sync batch/scan/query commands.
	 */
	public final int threadsInUse;

	/**
	 * Cluster statistics constructor.
	 */
	public ClusterStats(NodeStats[] nodes, int threadsInUse) {
		this.nodes = nodes;
		this.threadsInUse = threadsInUse;
	}
}

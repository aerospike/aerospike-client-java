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
package com.aerospike.client.listener;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.ClusterStats;

/**
 * Async callback for cluster statistics; receives {@link com.aerospike.client.cluster.ClusterStats} or exception.
 * <p>
 * Pass to {@link com.aerospike.client.IAerospikeClient#getClusterStats}.
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 *
 * client.getClusterStats(loop, new ClusterStatsListener() {
 *   public void onSuccess(ClusterStats stats) { }
 *   public void onFailure(AerospikeException e) { }
 * });
 * }</pre>
 *
 * @see com.aerospike.client.cluster.ClusterStats
 * @see com.aerospike.client.IAerospikeClient#getClusterStats
 */
public interface ClusterStatsListener {
	/**
	 * Called when the cluster stats request completes successfully.
	 * @param stats cluster statistics (must not be null)
	 */
	public void onSuccess(ClusterStats stats);

	/**
	 * Called when the cluster stats request fails.
	 * @param ae exception cause of failure wrapped into Aerospike exception
	 */
	public void onFailure(AerospikeException ae);
}

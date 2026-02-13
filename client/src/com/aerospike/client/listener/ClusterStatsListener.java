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
 * Asynchronous result notifications for cluster statistics.
 * <p>
 * No built-in implementations; implement this interface in application code and pass to
 * {@link com.aerospike.client.IAerospikeClient#getClusterStats(com.aerospike.client.async.EventLoop, ClusterStatsListener, com.aerospike.client.policy.InfoPolicy) getClusterStats(EventLoop, ClusterStatsListener, ...)}.
 * <p>Implement and pass to async getClusterStats to receive ClusterStats on success or exception on failure.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * client.getClusterStats(eventLoop, new ClusterStatsListener() {
 *   public void onSuccess(com.aerospike.client.cluster.ClusterStats stats) { // use stats
 *   }
 *   public void onFailure(AerospikeException ae) { ae.printStackTrace(); }
 * }, null);
 * }</pre>
 */
public interface ClusterStatsListener {
	/**
	 * This method is called when command completes successfully.
	 */
	public void onSuccess(ClusterStats stats);

	/**
	 * This method is called when command fails.
	 */
	public void onFailure(AerospikeException ae);
}

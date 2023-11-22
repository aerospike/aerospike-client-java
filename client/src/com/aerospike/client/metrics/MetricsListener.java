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

import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;

/**
 * Client metrics listener.
 */
public interface MetricsListener {
	/**
	 * Periodic extended metrics has been enabled for the given cluster.
	 */
	public void onEnable(Cluster cluster, MetricsPolicy policy);

	/**
	 * A metrics snapshot has been requested for the given cluster.
	 */
	public void onSnapshot(Cluster cluster);

	/**
	 * A node is being dropped from the cluster.
	 */
	public void onNodeClose(Node node);

	/**
	 * Periodic extended metrics has been disabled for the given cluster.
	 */
	public void onDisable(Cluster cluster);
}

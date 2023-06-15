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
package com.aerospike.client.listener;

import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.NodeStats;
import com.aerospike.client.policy.StatsPolicy;

/**
 * Client periodic statistics configuration.
 */
public interface StatsListener {
	/**
	 * Periodic notification of metrics snapshot. The default listener implementation writes the
	 * metrics snapshot to a file which will later be read and forwarded to OpenTelemetry by a
	 * separate offline application.
	 * <p>
	 * The listener could be overriden to send the metrics snapshot directly to OpenTelemetry.
	 */
	public void onEnable(StatsPolicy policy);
	public void onStats(ClusterStats stats);
	public void onNodeClose(NodeStats stats);
	public void onDisable(ClusterStats stats);
}

/*
 * Copyright 2012-2025 Aerospike, Inc.
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
package com.aerospike.client.policy;

/**
 * Defines algorithm used to determine the target node for a command.
 */
public enum Replica {
	/**
	 * Use node containing key's master partition.
	 */
	MASTER,

	/**
	 * Distribute reads across nodes containing key's master and replicated partitions
	 * in round-robin fashion.  Writes always use node containing key's master partition.
	 */
	MASTER_PROLES,

	/**
	 * Try node containing master partition first.
	 * If connection fails, all commands try nodes containing replicated partitions.
	 * If socketTimeout is reached, reads also try nodes containing replicated partitions,
	 * but writes remain on master node.
	 */
	SEQUENCE,

	/**
	 * Try node on the same rack as the client first.  If timeout or there are no nodes on the
	 * same rack, use SEQUENCE instead.
	 * <p>
	 * {@link ClientPolicy#rackAware}, {@link ClientPolicy#rackId}, and server rack
	 * configuration must also be set to enable this functionality.
	 */
	PREFER_RACK,

	/**
	 * Distribute reads and writes across all nodes in cluster in round-robin fashion.
	 * Writes always use node containing key's master partition.
	 * <p>
	 * This option is useful on reads when the replication factor equals the number
	 * of nodes in the cluster and the overhead of requesting proles is not desired.
	 * <p>
	 * This option could temporarily be useful on writes when the client can't connect
	 * to a node, but that node is reachable via a proxy from a different node.
	 * <p>
	 * This option can also be used to test server proxies.
	 */
	RANDOM;
}

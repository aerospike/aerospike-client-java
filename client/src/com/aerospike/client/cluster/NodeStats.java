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

/**
 * Node statistics.
 */
public final class NodeStats {
	/**
	 * Node for which statistics are generated.
	 */
	public final Node node;

	/**
	 * Connection statistics for sync commands.
	 */
	public final ConnectionStats sync;

	/**
	 * Connection statistics for async commands.
	 */
	public final ConnectionStats async;

	/**
	 * Node statistics constructor.
	 */
	public NodeStats(Node node) {
		this.node = node;
		this.sync = node.getConnectionStats();
		this.async = node.getAsyncConnectionStats();
	}

	/**
	 * Convert statistics to string.
	 */
	public String toString() {
		return node + " sync(" + sync + ") async(" + async + ')';
	}
}

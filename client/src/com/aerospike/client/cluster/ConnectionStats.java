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
 * Connection statistics.
 */
public final class ConnectionStats {
	/**
	 * Active connections in currently executing commands.
	 */
	public final int inUse;

	/**
	 * Connections residing in connection pool(s).
	 */
	public final int inPool;

	/**
	 * Total number of node connections opened since node creation.
	 */
	public final int opened;

	/**
	 * Total number of node connections closed since node creation.
	 */
	public final int closed;

	/**
	 * Connection statistics constructor.
	 */
	public ConnectionStats(int inUse, int inPool, int opened, int closed) {
		this.inUse = inUse;
		this.inPool = inPool;
		this.opened = opened;
		this.closed = closed;
	}

	/**
	 * Convert statistics to string.
	 */
	public String toString() {
		return "" + inUse + ',' + inPool + ',' + opened + ',' + closed;
	}
}

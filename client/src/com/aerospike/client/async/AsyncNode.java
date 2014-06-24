/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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

import java.util.concurrent.ArrayBlockingQueue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.NodeValidator;

/**
 * Asynchronous server node representation.
 */
public final class AsyncNode extends Node {

	private final ArrayBlockingQueue<AsyncConnection> asyncConnQueue;
	private final AsyncCluster cluster;

	/**
	 * Initialize server node with connection parameters.
	 * 
	 * @param cluster			collection of active server nodes 
	 * @param nv				connection parameters
	 */
	public AsyncNode(AsyncCluster cluster, NodeValidator nv) {
		super(cluster, nv);
		this.cluster = cluster;
		asyncConnQueue = new ArrayBlockingQueue<AsyncConnection>(cluster.getMaxCommands());
	}
	
	/**
	 * Get asynchronous socket connection from connection pool for the server node.
	 */
	public AsyncConnection getAsyncConnection() throws AerospikeException.Connection {
		// Try to find connection in pool.
		AsyncConnection conn;

		while ((conn = asyncConnQueue.poll()) != null) {		
			if (conn.isValid()) {
				return conn;
			}
			conn.close();
		}
		return new AsyncConnection(address, cluster);
	}
	
	/**
	 * Put asynchronous connection back into connection pool.
	 * 
	 * @param conn				socket connection
	 */
	public void putAsyncConnection(AsyncConnection conn) {
		if (! active || ! asyncConnQueue.offer(conn)) {
			conn.close();
		}
	}
	
	/**
	 * Close all asynchronous connections in the pool.
	 */
	@Override
	protected void closeConnections() {
		super.closeConnections();
		
		AsyncConnection conn;
		while ((conn = asyncConnQueue.poll()) != null) {			
			conn.close();
		}
	}
}

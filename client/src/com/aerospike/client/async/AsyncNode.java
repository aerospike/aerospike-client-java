/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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

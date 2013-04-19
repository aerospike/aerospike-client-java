/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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

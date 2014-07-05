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
package com.aerospike.client.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Log;

/**
 * Server node representation.  This class manages server node connections and health status.
 */
public class Node implements Closeable {
	/**
	 * Number of partitions for each namespace.
	 */
	public static final int PARTITIONS = 4096;
	private static final int FULL_HEALTH = 100;

	protected final Cluster cluster;
	private final String name;
	private final Host host;
	private Host[] aliases;
	protected final InetSocketAddress address;
	private final ArrayBlockingQueue<Connection> connectionQueue;
	private final AtomicInteger health;
	private int partitionGeneration;
	protected int referenceCount;
	protected boolean responded;
	protected final boolean useNewInfo;
	protected volatile boolean active;

	/**
	 * Initialize server node with connection parameters.
	 * 
	 * @param cluster			collection of active server nodes 
	 * @param nv				connection parameters
	 */
	public Node(Cluster cluster, NodeValidator nv) {
		this.cluster = cluster;
		this.name = nv.name;
		this.aliases = nv.aliases;
		this.address = nv.address;
		this.useNewInfo = nv.useNewInfo;
		
		// Assign host to first IP alias because the server identifies nodes 
		// by IP address (not hostname). 
		this.host = aliases[0];
		
		connectionQueue = new ArrayBlockingQueue<Connection>(cluster.connectionQueueSize);		
		health = new AtomicInteger(FULL_HEALTH);
		partitionGeneration = -1;
		active = true;
	}
	
	/**
	 * Request current status from server node.
	 *  
	 * @param friends		other nodes in the cluster, populated by this method
	 * @throws Exception	if status request fails
	 */
	public final void refresh(List<Host> friends) throws Exception {
		Connection conn = getConnection(1000);
		
		try {
			HashMap<String,String> infoMap = Info.request(conn, "node", "partition-generation", "services");
			verifyNodeName(infoMap);			
			restoreHealth();
			responded = true;
			addFriends(infoMap, friends);
			updatePartitions(conn, infoMap);
			putConnection(conn);
		}
		catch (Exception e) {
			conn.close();
			decreaseHealth();
			throw e;
		}
	}
	
	private final void verifyNodeName(HashMap <String,String> infoMap) throws AerospikeException {
		// If the node name has changed, remove node from cluster and hope one of the other host
		// aliases is still valid.  Round-robbin DNS may result in a hostname that resolves to a
		// new address.
		String infoName = infoMap.get("node");
		
		if (infoName == null || infoName.length() == 0) {
			decreaseHealth();
			throw new AerospikeException.Parse("Node name is empty");
		}

		if (! name.equals(infoName)) {
			// Set node to inactive immediately.
			active = false;
			throw new AerospikeException("Node name has changed. Old=" + name + " New=" + infoName);
		}
	}
	
	private final void addFriends(HashMap <String,String> infoMap, List<Host> friends) throws AerospikeException {
		// Parse the service addresses and add the friends to the list.
		String friendString = infoMap.get("services");
		
		if (friendString == null || friendString.length() == 0) {
			return;
		}

		String friendNames[] = friendString.split(";");

		for (String friend : friendNames) {
			String friendInfo[] = friend.split(":");
			String host = friendInfo[0];
			int port = Integer.parseInt(friendInfo[1]);
			Host alias = new Host(host, port);
			Node node = cluster.findAlias(alias);
			
			if (node != null) {
				node.referenceCount++;
			}
			else {
				if (! findAlias(friends, alias)) {
					friends.add(alias);					
				}
			}
		}
	}
		
	private final static boolean findAlias(List<Host> friends, Host alias) {
		for (Host host : friends) {
			if (host.equals(alias)) {
				return true;
			}
		}
		return false;
	}

	private final void updatePartitions(Connection conn, HashMap<String,String> infoMap) 
		throws AerospikeException, IOException {	
		String genString = infoMap.get("partition-generation");
				
		if (genString == null || genString.length() == 0) {
			throw new AerospikeException.Parse("partition-generation is empty");
		}

		int generation = Integer.parseInt(genString);
		
		if (partitionGeneration != generation) {
			if (Log.debugEnabled()) {
				Log.debug("Node " + this + " partition generation " + generation + " changed.");
			}
			cluster.updatePartitions(conn, this);
			partitionGeneration = generation;
		}
	}
	
	/**
	 * Get a socket connection from connection pool to the server node.
	 * 
	 * @param timeoutMillis			connection timeout value in milliseconds if a new connection is created	
	 * @return						socket connection
	 * @throws AerospikeException	if a connection could not be provided 
	 */
	public final Connection getConnection(int timeoutMillis) throws AerospikeException.Connection {
		Connection conn;
		
		while ((conn = connectionQueue.poll()) != null) {		
			if (conn.isValid()) {
				try {
					conn.setTimeout(timeoutMillis);
					return conn;
				}
				catch (Exception e) {
					// Set timeout failed. Something is probably wrong with timeout
					// value itself, so don't empty queue retrying.  Just get out.
					conn.close();
					throw new AerospikeException.Connection(e);
				}
			}
			conn.close();
		}
		return new Connection(address, timeoutMillis, cluster.maxSocketIdle);		
	}
	
	/**
	 * Put connection back into connection pool.
	 * 
	 * @param conn					socket connection
	 */
	public final void putConnection(Connection conn) {
		if (! active || ! connectionQueue.offer(conn)) {
			conn.close();
		}
	}

	/**
	 * Set node status as healthy after successful database operation.
	 */
	public final void restoreHealth() {
		// There can be cases where health is full, but active is false.
		// Once a node has been marked inactive, it stays inactive.
		health.set(FULL_HEALTH);
	}

	/**
	 * Decrease server health status after a connection failure.
	 */
	public final void decreaseHealth() {
		health.decrementAndGet();
	}
	
	/**
	 * Has consecutive node connection errors become critical. 
	 */
	public final boolean isUnhealthy() {
		return health.get() <= 0;
	}
	
	/**
	 * Return server node IP address and port.
	 */
	public final Host getHost() {
		return host;
	}
	
	/**
	 * Return whether node is currently active.
	 */
	public final boolean isActive() {
		return active;
	}

	/**
	 * Return server node name.
	 */
	public final String getName() {
		return name;
	}

	/**
	 * Return server node IP address aliases.
	 */
	public final Host[] getAliases() {
		return aliases;
	}
	
	/**
	 * Add node alias to list.
	 */
	public final void addAlias(Host aliasToAdd) {
		// Aliases are only referenced in the cluster tend thread,
		// so synchronization is not necessary.
		Host[] tmpAliases = new Host[aliases.length + 1];
		int count = 0;
		
		for (Host host : aliases) {
			tmpAliases[count++] = host;
		}
		tmpAliases[count] = aliasToAdd;
		aliases = tmpAliases;
	}

	/**
	 * Close all server node socket connections.
	 */
	public final void close() {
		active = false;
		closeConnections();
	}
	
	@Override
	public final String toString() {
		return name + ' ' + host;
	}

	@Override
	public final int hashCode() {
		return name.hashCode();
	}

	@Override
	public final boolean equals(Object obj) {
		Node other = (Node) obj;
		return this.name.equals(other.name);
	}
	
	@Override
	protected final void finalize() throws Throwable {
		try {
			// Close connections that slipped through the cracks on race conditions.
			closeConnections();
		}
		finally {
			super.finalize();
		}
	}
	
	protected void closeConnections() {
		// Empty connection pool.
		Connection conn;
		while ((conn = connectionQueue.poll()) != null) {			
			conn.close();
		}		
	}	
}

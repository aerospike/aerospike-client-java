/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.cluster;

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
public final class Node {
	/**
	 * Number of partitions for each namespace.
	 */
	public static final int PARTITIONS = 4096;
	private static final int FULL_HEALTH = 300;

	private final Cluster cluster;
	private final String name;
	private final Host host;
	private final Host[] aliases;
	private final InetSocketAddress address;
	private final ArrayBlockingQueue<Connection> connectionQueue;
	private final AtomicInteger health;
	private int partitionGeneration;
	private volatile boolean active;

	/**
	 * Initialize server node with connection parameters.
	 * 
	 * @param cluster			collection of active server nodes 
	 * @param nv				connection parameters
	 * @param connectionLimit	max socket connections to store in pool
	 */
	public Node(Cluster cluster, NodeValidator nv, int connectionLimit) {
		this.cluster = cluster;
		this.name = nv.name;
		this.aliases = nv.aliases;
		this.address = nv.address;
		
		// Assign host to first IP alias because the server identifies nodes 
		// by IP address (not hostname). 
		this.host = aliases[0];
		
		connectionQueue = new ArrayBlockingQueue<Connection>(connectionLimit);
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
	public void refresh(List<Host> friends) throws Exception {
		Connection conn = getConnection(1000);
		
		try {
			HashMap<String,String> infoMap = Info.request(conn, "node", "partition-generation", "services");
			verifyNodeName(infoMap);			
			restoreHealth();
			addFriends(infoMap, friends);
			updatePartitions(conn, infoMap);
			putConnection(conn);
		}
		catch (Exception e) {
			conn.close();
			decreaseHealth(50);
			throw e;
		}
	}
	
	private void verifyNodeName(HashMap <String,String> infoMap) throws AerospikeException {
		// If the node name has changed, remove node from cluster and hope one of the other host
		// aliases is still valid.  Round-robbin DNS may result in a hostname that resolves to a
		// new address.
		String infoName = infoMap.get("node");
		
		if (infoName == null || infoName.length() == 0) {
			decreaseHealth(80);
			throw new AerospikeException.Parse("Node name is empty");
		}

		if (! name.equals(infoName)) {
			// Set node to inactive immediately.
			active = false;
			throw new AerospikeException("Node name has changed. Old=" + name + " New=" + infoName);
		}
	}
	
	private void addFriends(HashMap <String,String> infoMap, List<Host> friends) throws AerospikeException {
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
			
			if (! cluster.findAlias(alias)  && ! findAlias(friends, alias)) {
				friends.add(alias);
			}
		}
	}
		
	private static boolean findAlias(List<Host> friends, Host alias) {
		for (Host host : friends) {
			if (host.equals(alias)) {
				return true;
			}
		}
		return false;
	}

	private void updatePartitions(Connection conn, HashMap<String,String> infoMap) 
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
			PartitionTokenizer tokens = new PartitionTokenizer(conn, "replicas-write");
			Partition partition;
			
			while ((partition = tokens.getNext()) != null) {
				cluster.updatePartition(partition, this);
			}		
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
	public Connection getConnection(int timeoutMillis) throws AerospikeException {
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
		return new Connection(address, timeoutMillis);		
	}
	
	/**
	 * Put connection back into connection pool.
	 * 
	 * @param conn					socket connection
	 */
	public void putConnection(Connection conn) {
		if (! active || ! connectionQueue.offer(conn)) {
			conn.close();
		}
	}

	/**
	 * Set node status as healthy after successful database operation.
	 */
	public void restoreHealth() {
		// There can be cases where health is full, but active is false.
		// Once a node has been marked inactive, it stays inactive.
		health.set(FULL_HEALTH);
	}

	/**
	 * Decrease server health status after a connection failure.
	 * 
	 * @param value					health points
	 */
	public void decreaseHealth(int value) {
		//if (Log.debugEnabled()) {
		//	Log.debug("Node " + this + " decrease health " + value);
		//}
		if (health.addAndGet(-value) <= 0) {
			active = false;
		}
	}

	/**
	 * Return server node IP address and port.
	 */
	public Host getHost() {
		return host;
	}
	
	/**
	 * Return whether node is currently active.
	 */
	public boolean isActive() {
		return active;
	}

	/**
	 * Return server node name.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Return server node IP address aliases.
	 */
	public Host[] getAliases() {
		return aliases;
	}

	/**
	 * Close all server node socket connections.
	 */
	public void close() {
		active = false;
		closeConnections();
	}
	
	@Override
	public String toString() {
		return name + ' ' + host;
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		Node other = (Node) obj;
		return this.name.equals(other.name);
	}
	
	@Override
	protected void finalize() throws Throwable {
		try {
			// Close connections that slipped through the cracks on race conditions.
			closeConnections();
		}
		finally {
			super.finalize();
		}
	}
	
	private void closeConnections() {
		// Empty connection pool.
		Connection conn;
		while ((conn = connectionQueue.poll()) != null) {			
			conn.close();
		}		
	}	
}

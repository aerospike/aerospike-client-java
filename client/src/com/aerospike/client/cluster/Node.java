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
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Info;

public final class Node {	
	public static final int PARTITIONS = 4096;
	private static final int FULL_HEALTH = 300;

	private final Cluster cluster;
	private final String name;
	private final Host host;
	private final List<Host> aliases;
	private final InetSocketAddress address;
	private final ArrayBlockingQueue<Connection> connectionQueue;
	private final AtomicInteger health;
	private int partitionGeneration;
	private volatile boolean active;

	public Node(Cluster cluster, Host host, int connectionLimit, int timeoutMillis) throws AerospikeException {
		this.cluster = cluster;
		this.host = host;

		NodeValidator nv = new NodeValidator(host, timeoutMillis);
		name = nv.name;
		aliases = nv.aliases;
		address = nv.address;
		
		connectionQueue = new ArrayBlockingQueue<Connection>(connectionLimit);
		health = new AtomicInteger(FULL_HEALTH);
		partitionGeneration = -1;
		active = true;
	}
	
	public void refresh(List<Host> friends) throws IOException, AerospikeException {
		Connection conn = getConnection(2000);
		// Connection conn = new Connection(address, 2000);
		
		try {
			Socket socket = conn.getSocket();
			HashMap<String,String> infoMap = requestInfo(socket);
			verifyNodeName(infoMap);			
			restoreHealth();
			addFriends(infoMap, friends);
			updatePartitions(socket, infoMap);
		}
		finally {
			putConnection(conn);
			// conn.close();
		}
	}
	
	private HashMap<String,String> requestInfo(Socket socket) throws AerospikeException, IOException {		
		try {
			socket.setSoTimeout(2000);
			return Info.request(socket, "node", "partition-generation", "services");
		}
		catch (IOException e) {
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
			decreaseHealth(50);
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

	private void updatePartitions(Socket socket, HashMap<String,String> infoMap) 
		throws AerospikeException, IOException {	
		String genString = infoMap.get("partition-generation");
				
		if (genString == null || genString.length() == 0) {
			throw new AerospikeException.Parse("partition-generation is empty");
		}

		int generation = Integer.parseInt(genString);
		
		if (partitionGeneration != generation) {
			PartitionTokenizer tokens = new PartitionTokenizer(socket, "replicas-write");
			Partition partition;
			
			while ((partition = tokens.getNext()) != null) {
				cluster.updatePartition(partition, this);
			}		
			partitionGeneration = generation;
		}
	}
	
	public Connection getConnection(int timeoutMillis) throws AerospikeException {
		if (! active)
			throw new AerospikeException.InvalidNode();
			
		Connection conn;		
		while ((conn = connectionQueue.poll()) != null) {		
			if (conn.isValid()) {
				return conn;
			}
			conn.close();
		}
		
		return new Connection(address, timeoutMillis);		
	}
	
	public void putConnection(Connection conn) {
		if (! active || ! connectionQueue.offer(conn)) {
			conn.close();
		}
	}

	public void restoreHealth() {
		// There can be cases where health is full, but active is false.
		// Once a node has been marked inactive, it stays inactive.
		health.set(FULL_HEALTH);
	}

	public void decreaseHealth(int value) {
		if (health.addAndGet(-value) <= 0) {
			active = false;
		}
	}

	public Host getHost() {
		return host;
	}
	
	public boolean isActive() {
		return active;
	}

	public String getName() {
		return name;
	}

	public List<Host> getAliases() {
		return aliases;
	}

	public void close() {
		active = false;
		closeConnections();
	}
	
	@Override
	public String toString() {
		return name;
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

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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.admin.AdminCommand;
import com.aerospike.client.admin.AdminCommand.LoginCommand;
import com.aerospike.client.async.AsyncConnection;
import com.aerospike.client.async.EventState;
import com.aerospike.client.util.ThreadLocalData;
import com.aerospike.client.util.Util;

/**
 * Server node representation.  This class manages server node connections and health status.
 */
public class Node implements Closeable {
	/**
	 * Number of partitions for each namespace.
	 */
	public static final int PARTITIONS = 4096;

	public static final int HAS_GEO	= (1 << 0);
	public static final int HAS_TRUNCATE_NS = (1 << 1);
	public static final int HAS_BIT_OP = (1 << 2);
	public static final int HAS_INDEX_EXISTS = (1 << 3);
	public static final int HAS_PEERS = (1 << 4);
	public static final int HAS_REPLICAS = (1 << 5);
	public static final int HAS_CLUSTER_STABLE = (1 << 6);
	public static final int HAS_LUT_NOW = (1 << 7);
	public static final int HAS_PARTITION_SCAN = (1 << 8);

	private static final String[] INFO_PERIODIC = new String[] {"node", "peers-generation", "partition-generation"};
	private static final String[] INFO_PERIODIC_REB = new String[] {"node", "peers-generation", "partition-generation", "rebalance-generation"};

	protected final Cluster cluster;
	private final String name;
	private final Host host;
	protected final List<Host> aliases;
	protected final InetSocketAddress address;
	private final Pool[] connectionPools;
	private final AsyncPool[] asyncConnectionPools;
	private Connection tendConnection;
	private byte[] sessionToken;
	private long sessionExpiration;
	private volatile Map<String,Integer> racks;
	final AtomicInteger connsOpened;
	final AtomicInteger connsClosed;
	protected int connectionIter;
	protected int peersGeneration;
	protected int partitionGeneration;
	protected int rebalanceGeneration;
	protected int peersCount;
	protected int referenceCount;
	protected int failures;
	private final int features;
	protected boolean partitionChanged;
	protected boolean rebalanceChanged;
	protected volatile boolean performLogin;
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
		this.host = nv.primaryHost;
		this.address = nv.primaryAddress;
		this.tendConnection = nv.primaryConn;
		this.sessionToken = nv.sessionToken;
		this.sessionExpiration = nv.sessionExpiration;
		this.features = nv.features;
		this.connsOpened = new AtomicInteger(1);
		this.connsClosed = new AtomicInteger(0);

		// Create sync connection pools.
		connectionPools = new Pool[cluster.connPoolsPerNode];
		int max = cluster.connectionQueueSize / cluster.connPoolsPerNode;
		int rem = cluster.connectionQueueSize - (max * cluster.connPoolsPerNode);

		for (int i = 0; i < connectionPools.length; i++) {
			int capacity = i < rem ? max + 1 : max;
			connectionPools[i] = new Pool(capacity);
		}

		// Create async connection pools only if event loops have been defined.
		if (cluster.eventState != null) {
			asyncConnectionPools = new AsyncPool[cluster.eventState.length];
			max = cluster.connectionQueueSize / asyncConnectionPools.length;
			rem = cluster.connectionQueueSize - (max * asyncConnectionPools.length);

			for (int i = 0; i <  cluster.eventState.length; i++) {
				int capacity = i < rem ? max + 1 : max;
				asyncConnectionPools[i] = new AsyncPool(capacity);
			}
		}
		else {
			asyncConnectionPools = null;
		}

		if (cluster.rackAware) {
			this.racks = new HashMap<String,Integer>();
		}
		else {
			this.racks = null;
		}

		peersGeneration = -1;
		partitionGeneration = -1;
		rebalanceGeneration = -1;
		active = true;
	}

	/**
	 * Request current status from server node.
	 */
	public final void refresh(Peers peers) {
		if (! active) {
			return;
		}

		try {
			if (tendConnection.isClosed()) {
				tendConnection = (cluster.tlsPolicy != null && !cluster.tlsPolicy.forLoginOnly) ?
					new Connection(cluster.tlsPolicy, host.tlsName, address, cluster.connectionTimeout, cluster.maxSocketIdleNanos, null, this) :
					new Connection(address, cluster.connectionTimeout, cluster.maxSocketIdleNanos, null, this);

				if (cluster.user != null) {
					try {
						if (! ensureLogin()) {
							AdminCommand command = new AdminCommand(ThreadLocalData.getBuffer());
							if (! command.authenticate(cluster, tendConnection, sessionToken)) {
								// Authentication failed.  Session token probably expired.
								// Must login again to get new session token.
								LoginCommand login = new LoginCommand(cluster, tendConnection);
								sessionToken = login.sessionToken;
								sessionExpiration = login.sessionExpiration;
							}
						}
					}
					catch (AerospikeException ae) {
						tendConnection.close(this);
						throw ae;
					}
					catch (Exception e) {
						tendConnection.close(this);
						throw new AerospikeException(e);
					}
				}
			}
			else {
				if (cluster.user != null) {
					ensureLogin();
				}
			}

			if (peers.usePeers) {
				String[] commands = cluster.rackAware ? INFO_PERIODIC_REB : INFO_PERIODIC;
				HashMap<String,String> infoMap = Info.request(tendConnection, commands);

				verifyNodeName(infoMap);
				verifyPeersGeneration(infoMap, peers);
				verifyPartitionGeneration(infoMap);

				if (cluster.rackAware) {
					verifyRebalanceGeneration(infoMap);
				}
			}
			else {
				String[] commands = cluster.useServicesAlternate ?
					new String[] {"node", "partition-generation", "services-alternate"} :
					new String[] {"node", "partition-generation", "services"};

				HashMap<String,String> infoMap = Info.request(tendConnection, commands);
				verifyNodeName(infoMap);
				verifyPartitionGeneration(infoMap);
				addFriends(infoMap, peers);
			}
			peers.refreshCount++;
			failures = 0;
		}
		catch (Exception e) {
			if (peers.usePeers) {
				peers.genChanged = true;
			}
			refreshFailed(e);
		}
	}

	private boolean ensureLogin() throws IOException {
		if (performLogin || (sessionExpiration > 0 && System.nanoTime() >= sessionExpiration)) {
			LoginCommand login = new LoginCommand(cluster, tendConnection);
			sessionToken = login.sessionToken;
			sessionExpiration = login.sessionExpiration;
			performLogin = false;
			return true;
		}
		return false;
	}

	public final void signalLogin() {
		// Only login when sessionToken is supported
		// and login not already been requested.
		if (! performLogin) {
			performLogin = true;
			cluster.interruptTendSleep();
		}
	}

	private final void verifyNodeName(HashMap <String,String> infoMap) {
		// If the node name has changed, remove node from cluster and hope one of the other host
		// aliases is still valid.  Round-robbin DNS may result in a hostname that resolves to a
		// new address.
		String infoName = infoMap.get("node");

		if (infoName == null || infoName.length() == 0) {
			throw new AerospikeException.Parse("Node name is empty");
		}

		if (! name.equals(infoName)) {
			// Set node to inactive immediately.
			active = false;
			throw new AerospikeException("Node name has changed. Old=" + name + " New=" + infoName);
		}
	}

	private final void verifyPeersGeneration(HashMap<String,String> infoMap, Peers peers) {
		String genString = infoMap.get("peers-generation");

		if (genString == null || genString.length() == 0) {
			throw new AerospikeException.Parse("peers-generation is empty");
		}

		int gen = Integer.parseInt(genString);

		if (peersGeneration != gen) {
			peers.genChanged = true;
		}
	}

	private final void verifyPartitionGeneration(HashMap<String,String> infoMap) {
		String genString = infoMap.get("partition-generation");

		if (genString == null || genString.length() == 0) {
			throw new AerospikeException.Parse("partition-generation is empty");
		}

		int gen = Integer.parseInt(genString);

		if (partitionGeneration != gen) {
			this.partitionChanged = true;
		}
	}

	private final void verifyRebalanceGeneration(HashMap<String,String> infoMap) {
		String genString = infoMap.get("rebalance-generation");

		if (genString == null || genString.length() == 0) {
			throw new AerospikeException.Parse("rebalance-generation is empty");
		}

		int gen = Integer.parseInt(genString);

		if (rebalanceGeneration != gen) {
			this.rebalanceChanged = true;
		}
	}

	private final void addFriends(HashMap <String,String> infoMap, Peers peers) throws AerospikeException {
		// Parse the service addresses and add the friends to the list.
		String command = cluster.useServicesAlternate ? "services-alternate" : "services";
		String friendString = infoMap.get(command);

		if (friendString == null || friendString.length() == 0) {
			peersCount = 0;
			return;
		}

		String friendNames[] = friendString.split(";");
		peersCount = friendNames.length;

		for (String friend : friendNames) {
			String friendInfo[] = friend.split(":");
			String hostname = friendInfo[0];

			if (cluster.ipMap != null) {
				String alternativeHost = cluster.ipMap.get(hostname);

				if (alternativeHost != null) {
					hostname = alternativeHost;
				}
			}

			int port = Integer.parseInt(friendInfo[1]);
			Host host = new Host(hostname, port);

			// Check global aliases for existing cluster.
			Node node = cluster.aliases.get(host);

			if (node == null) {
				// Check local aliases for this tend iteration.
				if (! peers.hosts.contains(host)) {
					prepareFriend(host, peers);
				}
			}
			else {
				node.referenceCount++;
			}
		}
	}

	private final boolean prepareFriend(Host host, Peers peers) {
		try {
			NodeValidator nv = new NodeValidator();
			nv.validateNode(cluster, host);

			// Check for duplicate nodes in nodes slated to be added.
			Node node = peers.nodes.get(nv.name);

			if (node != null) {
				// Duplicate node name found.  This usually occurs when the server
				// services list contains both internal and external IP addresses
				// for the same node.
				nv.primaryConn.close();
				peers.hosts.add(host);
				node.aliases.add(host);
				return true;
			}

			// Check for duplicate nodes in cluster.
			node = cluster.nodesMap.get(nv.name);

			if (node != null) {
				nv.primaryConn.close();
				peers.hosts.add(host);
				node.aliases.add(host);
				node.referenceCount++;
				cluster.aliases.put(host, node);
				return true;
			}

			node = cluster.createNode(nv);
			peers.hosts.add(host);
			peers.nodes.put(nv.name, node);
			return true;
		}
		catch (Exception e) {
			if (Log.warnEnabled()) {
				Log.warn("Add node " + host + " failed: " + Util.getErrorMessage(e));
			}
			return false;
		}
	}

	protected final void refreshPeers(Peers peers) {
		// Do not refresh peers when node connection has already failed during this cluster tend iteration.
		if (failures > 0 || ! active) {
			return;
		}

		try {
			if (Log.debugEnabled()) {
				Log.debug("Update peers for node " + this);
			}
			PeerParser parser = new PeerParser(cluster, tendConnection, peers.peers);
			peersCount = peers.peers.size();

			boolean peersValidated = true;

			for (Peer peer : peers.peers) {
				if (findPeerNode(cluster, peers, peer.nodeName)) {
					// Node already exists. Do not even try to connect to hosts.
					continue;
				}

				boolean nodeValidated = false;

				// Find first host that connects.
				for (Host host : peer.hosts) {
					try {
						// Attempt connection to host.
						NodeValidator nv = new NodeValidator();
						nv.validateNode(cluster, host);

						if (! peer.nodeName.equals(nv.name)) {
							// Must look for new node name in the unlikely event that node names do not agree.
							if (Log.warnEnabled()) {
								Log.warn("Peer node " + peer.nodeName + " is different than actual node " + nv.name + " for host " + host);
							}

							if (findPeerNode(cluster, peers, nv.name)) {
								// Node already exists. Do not even try to connect to hosts.
								nv.primaryConn.close();
								nodeValidated = true;
								break;
							}
						}

						// Create new node.
						Node node = cluster.createNode(nv);
						peers.nodes.put(nv.name, node);
						nodeValidated = true;
						break;
					}
					catch (Exception e) {
						if (Log.warnEnabled()) {
							Log.warn("Add node " + host + " failed: " + Util.getErrorMessage(e));
						}
					}
				}

				if (! nodeValidated) {
					peersValidated = false;
				}
			}

			// Only set new peers generation if all referenced peers are added to the cluster.
			if (peersValidated) {
				peersGeneration = parser.generation;
			}
			peers.refreshCount++;
		}
		catch (Exception e) {
			refreshFailed(e);
		}
	}

	private static boolean findPeerNode(Cluster cluster, Peers peers, String nodeName) {
		// Check global node map for existing cluster.
		Node node = cluster.nodesMap.get(nodeName);

		if (node != null) {
			node.referenceCount++;
			return true;
		}

		// Check local node map for this tend iteration.
		node = peers.nodes.get(nodeName);

		if (node != null) {
			node.referenceCount++;
			return true;
		}
		return false;
	}

	protected final void refreshPartitions(Peers peers) {
		// Do not refresh partitions when node connection has already failed during this cluster tend iteration.
		// Also, avoid "split cluster" case where this node thinks it's a 1-node cluster.
		// Unchecked, such a node can dominate the partition map and cause all other
		// nodes to be dropped.
		if (failures > 0 || ! active || (peersCount == 0 && peers.refreshCount > 1)) {
			return;
		}

		try {
			if (Log.debugEnabled()) {
				Log.debug("Update partition map for node " + this);
			}
			PartitionParser parser = new PartitionParser(tendConnection, this, cluster.partitionMap, Node.PARTITIONS);

			if (parser.isPartitionMapCopied()) {
				cluster.partitionMap = parser.getPartitionMap();
			}
			partitionGeneration = parser.getGeneration();
		}
		catch (Exception e) {
			refreshFailed(e);
		}
	}

	protected final void refreshRacks() {
		// Do not refresh racks when node connection has already failed during this cluster tend iteration.
		if (failures > 0 || ! active) {
			return;
		}

		try {
			if (Log.debugEnabled()) {
				Log.debug("Update racks for node " + this);
			}
			RackParser parser = new RackParser(tendConnection, this);

			this.rebalanceGeneration = parser.getGeneration();
			this.racks = parser.getRacks();
		}
		catch (Exception e) {
			refreshFailed(e);
		}
	}

	private final void refreshFailed(Exception e) {
		failures++;

		if (! tendConnection.isClosed()) {
			tendConnection.close(this);
		}

		// Only log message if cluster is still active.
		if (cluster.tendValid && Log.warnEnabled()) {
			Log.warn("Node " + this + " refresh failed: " + Util.getErrorMessage(e));
		}
	}

	/**
	 * Get a socket connection from connection pool to the server node.
	 */
	public final Connection getConnection(int timeoutMillis) throws AerospikeException {
		try {
			return getConnection(timeoutMillis, 0);
		}
		catch (Connection.ReadTimeout crt) {
			throw new AerospikeException.Timeout(this, timeoutMillis);
		}
	}

	/**
	 * Get a socket connection from connection pool to the server node.
	 */
	public final Connection getConnection(int timeoutMillis, int timeoutDelay) {
		int max = cluster.connPoolsPerNode;
		int initialIndex;
		boolean backward;

		if (max == 1) {
			initialIndex = 0;
			backward = false;
		}
		else {
			int iter = connectionIter++; // not atomic by design
			initialIndex = iter % max;
			if (initialIndex < 0) {
				initialIndex += max;
			}
			backward = true;
		}

		Pool pool = connectionPools[initialIndex];
		int queueIndex = initialIndex;
		Connection conn;

		while (true) {
			conn = pool.poll();

			if (conn != null) {
				// Found socket.
				// Verify that socket is active and receive buffer is empty.
				if (conn.isValid()) {
					try {
						conn.setTimeout(timeoutMillis);
						return conn;
					}
					catch (Exception e) {
						// Set timeout failed. Something is probably wrong with timeout
						// value itself, so don't empty queue retrying.  Just get out.
						closeConnection(conn);
						throw new AerospikeException.Connection(e);
					}
				}
				closeConnection(conn);
			}
			else if (pool.total.getAndIncrement() < pool.capacity()) {
				// Socket not found and queue has available slot.
				// Create new connection.
				try {
					conn = (cluster.tlsPolicy != null && !cluster.tlsPolicy.forLoginOnly) ?
						new Connection(cluster.tlsPolicy, host.tlsName, address, timeoutMillis, cluster.maxSocketIdleNanos, pool, this) :
						new Connection(address, timeoutMillis, cluster.maxSocketIdleNanos, pool, this);
				}
				catch (RuntimeException re) {
					pool.total.getAndDecrement();
					throw re;
				}

				if (cluster.user != null) {
					try {
						AdminCommand command = new AdminCommand(ThreadLocalData.getBuffer());
						if (! command.authenticate(cluster, conn, sessionToken)) {
							signalLogin();
							throw new AerospikeException("Authentication failed");
						}
					}
					catch (AerospikeException ae) {
						// Socket not authenticated.  Do not put back into pool.
						closeConnection(conn);
						throw ae;
					}
					catch (Connection.ReadTimeout crt) {
						if (timeoutDelay > 0) {
							// The connection state is always STATE_READ_AUTH_HEADER here which does not reference
							// isSingle, so just pass in true for isSingle in ConnectionRecover.
							cluster.recoverConnection(new ConnectionRecover(conn, this, timeoutDelay, crt, true));
						}
						else {
							closeConnection(conn);
						}
						throw crt;
					}
					catch (RuntimeException re) {
						closeConnection(conn);
						throw re;
					}
					catch (SocketTimeoutException ste) {
						closeConnection(conn);
						// This is really a socket write timeout, but the calling
						// method's catch handler just identifies error as a client
						// timeout, which is what we need.
						throw new Connection.ReadTimeout(null, 0, 0, (byte)0);
					}
					catch (IOException ioe) {
						closeConnection(conn);
						throw new AerospikeException.Connection(ioe);
					}
				}
				return conn;
			}
			else {
				// Socket not found and queue is full.  Try another queue.
				pool.total.getAndDecrement();

				if (backward) {
					if (queueIndex > 0) {
						queueIndex--;
					}
					else {
						queueIndex = initialIndex;

						if (++queueIndex >= max) {
							break;
						}
						backward = false;
					}
				}
				else if (++queueIndex >= max) {
					break;
				}
				pool = connectionPools[queueIndex];
			}
		}
		throw new AerospikeException.Connection(ResultCode.NO_MORE_CONNECTIONS,
				"Node " + this + " max connections " + cluster.connectionQueueSize + " would be exceeded.");
	}

	/**
	 * Put connection back into connection pool.
	 *
	 * @param conn					socket connection
	 */
	public final void putConnection(Connection conn) {
		if (! active || ! conn.pool.offer(conn)) {
			closeConnection(conn);
		}
	}

	/**
	 * Close connection and decrement connection count.
	 */
	public final void closeConnection(Connection conn) {
		conn.pool.total.getAndDecrement();
		conn.close(this);
	}

	public final void closeIdleConnections() {
		for (Pool pool : connectionPools) {
			pool.closeIdle(this);
		}
	}

	public final ConnectionStats getConnectionStats() {
		int inUse = 0;
		int inPool = 0;

		for (Pool pool : connectionPools) {
			int tmp = pool.size();
			inPool += tmp;
			tmp = pool.total.get() - tmp;

			// Timing issues may cause values to go negative. Adjust.
			if (tmp < 0) {
				tmp = 0;
			}
			inUse += tmp;
		}
		return new ConnectionStats(inUse, inPool, connsOpened.get(), connsClosed.get());
	}

	public final AsyncConnection getAsyncConnection(int index, ByteBuffer byteBuffer) {
		AsyncPool pool = asyncConnectionPools[index];
		ArrayDeque<AsyncConnection> queue = pool.queue;
		AsyncConnection conn;

		while ((conn = queue.pollFirst()) != null) {
			if (conn.isValid(byteBuffer)) {
				return conn;
			}
			closeAsyncConnection(conn, index);
		}

		if (pool.total >= pool.capacity) {
			throw new AerospikeException.Connection(ResultCode.NO_MORE_CONNECTIONS,
					"Node " + this + " event loop " + index + " of " + cluster.eventLoops.getSize() +
					" max connections " + pool.capacity + " would be exceeded.");
		}
		pool.total++;
		return null;
	}

	public final void connectionOpened(int index) {
		asyncConnectionPools[index].opened++;
	}

	public final void putAsyncConnection(AsyncConnection conn, int index) {
		asyncConnectionPools[index].queue.addFirst(conn);
	}

	public final void closeAsyncConnection(AsyncConnection conn, int index) {
		asyncConnectionPools[index].connectionClosed();
		conn.close();
	}

	public final void decrAsyncConnection(int index) {
		asyncConnectionPools[index].total--;
	}

	public final void closeIdleAsyncConnections(int index) {
		AsyncPool pool = asyncConnectionPools[index];
		ArrayDeque<AsyncConnection> queue = pool.queue;
		AsyncConnection conn;

		// Oldest connection is at end of queue.
		while ((conn = queue.peekLast()) != null) {
			if (conn.isCurrent()) {
				return;
			}

			// Pop connection from queue.
			queue.pollLast();
			pool.connectionClosed();
			conn.close();
		}
	}

	public final ConnectionStats getAsyncConnectionStats() {
		int inUse = 0;
		int inPool = 0;
		int opened = 0;
		int closed = 0;

		if (asyncConnectionPools != null) {
			for (AsyncPool pool : asyncConnectionPools) {
				// Warning: cross-thread references are made without a lock
				// for pool's queue, opened and closed.
				int tmp =  pool.queue.size();

				// Timing issues may cause values to go negative. Adjust.
				if (tmp < 0) {
					tmp = 0;
				}
				inPool += tmp;
				tmp = pool.total - tmp;

				if (tmp < 0) {
					tmp = 0;
				}
				inUse += tmp;
				opened += pool.opened;
				closed += pool.closed;
			}
		}
		return new ConnectionStats(inUse, inPool, opened, closed);
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
	 * Return node IP address.
	 */
	public final InetSocketAddress getAddress() {
		return address;
	}

	/**
	 * Return node session token.
	 */
	public final byte[] getSessionToken() {
		return sessionToken;
	}

	/**
	 * Return if this node has the same rack as the client for the
	 * given namespace.
	 */
	public final boolean hasRack(String namespace, int rackId) {
		// Must copy map reference for copy on write semantics to work.
		Map<String,Integer> map = this.racks;

		if (map == null) {
			return false;
		}

		Integer r = map.get(namespace);

		if (r == null) {
			return false;
		}

		return r == rackId;
	}

	/**
	 * Does server support cluster-stable info command.
	 */
	public final boolean hasClusterStable() {
		return (features & HAS_CLUSTER_STABLE) != 0;
	}

	/**
	 * Does server support lut=now in truncate info command.
	 */
	public final boolean hasLutNow() {
		return (features & HAS_LUT_NOW) != 0;
	}

	/**
	 * Does server support truncate-namespace info command.
	 */
	public final boolean hasTruncateNamespace() {
		return (features & HAS_TRUNCATE_NS) != 0;
	}

	/**
	 * Does server support replicas info command.
	 */
	public final boolean hasReplicas() {
		return (features & HAS_REPLICAS) != 0;
	}

	/**
	 * Does server support peers info command.
	 */
	public final boolean hasPeers() {
		return (features & HAS_PEERS) != 0;
	}

	/**
	 * Does server support bit operations.
	 */
	public final boolean hasBitOperations() {
		return (features & HAS_BIT_OP) != 0;
	}

	/**
	 * Does server support sindex-exists info command.
	 */
	public final boolean hasIndexExists() {
		return (features & HAS_INDEX_EXISTS) != 0;
	}

	/**
	 * Does server support partition scans.
	 */
	public final boolean hasPartitionScan() {
		return (features & HAS_PARTITION_SCAN) != 0;
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

	/**
	 * Close all socket connections.
	 */
	public final void close() {
		if (cluster.eventLoops == null) {
			// Empty sync connection pools.
			closeSyncConnections();
		}
		else {
			final AtomicInteger eventLoopCount = new AtomicInteger(cluster.eventState.length);

			// Send close node notification to async event loops.
			for (final EventState state : cluster.eventState) {
				state.eventLoop.execute(new Runnable() {
					public void run() {
						closeConnections(eventLoopCount, state.index);
					}
				});
			}
		}
	}

	/**
	 * Close all node socket connections from event loop.
	 * Must be called from event loop thread.
	 */
	public final void closeConnections(AtomicInteger eventLoopCount, int index) {
		closeAsyncConnections(index);

		if (eventLoopCount.decrementAndGet() == 0) {
			// All event loops have reported.
			closeSyncConnections();
		}
	}

	/**
	 * Close asynchronous connections.
	 * Must be called from event loop thread.
	 */
	public final void closeAsyncConnections(int index) {
		AsyncPool pool = asyncConnectionPools[index];
		AsyncConnection conn;

		while ((conn = pool.queue.pollFirst()) != null) {
			conn.close();
		}
	}

	/**
	 * Close synchronous connections.
	 */
	public final void closeSyncConnections() {
		 // Mark node invalid.
		active = false;

		// Close tend connection after making reference copy.
		Connection conn = tendConnection;
		conn.close();

		// Close synchronous connections.
		for (Pool pool : connectionPools) {
			while ((conn = pool.poll()) != null) {
				conn.close();
			}
		}
	}

	private static final class AsyncPool {
		public final ArrayDeque<AsyncConnection> queue;
		public final int capacity;
		public int total;
		public int opened;
		public int closed;

		private AsyncPool(int capacity) {
			this.capacity = capacity;
			this.queue = new ArrayDeque<AsyncConnection>(capacity);
		}

		private void connectionClosed() {
			total--;
			closed++;
		}
	}
}

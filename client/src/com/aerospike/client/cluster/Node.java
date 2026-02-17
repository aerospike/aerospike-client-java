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
package com.aerospike.client.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;
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
import com.aerospike.client.async.AsyncConnectorExecutor;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventState;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.async.NettyConnection;
import com.aerospike.client.command.SyncCommand;
import com.aerospike.client.metrics.Counter;
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.metrics.MetricsPolicy;
import com.aerospike.client.metrics.NodeMetrics;
import com.aerospike.client.util.Util;
import com.aerospike.client.util.Version;

/**
 * Server node representation.  This class manages server node connections and health status.
 */
public class Node implements Closeable {
	/**
	 * Number of partitions for each namespace.
	 */
	public static final int PARTITIONS = 4096;

	public static final int HAS_PARTITION_SCAN = (1 << 0);
	public static final int HAS_QUERY_SHOW = (1 << 1);
	public static final int HAS_BATCH_ANY = (1 << 2);
	public static final int HAS_PARTITION_QUERY = (1 << 3);

	private static final String[] INFO_PERIODIC = new String[] {"node", "peers-generation", "partition-generation"};
	private static final String[] INFO_PERIODIC_REB = new String[] {"node", "peers-generation", "partition-generation", "rebalance-generation"};

	protected final Cluster cluster;
	private final String name;
	private String hostname; // Optional hostname.
	private final Host host; // Host with IP address name.
	protected final InetSocketAddress address;
	private final Pool[] connectionPools;
	private final AsyncPool[] asyncConnectionPools;
	private Connection tendConnection;
	private byte[] sessionToken;
	private long sessionExpiration;
	protected volatile Map<String,Integer> racks;
	private volatile NodeMetrics metrics;
	final AtomicInteger connsOpened;
	final AtomicInteger connsClosed;
	private final AtomicInteger errorRateCount;
	protected int maxErrorRate;
	private final Counter errorCounter;
	private final Counter timeoutCounter;
	private final Counter keyBusyCounter;
	protected int connectionIter;
	private int peersGeneration;
	int partitionGeneration;
	private int rebalanceGeneration;
	protected int peersCount;
	protected int referenceCount;
	protected int failures;
	private final int features;
	protected boolean partitionChanged;
	protected boolean rebalanceChanged;
	protected volatile boolean performLogin;
	protected volatile boolean active;
	public final Version serverVersion;

	/**
	 * Initialize server node with connection parameters.
	 *
	 * @param cluster			collection of active server nodes
	 * @param nv				connection parameters
	 */
	public Node(Cluster cluster, NodeValidator nv) {
		this.cluster = cluster;
		this.name = nv.name;
		this.host = nv.primaryHost;
		this.address = nv.primaryAddress;
		this.tendConnection = nv.primaryConn;
		this.sessionToken = nv.sessionToken;
		this.sessionExpiration = nv.sessionExpiration;
		this.features = nv.features;
		this.serverVersion = nv.serverVersion;
		this.connsOpened = new AtomicInteger(1);
		this.connsClosed = new AtomicInteger(0);
		this.errorRateCount = new AtomicInteger(0);
		this.maxErrorRate = cluster.maxErrorRate;
		this.errorCounter = new Counter();
		this.timeoutCounter = new Counter();
		this.keyBusyCounter = new Counter();
		this.peersGeneration = -1;
		this.partitionGeneration = -1;
		this.rebalanceGeneration = -1;
		this.partitionChanged = true;
		this.rebalanceChanged = cluster.rackAware;
		this.racks = cluster.rackAware ? new HashMap<String,Integer>() : null;
		this.active = true;

		if (cluster.metricsEnabled) {
			this.metrics = new NodeMetrics(cluster.metricsPolicy);
		}

		// Create sync connection pools.
		connectionPools = new Pool[cluster.connPoolsPerNode];
		int min = cluster.minConnsPerNode / cluster.connPoolsPerNode;
		int remMin = cluster.minConnsPerNode - (min * cluster.connPoolsPerNode);
		int max = cluster.maxConnsPerNode / cluster.connPoolsPerNode;
		int remMax = cluster.maxConnsPerNode - (max * cluster.connPoolsPerNode);

		for (int i = 0; i < connectionPools.length; i++) {
			int minSize = i < remMin ? min + 1 : min;
			int maxSize = i < remMax ? max + 1 : max;

			Pool pool = new Pool(minSize, maxSize);
			connectionPools[i] = pool;
		}

		EventState[] eventState = cluster.eventState;

		if (eventState == null) {
			asyncConnectionPools = null;
			return;
		}

		// Create async connection pools.
		asyncConnectionPools = new AsyncPool[eventState.length];
		min = cluster.asyncMinConnsPerNode / asyncConnectionPools.length;
		remMin = cluster.asyncMinConnsPerNode - (min * asyncConnectionPools.length);
		max = cluster.asyncMaxConnsPerNode / asyncConnectionPools.length;
		remMax = cluster.asyncMaxConnsPerNode - (max * asyncConnectionPools.length);

		for (int i = 0; i <  eventState.length; i++) {
			int minSize = i < remMin ? min + 1 : min;
			int maxSize = i < remMax ? max + 1 : max;
			asyncConnectionPools[i] = new AsyncPool(minSize, maxSize);
		}
	}

	public final void createMinConnections() {
		// Create sync connections.
		for (Pool pool : connectionPools) {
			if (pool.minSize > 0) {
				createConnections(pool, pool.minSize);
			}
		}

		EventState[] eventState = cluster.eventState;

		if (eventState == null || cluster.asyncMinConnsPerNode <= 0) {
			return;
		}

		// Create async connections.
		final Monitor monitor = new Monitor();
		final AtomicInteger eventLoopCount = new AtomicInteger(eventState.length);
		final int maxConcurrent = 20 / eventState.length + 1;

		for (int i = 0; i < eventState.length; i++) {
			final int minSize = asyncConnectionPools[i].minSize;

			if (minSize > 0) {
				final EventLoop eventLoop = eventState[i].eventLoop;
				final Node node = this;

				eventLoop.execute(new Runnable() {
					public void run() {
						try {
							new AsyncConnectorExecutor(
								eventLoop, cluster, node, minSize, maxConcurrent, monitor, eventLoopCount
							);
						}
						catch (Throwable e) {
							if (Log.warnEnabled()) {
								Log.warn("AsyncConnectorExecutor failed: " + Util.getErrorMessage(e));
							}
						}
					}
				});
			}
			else {
				AsyncConnectorExecutor.eventLoopComplete(monitor, eventLoopCount);
			}
		}
		// Wait until all async connections are created.
		monitor.waitTillComplete();
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
				tendConnection = createConnection(null, cluster.connectTimeout);

				if (cluster.authEnabled) {
					byte[] token = sessionToken;

					if (token == null || shouldLogin()) {
						login();
					}
					else {
						if (! AdminCommand.authenticate(cluster, tendConnection, token)) {
							// Authentication failed. Session token probably expired.
							// Login again to get new session token.
							login();
						}
					}
				}
			}
			else {
				if (cluster.authEnabled && shouldLogin()) {
					login();
				}
			}

			String[] commands = cluster.rackAware ? INFO_PERIODIC_REB : INFO_PERIODIC;
			HashMap<String,String> infoMap = infoRequest(tendConnection, commands);

			verifyNodeName(infoMap);
			verifyPeersGeneration(infoMap, peers);
			verifyPartitionGeneration(infoMap);

			if (cluster.rackAware) {
				verifyRebalanceGeneration(infoMap);
			}
			peers.refreshCount++;

			// Reload peers, partitions and racks if there were failures on previous tend.
			if (failures > 0) {
				peers.genChanged = true;
				partitionChanged = true;
				rebalanceChanged = cluster.rackAware;
			}
			failures = 0;
		}
		catch (Throwable e) {
			peers.genChanged = true;
			refreshFailed(e);
		}
	}

	private HashMap<String,String> infoRequest(Connection conn, String... names) {
		Info info = new Info(this, conn, names);
		return info.parseMultiResponse();
	}

	private boolean shouldLogin() {
		return performLogin || (sessionExpiration > 0 && System.nanoTime() >= sessionExpiration);
	}

	private void login() throws IOException {
		if (Log.infoEnabled()) {
			Log.info("Login to " + this);
		}

		try {
			LoginCommand login = new LoginCommand(cluster, tendConnection);
			sessionToken = login.sessionToken;
			sessionExpiration = login.sessionExpiration;
			performLogin = false;
		}
		catch (Throwable e) {
			performLogin = true;
			throw e;
		}
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

			if (peersGeneration > gen) {
				if (Log.infoEnabled()) {
					Log.info("Quick node restart detected: node=" + this + " oldgen=" + peersGeneration + " newgen=" + gen);
				}
				restart();
			}
		}
	}

	private final void restart() {
		try {
			// Reset error rate.
			resetErrorRate();

			// Login when user authentication is enabled.
			if (cluster.authEnabled) {
				login();
			}

			// Balance sync connections.
			balanceConnections();

			// Balance async connections.
			if (cluster.eventState != null) {
				for (EventState es : cluster.eventState) {
					final EventLoop eventLoop = es.eventLoop;

					eventLoop.execute(new Runnable() {
						public void run() {
							try {
								balanceAsyncConnections(eventLoop);
							}
							catch (Throwable e) {
								if (Log.warnEnabled()) {
									Log.warn("balanceAsyncConnections failed: " + this + ' ' + Util.getErrorMessage(e));
								}
							}
						}
					});
				}
			}
		}
		catch (Throwable e) {
			if (Log.warnEnabled()) {
				Log.warn("Node restart failed: " + this + ' ' + Util.getErrorMessage(e));
			}
		}
	}

	private boolean isErrorRateValid() {
		return this.errorRateCount.get() <= this.maxErrorRate;
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

	protected final void refreshPeers(Peers peers) {
		// Do not refresh peers when node connection has already failed during this cluster tend iteration.
		if (failures > 0 || ! active) {
			return;
		}

		try {
			if (Log.debugEnabled()) {
				Log.debug("Update peers for node " + this);
			}
			PeerParser parser = new PeerParser(cluster, this, tendConnection, peers.peers);
			peersCount = peers.peers.size();

			boolean peersValidated = true;

			for (Peer peer : peers.peers) {
				if (findPeerNode(cluster, peers, peer)) {
					// Node already exists. Do not even try to connect to hosts.
					continue;
				}

				boolean nodeValidated = false;

				// Find first host that connects.
				for (Host host : peer.hosts) {
					// Do not attempt to add a peer if it has already failed in this cluster tend iteration.
					if (peers.hasFailed(host)) {
						continue;
					}

					try {
						// Attempt connection to host.
						NodeValidator nv = new NodeValidator();
						nv.validateNode(cluster, host);

						if (! peer.nodeName.equals(nv.name)) {
							// Must look for new node name in the unlikely event that node names do not agree.
							if (Log.warnEnabled()) {
								Log.warn("Peer node " + peer.nodeName + " is different than actual node " + nv.name + " for host " + host);
							}
						}

						// Create new node.
						Node node = cluster.createNode(nv);
						peers.nodes.put(nv.name, node);
						nodeValidated = true;

						if (peer.replaceNode != null) {
							if (Log.infoEnabled()) {
								Log.info("Replace node: " + peer.replaceNode);
							}
							peers.removeNodes.add(peer.replaceNode);
						}
						break;
					}
					catch (Throwable e) {
						peers.fail(host);

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
		catch (Throwable e) {
			refreshFailed(e);
		}
	}

	private static boolean findPeerNode(Cluster cluster, Peers peers, Peer peer) {
		// Check global node map for existing cluster.
		Node node = cluster.nodesMap.get(peer.nodeName);

		if (node != null) {
			// Node name found.
			if (node.failures <= 0 || node.address.getAddress().isLoopbackAddress()) {
				// If the node does not have cluster tend errors or is localhost,
				// reject new peer as the IP address does not need to change.
				node.referenceCount++;
				return true;
			}

			// Match peer hosts with the node host.
			for (Host h : peer.hosts) {
				if (h.port == node.host.port) {
					// Check for IP address (node.host.name is an IP address) or hostname if it exists.
					if (h.name.equals(node.host.name) || (node.hostname != null && h.name.equals(node.hostname))) {
						// Main node host is also the same as one of the peer hosts.
						// Peer should not be added.
						node.referenceCount++;
						return true;
					}

					// Peer name might be a hostname. Get peer IP addresses and check with node IP address.
					try {
						InetAddress[] addresses = InetAddress.getAllByName(h.name);

						for (InetAddress address : addresses) {
							if (address.equals(node.address.getAddress()) ||
								address.isLoopbackAddress()) {
								// Set peer hostname for faster future lookups.
								node.hostname = h.name;
								node.referenceCount++;
								return true;
							}
						}
					}
					catch (Throwable t) {
						// Peer name is invalid. replaceNode may be set, but that node will
						// not be replaced because NodeValidator will reject it.
						Log.error("Invalid peer received by cluster tend: " + h.name);
					}
				}
			}
			peer.replaceNode = node;
		}

		// Check local node map for this tend iteration.
		node = peers.nodes.get(peer.nodeName);

		if (node != null) {
			node.referenceCount++;
			peer.replaceNode = null;
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
		catch (Throwable e) {
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
			RackParser parser = new RackParser(this, tendConnection);

			rebalanceGeneration = parser.getGeneration();
			racks = parser.getRacks();
		}
		catch (Throwable e) {
			refreshFailed(e);
		}
	}

	private final void refreshFailed(Throwable e) {
		failures++;

		if (! tendConnection.isClosed()) {
			closeConnectionOnError(tendConnection);
		}

		// Only log message if cluster is still active.
		if (cluster.tendValid && Log.warnEnabled()) {
			Log.warn("Node " + this + " refresh failed: " + Util.getErrorMessage(e));
		}
	}

	private void createConnections(Pool pool, int count) {
		// Create sync connections.
		while (count > 0) {
			Connection conn;

			try {
				conn = createConnection(pool);
			}
			catch (Throwable e) {
				// Failing to create min connections is not considered fatal.
				// Log failure and return.
				if (Log.debugEnabled()) {
					Log.debug("Failed to create connection: " + e.getMessage());
				}
				return;
			}

			if (pool.offer(conn)) {
				pool.total.getAndIncrement();
			}
			else {
				closeIdleConnection(conn);
				break;
			}
			count--;
		}
	}

	private Connection createConnection(Pool pool) {
		Connection conn = createConnection(pool, cluster.connectTimeout);

		if (cluster.authEnabled) {
			byte[] token = sessionToken;

			if (token != null) {
				try {
					if (! AdminCommand.authenticate(cluster, conn, token)) {
						throw new AerospikeException("Authentication failed");
					}
				}
				catch (AerospikeException ae) {
					closeConnectionOnError(conn);
					throw ae;
				}
				catch (Throwable e) {
					closeConnectionOnError(conn);
					throw new AerospikeException(e);
				}
			}
		}
		return conn;
	}

	private Connection createConnection(Pool pool, int timeout) {
		// Create sync connection.
		Connection conn;

		if (cluster.metricsEnabled) {
			long begin = System.nanoTime();

			conn = (cluster.tlsPolicy != null && !cluster.tlsPolicy.forLoginOnly) ?
				new Connection(cluster.tlsPolicy, host.tlsName, address, timeout, this, pool) :
				new Connection(address, timeout, this, pool);

			long elapsed = System.nanoTime() - begin;
			metrics.addLatency(null, LatencyType.CONN, elapsed);
		}
		else {
			conn = (cluster.tlsPolicy != null && !cluster.tlsPolicy.forLoginOnly) ?
				new Connection(cluster.tlsPolicy, host.tlsName, address, timeout, this, pool) :
				new Connection(address, timeout, this, pool);
		}

		connsOpened.getAndIncrement();
		return conn;
	}

	/**
	 * Get a socket connection from connection pool to the server node.
	 */
	public final Connection getConnection(int timeoutMillis) {
		try {
			return getConnection(null, timeoutMillis, timeoutMillis, 0);
		}
		catch (Connection.ReadTimeout crt) {
			throw new AerospikeException.Timeout(this, timeoutMillis, timeoutMillis, timeoutMillis);
		}
	}

	/**
	 * Get a socket connection from connection pool to the server node.
	 */
	public final Connection getConnection(int connectTimeout, int socketTimeout) {
		try {
			return getConnection(null, connectTimeout, socketTimeout, 0);
		}
		catch (Connection.ReadTimeout crt) {
			throw new AerospikeException.Timeout(this, connectTimeout, socketTimeout, socketTimeout);
		}
	}

	/**
	 * Get a socket connection from connection pool to the server node.
	 */
	public final Connection getConnection(SyncCommand cmd, int connectTimeout, int socketTimeout, int timeoutDelay) {
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
				// Verify that socket is active.
				if (cluster.isConnCurrentTran(conn.getLastUsed())) {
					try {
						conn.setTimeout(socketTimeout);
						return conn;
					}
					catch (Throwable e) {
						// Set timeout failed. Something is probably wrong with timeout
						// value itself, so don't empty queue retrying.  Just get out.
						closeConnection(conn);
						throw new AerospikeException.Connection(e);
					}
				}
				pool.closeIdle(this, conn);
			}
			else if (pool.total.getAndIncrement() < pool.capacity()) {
				// Socket not found and queue has available slot.
				// Create new connection.
				long startTime;
				int timeout;

				if (connectTimeout > 0) {
					timeout = connectTimeout;
					startTime = System.nanoTime();
				}
				else {
					timeout = socketTimeout;
					startTime = 0;
				}

				try {
					conn = createConnection(pool, timeout);
				}
				catch (Throwable e) {
					pool.total.getAndDecrement();
					throw e;
				}

				if (cluster.authEnabled) {
					byte[] token = this.sessionToken;

					if (token != null) {
						try {
							if (! AdminCommand.authenticate(cluster, conn, token)) {
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
						catch (Throwable e) {
							closeConnection(conn);
							throw e;
						}
					}
				}

				if (timeout != socketTimeout) {
					// Reset timeout to socketTimeout.
					try {
						conn.setTimeout(socketTimeout);
					}
					catch (Throwable e) {
						closeConnection(conn);
						throw new AerospikeException.Connection(e);
					}
				}

				if (connectTimeout > 0 && cmd != null) {
					// Adjust deadline for socket connect time when connectTimeout defined.
					cmd.resetDeadline(startTime);
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
				"Node " + this + " max connections " + cluster.maxConnsPerNode + " would be exceeded.");
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
	 * Close pooled connection on error and decrement connection count.
	 */
	public final void closeConnection(Connection conn) {
		conn.pool.total.getAndDecrement();
		closeConnectionOnError(conn);
	}

	/**
	 * Close any connection on error.
	 */
	public final void closeConnectionOnError(Connection conn) {
		connsClosed.getAndIncrement();
		incrErrorRate();
		conn.close();
	}

	/**
	 * Close connection without incrementing error count.
	 */
	public final void closeIdleConnection(Connection conn) {
		connsClosed.getAndIncrement();
		conn.close();
	}

	final void balanceConnections() {
		for (Pool pool : connectionPools) {
			int excess = pool.excess();

			if (excess > 0) {
				pool.closeIdle(this, excess);
			}
			else if (excess < 0 && errorRateWithinLimit()) {
				createConnections(pool, -excess);
			}
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
			if (! cluster.isConnCurrentTran(conn.getLastUsed())) {
				closeAsyncIdleConnection(conn, index);
				continue;
			}

			if (! conn.isValid(byteBuffer)) {
				closeAsyncConnection(conn, index);
				continue;
			}
			return conn;
		}

		if (pool.reserve()) {
			return null;
		}

		throw new AerospikeException.Connection(ResultCode.NO_MORE_CONNECTIONS,
			"Max async conns reached: " + this + ',' + index + ',' + pool.total +
			',' + pool.queue.size() + ',' + pool.maxSize);
	}

	public final boolean reserveAsyncConnectionSlot(int index) {
		return asyncConnectionPools[index].reserve();
	}

	public final void connectionOpened(int index) {
		asyncConnectionPools[index].opened++;
	}

	public final boolean putAsyncConnection(AsyncConnection conn, int index) {
		if (conn == null) {
			if (Log.warnEnabled()) {
				Log.warn("Async conn is null: " + this + ',' + index);
			}
			return false;
		}

		AsyncPool pool = asyncConnectionPools[index];

		if (! pool.addFirst(conn)) {
			// This should not happen since connection slots are reserved in advance
			// and total connections should never exceed maxSize. If it does happen,
			// it's highly likely that total count was decremented twice for the same
			// command, causing the connection balancer to create more connections
			// than necessary. Attempt to correct situation by not decrementing total
			// when this excess connection is closed.
			conn.close();
			//connectionClosed();

			if (Log.warnEnabled()) {
				Log.warn("Async conn pool is full: " + this + ',' + index + ',' + pool.total +
						 ',' + pool.queue.size() + ',' + pool.maxSize);
			}
			return false;
		}
		return true;
	}

	/**
	 * Close async connection on error.
	 */
	public final void closeAsyncConnection(AsyncConnection conn, int index) {
		incrErrorRate();
		asyncConnectionPools[index].connectionClosed();
		conn.close();
	}

	/**
	 * Close async connection without incrementing error count.
	 */
	public final void closeAsyncIdleConnection(AsyncConnection conn, int index) {
		asyncConnectionPools[index].connectionClosed();
		conn.close();
	}

	public final void decrAsyncConnection(int index) {
		incrErrorRate();
		asyncConnectionPools[index].total--;
	}

	public final AsyncPool getAsyncPool(int index) {
		return asyncConnectionPools[index];
	}

	public final void balanceAsyncConnections(EventLoop eventLoop) {
		AsyncPool pool = asyncConnectionPools[eventLoop.getIndex()];
		pool.handleRemove();

		int excess = pool.excess();

		if (excess > 0) {
			closeIdleAsyncConnections(pool, excess);
		}
		else if (excess < 0 && errorRateWithinLimit()) {
			// Create connection requests sequentially because they will be done in the
			// background and there is no immediate need for them to complete.
			new AsyncConnectorExecutor(eventLoop, cluster, this, -excess, 1, null, null);
		}
	}

	private final void closeIdleAsyncConnections(AsyncPool pool, int count) {
		ArrayDeque<AsyncConnection> queue = pool.queue;

		// Oldest connection is at end of queue.
		while (count > 0) {
			AsyncConnection conn = queue.peekLast();

			if (conn == null || cluster.isConnCurrentTrim(conn.getLastUsed())) {
				break;
			}

			// Pop connection from queue.
			queue.pollLast();
			pool.connectionClosed();
			conn.close();
			count--;
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

	public final void enableMetrics(MetricsPolicy policy) {
		metrics = new NodeMetrics(policy);
	}

	public final NodeMetrics getMetrics() {
		return metrics;
	}

	/**
	 * Add elapsed time in nanoseconds to latency buckets corresponding to latency type.
	 */
	public final void addLatency(String namespace, LatencyType type, long elapsed) {
		metrics.addLatency(namespace, type, elapsed);
	}

	public final void incrErrorRate() {
		if (cluster.maxErrorRate > 0) {
			errorRateCount.getAndIncrement();
		}
	}

	public final void resetErrorRate() {
		if (isErrorRateValid()) {
			errorRateCount.set(0);
			// Error rate limit was not breached. Next error rate trigger is doubled up to a max of cluster maxErrorRate
			maxErrorRate = Math.min(maxErrorRate * 2, cluster.maxErrorRate);
		}
		else {
			errorRateCount.set(0);
			// Error rate limit was breached. Next error rate trigger is half.
			if (maxErrorRate >= 2) {
				maxErrorRate /= 2;
			}
			else {
				maxErrorRate = 1;
			}
		}
	}

	public final boolean errorRateWithinLimit() {
		return cluster.maxErrorRate <= 0 || errorRateCount.get() <= cluster.maxErrorRate;
	}

	public final void validateErrorCount() {
		if (! errorRateWithinLimit()) {
			throw new AerospikeException.Backoff(ResultCode.MAX_ERROR_RATE);
		}
	}

	/**
	 * Increment transaction error count. If the error is retryable, multiple errors per
	 * transaction may occur.
	 */
	public void addError(String namespace) {
		errorCounter.increment(namespace);
	}

	/**
	 * Increment transaction timeout count. If the timeout is retryable (ie socketTimeout),
	 * multiple timeouts per transaction may occur.
	 */
	public void addTimeout(String namespace) {
		timeoutCounter.increment(namespace);
	}

	/**
	 * Increment the key busy counter.
	 */
	public void addKeyBusy(String namespace) {
		keyBusyCounter.increment(namespace);
	}

	/**
	 * Add to the count of bytes sent to the node.
	 */
	public void addBytesOut(String namespace, long count) {
		metrics.bytesOutCounter.increment(namespace, count);
	}

	/**
	 * Add to the count of bytes received from the node.
	 */
	public void addBytesIn(String namespace, long count) {
		metrics.bytesInCounter.increment(namespace, count);
	}

	/**
	 * Return error count. The value is cumulative and not reset per metrics interval.
	 */
	public long getErrorCount() {
        return errorCounter.getTotal();
	}

	/**
	 * Return error count by namespace. The value is cumulative and not reset per metrics interval.
	 */
	public long getErrorCountByNS(String namespace) {
		return errorCounter.getCountByNS(namespace);
	}

	/**
	 * Return timeout count. The value is cumulative and not reset per metrics interval.
	 */
	public long getTimeoutCount() {
        return timeoutCounter.getTotal();
	}

	/**
	 * Return timeout count. The value is cumulative and not reset per metrics interval.
	 */
	public long getTimeoutCountbyNS(String namespace) {
		return timeoutCounter.getCountByNS(namespace);
	}

	/**
	 * Return key busy count. The value is cumulative and not reset per metrics interval.
	 */
	public long getKeyBusyCount() {
		return keyBusyCounter.getTotal();
	}

	/**
	 * Return key busy count for a given namespace. The value is cumulative and not reset per metrics interval.
	 */
	public long getKeyBusyCountByNS(String namespace) {
		return keyBusyCounter.getCountByNS(namespace);
	}

	/**
	 * Return count of bytes in by namespace. The value is cumulative and not reset per metrics interval.
	 */
	public long getBytesInByNS(String namespace) {
		return metrics.bytesInCounter.getCountByNS(namespace);
	}

	/**
	 * Return count of bytes out by namespace. The value is cumulative and not reset per metrics interval.
	 */
	public long getBytesOutByNS(String namespace) {
		return metrics.bytesOutCounter.getCountByNS(namespace);
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
	 * Return current generation of cluster peers.
	 */
	public final int getPeersGeneration() {
		return peersGeneration;
	}

	/**
	 * Return current generation of partition maps.
	 */
	public final int getPartitionGeneration() {
		return partitionGeneration;
	}

	/**
	 * Return current generation of racks.
	 */
	public final int getRebalanceGeneration() {
		return rebalanceGeneration;
	}

	/**
	 * Return this node's build version
	 */
	public Version getServerVersion() {
		return serverVersion;
	}

	/**
	 * Return metrics enablement status
	 */
	public boolean areMetricsEnabled() { return cluster.metricsEnabled;}

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

	public final boolean hasQueryShow() {
		return (features & HAS_QUERY_SHOW) != 0;
	}

	public final boolean hasBatchAny() {
		return (features & HAS_BATCH_ANY) != 0;
	}

	public final boolean hasPartitionQuery() {
		return (features & HAS_PARTITION_QUERY) != 0;
	}

	/**
	 * Does server support partition scans.
	 */
	/*
	public final boolean hasPartitionScan() {
		return (features & HAS_PARTITION_SCAN) != 0;
	}
	*/

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
		pool.closeConnections();
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

	public static final class AsyncPool {
		public final ArrayDeque<AsyncConnection> queue;
		public final int minSize;
		public final int maxSize;
		public int total;
		public int opened;
		public int closed;
		private boolean shouldRemove;

		private AsyncPool(int minSize, int maxSize) {
			this.minSize = minSize;
			this.maxSize = maxSize;
			this.queue = new ArrayDeque<AsyncConnection>(maxSize);
		}

		private boolean reserve() {
			if (total >= maxSize || queue.size() >= maxSize) {
				return false;
			}
			total++;
			return true;
		}

		private boolean addFirst(AsyncConnection conn) {
			if (queue.size() < maxSize) {
				queue.addFirst(conn);
				return true;
			}
			return false;
		}

		private int excess() {
			return total - minSize;
		}

		private void connectionClosed() {
			total--;
			closed++;
		}

		public void signalRemove() {
			shouldRemove = true;
		}

		public void handleRemove() {
			if (shouldRemove) {
				shouldRemove = false;
				removeClosed();
			}
		}

		public void removeClosed() {
			// Remove all closed netty connections in the queue.
			NettyConnection first = (NettyConnection)queue.pollFirst();
			NettyConnection conn = first;

			do {
				if (conn == null) {
					break;
				}

				if (conn.isOpen()) {
					queue.addLast(conn);
				}
				else {
					connectionClosed();
				}

				conn = (NettyConnection)queue.pollFirst();
			} while (conn != first);
		}

		public void closeConnections() {
			AsyncConnection conn;

			while ((conn = queue.pollFirst()) != null) {
				conn.close();
			}
		}
	}
}

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.admin.AdminCommand;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoopStats;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventState;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.async.NettyTlsContext;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.cluster.Node.AsyncPool;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.configuration.ConfigurationProvider;
import com.aerospike.client.configuration.YamlConfigProvider;
import com.aerospike.client.configuration.serializers.Configuration;
import com.aerospike.client.configuration.serializers.StaticConfiguration;
import com.aerospike.client.configuration.serializers.staticconfig.StaticClientConfig;
import com.aerospike.client.listener.ClusterStatsListener;
import com.aerospike.client.metrics.MetricsListener;
import com.aerospike.client.metrics.MetricsPolicy;
import com.aerospike.client.metrics.MetricsWriter;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TCPKeepAlive;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.ThreadLocalData;
import com.aerospike.client.util.Util;

public class Cluster implements Runnable, Closeable {
	private final static long MAX_SOCKET_IDLE_TRIM_DEFAULT_SECS = 55;
	private final static int DEFAULT_CONFIG_INTERVAL_MS = 5_000;
	private final static int TEND_INTERVAL_MIN_MS = 250;

	// Client back pointer.
	public final AerospikeClient client;

	// Config created from a ConfigProvider, if available
	private Configuration config;

	// Expected cluster name.
	protected final String clusterName;

	// Initial host nodes specified by user.
	private volatile Host[] seeds;

	// Map of active nodes in cluster.
	// Only accessed within cluster tend thread.
	protected final HashMap<String,Node> nodesMap;

	// Active nodes in cluster.
	private volatile Node[] nodes;

	// Hints for best node for a partition
	public volatile HashMap<String,Partitions> partitionMap;

	// IP translations.
	protected final Map<String,String> ipMap;

	// TLS connection policy.
	public final TlsPolicy tlsPolicy;

	// Netty TLS context.
	public final NettyTlsContext nettyTlsContext;

	// Authentication mode.
	public final AuthMode authMode;

	// User name in UTF-8 encoded bytes.
	protected final byte[] user;

	// Password in UTF-8 encoded bytes.
	private byte[] password;

	// Password in hashed format in bytes.
	private byte[] passwordHash;

	// Random node index counter.
	private final AtomicInteger nodeIndex;

	// Random partition replica index counter.
	final AtomicInteger replicaIndex;

	// Count of connections in recover queue.
	private final AtomicInteger recoverCount;

	// Thread-safe queue of sync connections to be recovered.
	private final ConcurrentLinkedDeque<ConnectionRecover> recoverQueue;

	// Thread factory used in synchronous batch, scan and query commands.
	public final ThreadFactory threadFactory;

	// TCP keep-alive configuration. Only used in native netty epoll library.
	public final TCPKeepAlive keepAlive;

	// Optional event loops for async mode.
	public final EventLoops eventLoops;

	// Extra event loop state for this cluster.
	public final EventState[] eventState;

	// Maximum socket idle to validate connections in command.
	private long maxSocketIdleNanosTran;

	// Maximum socket idle to trim peak connections to min connections.
	private long maxSocketIdleNanosTrim;

	// Minimum sync connections per node.
	protected int minConnsPerNode;

	// Maximum sync connections per node.
	protected int maxConnsPerNode;

	// Minimum async connections per node.
	protected final int asyncMinConnsPerNode;

	// Maximum async connections per node.
	protected final int asyncMaxConnsPerNode;

	// Sync connection pools per node.
	protected final int connPoolsPerNode;

	// Max errors per node per errorRateWindow.
	int maxErrorRate;

	// Number of tend iterations defining window for maxErrorRate.
	int errorRateWindow;

	// Initial connection timeout.
	public int connectTimeout;

	// Login timeout.
	public int loginTimeout;

	// Cluster close timeout.
	public final int closeTimeout;

	// Rack id.
	public int[] rackIds;

	// Count of add node failures in the most recent cluster tend iteration.
	private volatile int invalidNodeCount;

	// Interval in milliseconds between cluster tends.
	private int tendInterval;

	// Cluster tend counter
	private int tendCount;

	// Milliseconds between dynamic configuration check for file modifications.
	public final int configInterval;

	// Dynamic configuration path. If not null, dynamic configuration is enabled.
	private final String configPath;

	// Has cluster instance been closed.
	private AtomicBoolean closed;

	// Tend thread variables.
	private Thread tendThread;
	protected volatile boolean tendValid;

	// Should use "services-alternate" instead of "services" in info request?
	protected boolean useServicesAlternate;

	// Request server rack ids.
	boolean rackAware;

	// Verify clusterName if populated.
	public final boolean validateClusterName;

	// Is authentication enabled
	public final boolean authEnabled;

	// Does cluster support query by partition.
	public boolean hasPartitionQuery;

	private boolean asyncComplete;

	public boolean metricsEnabled;
	MetricsPolicy metricsPolicy;
	private volatile MetricsListener metricsListener;
	private final Object metricsLock = new Object();
	private final AtomicLong retryCount = new AtomicLong();
	private final AtomicLong commandCount = new AtomicLong();
	private final AtomicLong delayQueueTimeoutCount = new AtomicLong();

	public Cluster(AerospikeClient client, ClientPolicy policy, String configPath, Host[] hosts) {
		this.client = client;
		this.configPath = configPath;
		this.clusterName = policy.clusterName;
		this.validateClusterName = policy.validateClusterName;
		this.tlsPolicy = policy.tlsPolicy;
		this.authMode = policy.authMode;

		// Default TLS names when TLS enabled.
		if (tlsPolicy != null) {
			boolean useClusterName = clusterName != null && clusterName.length() > 0;

			for (int i = 0; i < hosts.length; i++) {
				Host host = hosts[i];

				if (host.tlsName == null) {
					String tlsName = useClusterName ? clusterName : host.name;
					hosts[i] = new Host(host.name, tlsName, host.port);
				}
			}
		}
		else {
			if (authMode == AuthMode.EXTERNAL || authMode == AuthMode.PKI) {
				throw new AerospikeException("TLS is required for authentication mode: " + authMode);
			}
		}

		this.seeds = hosts;

		if (policy.authMode == AuthMode.PKI) {
			if (policy.password != null) {
				throw new AerospikeException("Password authentication is disabled for PKI-only users");
			}
			this.authEnabled = true;
			this.user = null;
		}
		else if (policy.user != null && policy.user.length() > 0) {
			this.authEnabled = true;
			this.user = Buffer.stringToUtf8(policy.user);

			// Only store clear text password if external authentication is used.
			if (authMode != AuthMode.INTERNAL) {
				this.password = Buffer.stringToUtf8(policy.password);
			}

			String pass = policy.password;

			if (pass == null)
			{
				pass = "";
			}

			pass = AdminCommand.hashPassword(pass);
			this.passwordHash = Buffer.stringToUtf8(pass);
		}
		else {
			this.authEnabled = false;
			this.user = null;
		}

		minConnsPerNode = policy.minConnsPerNode;
		maxConnsPerNode = policy.maxConnsPerNode;

		if (minConnsPerNode > maxConnsPerNode) {
			throw new AerospikeException("Invalid connection range: " + minConnsPerNode + " - " +  maxConnsPerNode);
		}

		asyncMinConnsPerNode = policy.asyncMinConnsPerNode;
		asyncMaxConnsPerNode = (policy.asyncMaxConnsPerNode >= 0)? policy.asyncMaxConnsPerNode : policy.maxConnsPerNode;

		if (asyncMinConnsPerNode > asyncMaxConnsPerNode) {
			throw new AerospikeException("Invalid async connection range: " + asyncMinConnsPerNode + " - " +  asyncMaxConnsPerNode);
		}

		connPoolsPerNode = policy.connPoolsPerNode;

		int configIntervalDuration = DEFAULT_CONFIG_INTERVAL_MS;

		if (client.getConfigProvider() != null) {
			config = client.getConfigProvider().fetchConfiguration();
			if (config != null) {
				StaticConfiguration sConfig = config.getStaticConfiguration();
				if (sConfig != null) {
					StaticClientConfig staCC = sConfig.getStaticClientConfig();
					if (staCC != null && staCC.configInterval != null) {
						configIntervalDuration = staCC.configInterval.value;
					}
				}
			}
		}
		this.configInterval = configIntervalDuration;

		applyCommonClientPolicyParameters(policy, true);

		closeTimeout = policy.closeTimeout;
		ipMap = policy.ipMap;
		keepAlive = policy.keepAlive;
		threadFactory = Thread.ofVirtual().name("Aerospike-", 0L).factory();

		nodesMap = new HashMap<String,Node>();
		nodes = new Node[0];
		partitionMap = new HashMap<String,Partitions>();
		nodeIndex = new AtomicInteger();
		replicaIndex = new AtomicInteger();
		recoverCount = new AtomicInteger();
		recoverQueue = new ConcurrentLinkedDeque<ConnectionRecover>();
		closed = new AtomicBoolean();

		eventLoops = policy.eventLoops;

		if (eventLoops != null) {
			EventLoop[] loops = eventLoops.getArray();

			if (asyncMaxConnsPerNode < loops.length) {
				throw new AerospikeException("asyncMaxConnsPerNode " + asyncMaxConnsPerNode +
					" must be >= event loop count " + loops.length);
			}

			eventState = new EventState[loops.length];

			for (int i = 0; i < loops.length; i++) {
				eventState[i] = loops[i].createState();
			}

			if (policy.tlsPolicy != null) {
				if (eventLoops instanceof NioEventLoops) {
					throw new AerospikeException("TLS not supported in direct NIO event loops");
				}

				if (policy.tlsPolicy.nettyContext != null) {
					nettyTlsContext = policy.tlsPolicy.nettyContext;
				}
				else {
					nettyTlsContext = new NettyTlsContext(policy.tlsPolicy);
				}
			}
			else {
				nettyTlsContext = null;
			}
		}
		else {
			eventState = null;
			nettyTlsContext = null;
		}

		if (policy.forceSingleNode) {
			// Communicate with the first seed node only.
			// Do not run cluster tend thread.
			try {
				forceSingleNode();
			}
			catch (Throwable e) {
				close();
				throw e;
			}
		}
		else {
			initTendThread(policy.failIfNotConnected);
		}
	}

	/**
	 * A common place to apply certain Client Policy parameters. The allowed parameters for dynamic client config
	 * dictate which Client Policy parameters can be applied here.
	 */
	private void applyCommonClientPolicyParameters(ClientPolicy clientPolicy, boolean init) {
		if (clientPolicy.tendInterval < TEND_INTERVAL_MIN_MS) {
			throw new AerospikeException("Invalid tendInterval: " + clientPolicy.tendInterval + ". min: " +
				TEND_INTERVAL_MIN_MS);
		}

		if (configInterval < clientPolicy.tendInterval) {
			throw new AerospikeException("Dynamic config interval " + configInterval +
				" must be greater or equal to the tend interval " + clientPolicy.tendInterval);
		}

		tendInterval = clientPolicy.tendInterval;
		connectTimeout = clientPolicy.timeout;
		loginTimeout = clientPolicy.loginTimeout;

		if (clientPolicy.maxSocketIdle < 0) {
			throw new AerospikeException("Invalid maxSocketIdle: " + clientPolicy.maxSocketIdle);
		}
		if (clientPolicy.maxSocketIdle == 0) {
			maxSocketIdleNanosTran = 0;
			maxSocketIdleNanosTrim = TimeUnit.SECONDS.toNanos(MAX_SOCKET_IDLE_TRIM_DEFAULT_SECS);
		}
		else {
			maxSocketIdleNanosTran = TimeUnit.SECONDS.toNanos(clientPolicy.maxSocketIdle);
			maxSocketIdleNanosTrim = maxSocketIdleNanosTran;
		}

		useServicesAlternate = clientPolicy.useServicesAlternate;
		rackAware = clientPolicy.rackAware;

		errorRateWindow = clientPolicy.errorRateWindow;
		if (maxErrorRate != clientPolicy.maxErrorRate) {
			maxErrorRate = clientPolicy.maxErrorRate;
			if (!init) {
				for (Node node : nodes) {
					node.maxErrorRate = maxErrorRate;
				}
			}
		}

		if (init || !Util.rackIdsEqual(clientPolicy.rackIds, this.rackIds)) {
			int[] rackIdsTemp;

			if (clientPolicy.rackIds != null && !clientPolicy.rackIds.isEmpty()) {
				List<Integer> list = clientPolicy.rackIds;
				int max = list.size();
				rackIdsTemp = new int[max];

				for (int i = 0; i < max; i++) {
					rackIdsTemp[i] = list.get(i);
				}
			}
			else {
				rackIdsTemp = new int[] {clientPolicy.rackId};
			}

			this.rackIds = rackIdsTemp;

			if (!init) {
				for (Node node : nodes) {
					if (rackAware && node.racks == null) {
						node.racks = new HashMap<>();
					}
					else if (!rackAware && node.racks != null) {
						node.racks = null;
					}
				}
			}
		}
	}

	public void forceSingleNode() {
		// Initialize tendThread, but do not start it.
		tendValid = true;
		tendThread = new Thread(this);

		// Validate first seed.
		Host seed = seeds[0];
		NodeValidator nv = new NodeValidator();
		Node node = null;

		try {
			node = nv.seedNode(this, seed, null);
		}
		catch (Throwable e) {
			throw new AerospikeException("Seed " + seed + " failed: " + e.getMessage(), e);
		}

		node.createMinConnections();

		// Add seed node to nodes.
		HashMap<String,Node> nodesToAdd = new HashMap<String,Node>(1);
		nodesToAdd.put(node.getName(), node);
		addNodes(nodesToAdd);

		// Initialize partitionMaps.
		Peers peers = new Peers(nodes.length + 16);
		node.refreshPartitions(peers);

		// Set partition maps for all namespaces to point to same node.
		for (Partitions partitions : partitionMap.values()) {
			for (AtomicReferenceArray<Node> nodeArray : partitions.replicas) {
				int max = nodeArray.length();

				for (int i = 0; i < max; i++) {
					nodeArray.set(i, node);
				}
			}
		}
	}

	public void initTendThread(boolean failIfNotConnected) {
		// Tend cluster until all nodes identified.
		waitTillStabilized(failIfNotConnected);

		if (Log.debugEnabled()) {
			for (Host host : seeds) {
				Log.debug("Add seed " + host);
			}
		}

		// Add other nodes as seeds, if they don't already exist.
		ArrayList<Host> seedsToAdd = new ArrayList<Host>(nodes.length);
		for (Node node : nodes) {
			Host host = node.getHost();
			if (! findSeed(host)) {
				seedsToAdd.add(host);
			}
		}

		if (seedsToAdd.size() > 0) {
			addSeeds(seedsToAdd.toArray(new Host[seedsToAdd.size()]));
		}

		if (config != null && config.hasMetrics() && config.dynamicConfiguration.dynamicMetricsConfig.enable != null &&
			config.dynamicConfiguration.dynamicMetricsConfig.enable.value) {
			synchronized(metricsLock) {
				enableMetricsInternal(metricsPolicy);
			}
		}

		// Run cluster tend thread.
		tendValid = true;
		tendThread = new Thread(this);
		tendThread.setName("tend");
		tendThread.setDaemon(true);
		tendThread.start();
	}

	public final void addSeeds(Host[] hosts) {
		// Use copy on write semantics.
		Host[] seedArray = new Host[seeds.length + hosts.length];
		int count = 0;

		// Add existing seeds.
		for (Host seed : seeds) {
			seedArray[count++] = seed;
		}

		// Add new seeds
		for (Host host : hosts) {
			if (Log.debugEnabled()) {
				Log.debug("Add seed " + host);
			}
			seedArray[count++] = host;
		}

		// Replace nodes with copy.
		seeds = seedArray;
	}

	private final boolean findSeed(Host search) {
		for (Host seed : seeds) {
			if (seed.equals(search)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Tend the cluster until it has stabilized and return control.
	 * This helps avoid initial database request timeout issues when
	 * a large number of threads are initiated at client startup.
	 */
	private final void waitTillStabilized(boolean failIfNotConnected) {
		// Tend now requests partition maps in same iteration as the nodes
		// are added, so there is no need to call tend twice anymore.
		tend(failIfNotConnected, true);

		if (nodes.length == 0) {
			String message = "Cluster seed(s) failed";

			if (failIfNotConnected) {
				throw new AerospikeException(message);
			}
			else {
				Log.warn(message);
			}
		}
	}

	public final void run() {
		while (tendValid) {
			// Tend cluster.
			try {
				tend(false, false);
			}
			catch (Throwable e) {
				if (Log.warnEnabled()) {
					Log.warn("Cluster tend failed: " + Util.getErrorMessage(e));
				}
			}
			// Sleep between polling intervals.
			Util.sleep(tendInterval);
		}
	}

	/**
	 * Check health of all nodes in the cluster.
	 */
	private final void tend(boolean failIfNotConnected, boolean isInit) {
		// All node additions/deletions are performed in tend thread.
		// Initialize tend iteration node statistics.
		Peers peers = new Peers(nodes.length + 16);

		// Clear node reference counts.
		for (Node node : nodes) {
			node.referenceCount = 0;
			node.partitionChanged = false;
			node.rebalanceChanged = false;
		}

		// If active nodes don't exist, seed cluster.
		if (nodes.length == 0) {
			seedNode(peers, failIfNotConnected);

			// Abort cluster init if all peers of the seed are not reachable and failIfNotConnected is true.
			if (isInit && failIfNotConnected && nodes.length == 1 && peers.getInvalidCount() > 0) {
				peers.clusterInitError();
			}
		}
		else {
			// Refresh all known nodes.
			for (Node node : nodes) {
				node.refresh(peers);
			}

			// Refresh peers when necessary.
			if (peers.genChanged) {
				// Refresh peers for all nodes that responded the first time even if only one node's peers changed.
				peers.refreshCount = 0;

				for (Node node : nodes) {
					node.refreshPeers(peers);
				}

				// Handle nodes changes determined from refreshes.
				findNodesToRemove(peers);

				// Remove nodes in a batch.
				if (peers.removeNodes.size() > 0) {
					removeNodes(peers.removeNodes);
				}
			}

			// Add peer nodes to cluster.
			if (peers.nodes.size() > 0) {
				addNodes(peers.nodes);
				refreshPeers(peers);
			}
		}

		invalidNodeCount += peers.getInvalidCount();

		// Refresh partition map when necessary.
		for (Node node : nodes) {
			if (node.partitionChanged) {
				node.refreshPartitions(peers);
			}

			if (node.rebalanceChanged) {
				node.refreshRacks();
			}
		}

		tendCount++;

		// Balance connections every 30 tend iterations.
		if (tendCount % 30 == 0) {
			for (Node node : nodes) {
				node.balanceConnections();
			}

			if (eventState != null) {
				for (EventState es : eventState) {
					final EventLoop eventLoop = es.eventLoop;

					eventLoop.execute(new Runnable() {
						public void run() {
							try {
								final Node[] nodeArray = nodes;

								for (Node node : nodeArray) {
									node.balanceAsyncConnections(eventLoop);
								}
							}
							catch (Throwable e) {
								if (Log.warnEnabled()) {
									Log.warn("balanceAsyncConnections failed: " + Util.getErrorMessage(e));
								}
							}
						}
					});
				}
			}
		}

		// Reset connection error window for all nodes every connErrorWindow tend iterations.
		if (tendCount % errorRateWindow == 0) {
			for (Node node : nodes) {
				node.resetErrorRate();
			}
		}

		// Perform metrics snapshot.
		synchronized(metricsLock) {
			if (metricsEnabled && (tendCount % metricsPolicy.interval) == 0) {
				metricsListener.onSnapshot(this);
			}
		}

		// Convert config interval from a millisecond duration to the number of cluster tend
		// iterations.
		int interval = configInterval / tendInterval;

		// Check configuration file for updates.
		if (configPath != null && tendCount % interval == 0) {
			try {
				loadConfiguration();
			}
			catch (Throwable t) {
				if (Log.warnEnabled()) {
					Log.warn("Dynamic configuration failed: " + t);
				}
			}
		}

		processRecoverQueue();
	}

	private final boolean seedNode(Peers peers, boolean failIfNotConnected) {
		// Must copy array reference for copy on write semantics to work.
		Host[] seedArray = seeds;
		Throwable[] exceptions = null;
		NodeValidator nv = new NodeValidator();

		for (int i = 0; i < seedArray.length; i++) {
			Host seed = seedArray[i];

			try {
				Node node = nv.seedNode(this, seed, peers);

				if (node != null) {
					addSeedAndPeers(node, peers);
					return true;
				}
			}
			catch (Throwable e) {
				peers.fail(seed);

				if (seed.tlsName != null && tlsPolicy == null) {
					// Fail immediately for known configuration errors like this.
					throw new AerospikeException.Connection("Seed host tlsName '" + seed.tlsName +
						"' defined but client tlsPolicy not enabled", e);
				}

				// Store exception and try next seed.
				if (failIfNotConnected) {
					if (exceptions == null) {
						exceptions = new Exception[seedArray.length];
					}
					exceptions[i] = e;
				}
				else {
					if (Log.warnEnabled()) {
						Log.warn("Seed " + seed + " failed: " + Util.getErrorMessage(e));
					}
				}
			}
		}

		// No seeds valid. Use fallback node if it exists.
		if (nv.fallback != null) {
			// When a fallback is used, peers refreshCount is reset to zero.
			// refreshCount should always be one at this point.
			peers.refreshCount = 1;
			addSeedAndPeers(nv.fallback, peers);
			return true;
		}

		if (failIfNotConnected) {
			StringBuilder sb = new StringBuilder(500);
			sb.append("Failed to connect to ["+ seedArray.length +"] host(s): ");
			sb.append(System.lineSeparator());

			for (int i = 0; i < seedArray.length; i++) {
				sb.append(seedArray[i]);
				sb.append(' ');

				Throwable ex = exceptions == null ? null : exceptions[i];

				if (ex != null) {
					sb.append(ex.getMessage());
					sb.append(System.lineSeparator());
				}
			}
			throw new AerospikeException.Connection(sb.toString());
		}
		return false;
	}

	private void addSeedAndPeers(Node seed, Peers peers) {
		seed.createMinConnections();
		nodesMap.clear();

		addNodes(seed, peers);

		if (peers.nodes.size() > 0) {
			refreshPeers(peers);
		}
	}

	private void refreshPeers(Peers peers) {
		// Iterate until peers have been refreshed and all new peers added.
		while (true) {
			// Copy peer node references to array.
			Node[] nodeArray = new Node[peers.nodes.size()];
			int count = 0;

			for (Node node : peers.nodes.values()) {
				nodeArray[count++] = node;
			}

			// Reset peer nodes.
			peers.nodes.clear();

			// Refresh peers of peers in order retrieve the node's peersCount
			// which is used in RefreshPartitions(). This call might add even
			// more peers.
			for (Node node : nodeArray) {
				node.refreshPeers(peers);
			}

			if (peers.nodes.size() > 0) {
				// Add new peer nodes to cluster.
				addNodes(peers.nodes);
			}
			else {
				break;
			}
		}
	}

	protected Node createNode(NodeValidator nv) {
		Node node = new Node(this, nv);
		node.createMinConnections();
		return node;
	}

	private final void findNodesToRemove(Peers peers) {
		int refreshCount = peers.refreshCount;
		HashSet<Node> removeNodes = peers.removeNodes;

		for (Node node : nodes) {
			if (! node.isActive()) {
				// Inactive nodes must be removed.
				removeNodes.add(node);
				continue;
			}

			if (refreshCount == 0 && node.failures >= 5) {
				// All node info requests failed and this node had 5 consecutive failures.
				// Remove node.  If no nodes are left, seeds will be tried in next cluster
				// tend iteration.
				removeNodes.add(node);
				continue;
			}

			if (nodes.length > 1 && refreshCount >= 1 && node.referenceCount == 0) {
				// Node is not referenced by other nodes.
				// Check if node responded to info request.
				if (node.failures == 0) {
					// Node is alive, but not referenced by other nodes.  Check if mapped.
					if (! findNodeInPartitionMap(node)) {
						// Node doesn't have any partitions mapped to it.
						// There is no point in keeping it in the cluster.
						removeNodes.add(node);
					}
				}
				else {
					// Node not responding. Remove it.
					removeNodes.add(node);
				}
			}
		}
	}

	private final boolean findNodeInPartitionMap(Node filter) {
		for (Partitions partitions : partitionMap.values()) {
			for (AtomicReferenceArray<Node> nodeArray : partitions.replicas) {
				int max = nodeArray.length();

				for (int i = 0; i < max; i++) {
					Node node = nodeArray.get(i);
					// Use reference equality for performance.
					if (node == filter) {
						return true;
					}
				}
			}
		}
		return false;
	}

	private void addNodes(Node seed, Peers peers) {
		// Add all nodes at once to avoid copying entire array multiple times.
		// Create temporary nodes array.
		Node[] nodeArray = new Node[peers.nodes.size() + 1];
		int count = 0;

		// Add seed.
		nodeArray[count++] = seed;
		addNode(seed);

		// Add peers.
		for (Node peer : peers.nodes.values()) {
			nodeArray[count++] = peer;
			addNode(peer);
		}
		hasPartitionQuery = Cluster.supportsPartitionQuery(nodeArray);

		// Replace nodes with copy.
		nodes = nodeArray;
	}

	/**
	 * Add nodes using copy on write semantics.
	 */
	private final void addNodes(HashMap<String,Node> nodesToAdd) {
		// Add all nodes at once to avoid copying entire array multiple times.
		// Create temporary nodes array.
		Node[] nodeArray = new Node[nodes.length + nodesToAdd.size()];
		int count = 0;

		// Add existing nodes.
		for (Node node : nodes) {
			nodeArray[count++] = node;
		}

		// Add new nodes.
		for (Node node : nodesToAdd.values()) {
			nodeArray[count++] = node;
			addNode(node);
		}
		hasPartitionQuery = Cluster.supportsPartitionQuery(nodeArray);

		// Replace nodes with copy.
		nodes = nodeArray;
	}

	private final void addNode(Node node) {
		if (Log.infoEnabled()) {
			Log.info("Add node " + node);
		}

		nodesMap.put(node.getName(), node);
	}

	private final void removeNodes(HashSet<Node> nodesToRemove) {
		// There is no need to delete nodes from partitionWriteMap because the nodes
		// have already been set to inactive. Further connection requests will result
		// in an exception and a different node will be tried.

		// Cleanup node resources.
		for (Node node : nodesToRemove) {
			// Remove node from map.
			nodesMap.remove(node.getName());

			synchronized(metricsLock) {
				if (metricsEnabled) {
					// Flush node metrics before removal.
					try {
						metricsListener.onNodeClose(node);
					}
					catch (Throwable e) {
						Log.warn("Write metrics failed on " + node + ": " + Util.getErrorMessage(e));
					}
				}
			}
			node.close();
		}

		// Remove all nodes at once to avoid copying entire array multiple times.
		removeNodesCopy(nodesToRemove);
	}

	/**
	 * Remove nodes using copy on write semantics.
	 */
	private final void removeNodesCopy(HashSet<Node> nodesToRemove) {
		// Create temporary nodes array.
		// Since nodes are only marked for deletion using node references in the nodes array,
		// and the tend thread is the only thread modifying nodes, we are guaranteed that nodes
		// in nodesToRemove exist.  Therefore, we know the final array size.
		Node[] nodeArray = new Node[nodes.length - nodesToRemove.size()];
		int count = 0;

		// Add nodes that are not in remove list.
		for (Node node : nodes) {
			if (nodesToRemove.contains(node)) {
				if (Log.infoEnabled()) {
					Log.info("Remove node " + node);
				}
			}
			else {
				nodeArray[count++] = node;
			}
		}

		// Do sanity check to make sure assumptions are correct.
		if (count < nodeArray.length) {
			if (Log.warnEnabled()) {
				Log.warn("Node remove mismatch. Expected " + nodeArray.length + " Received " + count);
			}
			// Resize array.
			Node[] nodeArray2 = new Node[count];
			System.arraycopy(nodeArray, 0, nodeArray2, 0, count);
			nodeArray = nodeArray2;
		}
		hasPartitionQuery = Cluster.supportsPartitionQuery(nodeArray);

		// Replace nodes with copy.
		nodes = nodeArray;
	}

	public final boolean isConnected() {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;

		if (nodeArray.length > 0 && tendValid) {
			// Even though nodes exist, they may not be currently responding.  Check further.
			for (Node node : nodeArray) {
				// Mark connected if any node is active and cluster tend consecutive info request
				// failures are less than 5.
				if (node.active && node.failures < 5) {
					return true;
				}
			}
		}
		return false;
	}

	public final Node getRandomNode() throws AerospikeException.InvalidNode {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;

		if (nodeArray.length > 0) {
			int index = Math.abs(nodeIndex.getAndIncrement() % nodeArray.length);

			for (int i = 0; i < nodeArray.length; i++) {
				Node node = nodeArray[index];

				if (node.isActive()) {
					return node;
				}
				index++;
				index %= nodeArray.length;
			}
		}
		throw new AerospikeException.InvalidNode("Cluster is empty");
	}

	public final Node[] getNodes() {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;
		return nodeArray;
	}

	public final Node[] validateNodes() {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;

		if (nodeArray.length == 0) {
			throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Cluster is empty");
		}
		return nodeArray;
	}

	public final Node getNode(String nodeName) throws AerospikeException.InvalidNode {
		Node node = findNode(nodeName);

		if (node == null) {
			throw new AerospikeException.InvalidNode("Invalid node name: " + nodeName);
		}
		return node;
	}

	protected final Node findNode(String nodeName) {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;

		for (Node node : nodeArray) {
			if (node.getName().equals(nodeName)) {
				return node;
			}
		}
		return null;
	}

	public final boolean isConnCurrentTran(long lastUsed) {
		return maxSocketIdleNanosTran == 0 || (System.nanoTime() - lastUsed) <= maxSocketIdleNanosTran;
	}

	public final boolean isConnCurrentTrim(long lastUsed) {
		return (System.nanoTime() - lastUsed) <= maxSocketIdleNanosTrim;
	}

	public final void recoverConnection(ConnectionRecover cs) {
		// Many cloud providers encounter performance problems when sockets are
		// closed by the client when the server still has data left to write.
		// The solution is to shutdown the socket and give the server time to
		// respond before closing the socket.
		//
		// Put connection on a queue for later closing.
		if (cs.isComplete()) {
			return;
		}

		// Do not let queue get out of control.
		if (recoverCount.getAndIncrement() < 10000) {
			recoverQueue.offerLast(cs);
		}
		else {
			recoverCount.getAndDecrement();
			cs.abort();
		}
	}

	private void processRecoverQueue() {
		ConnectionRecover last = recoverQueue.peekLast();

		if (last == null) {
			return;
		}

		// Thread local can be used here because this method
		// is only called from the cluster tend thread.
		byte[] buf = ThreadLocalData.getBuffer();
		ConnectionRecover cs;

		while ((cs = recoverQueue.pollFirst()) != null) {
			if (cs.drain(buf)) {
				recoverCount.getAndDecrement();
			}
			else {
				recoverQueue.offerLast(cs);
			}

			if (cs == last) {
				break;
			}
		}
	}

	private void loadConfiguration() {
		ConfigurationProvider provider = client.getConfigProvider();

		if (provider == null) {
			provider = YamlConfigProvider.getConfigProvider(configPath);

			if (provider == null) {
				// Failed to read configuration file. Warning was already logged.
				return;
			}

			client.setConfigProvider(provider);
		}
		else {
			if (!provider.loadConfiguration()) {
				return;
			}
		}

		config = provider.fetchConfiguration();
		client.mergePoliciesWithConfig();
		applyCommonClientPolicyParameters(client.getClientPolicy(), false);

		synchronized(metricsLock) {
			metricsPolicy = mergeMetricsPolicyWithConfig(metricsPolicy);

			if (metricsEnabled && metricsPolicy.isMetricsRestartRequired()) {
				disableMetricsInternal();
				enableMetricsInternal(metricsPolicy);
				metricsPolicy.setMetricsRestartRequired(false);
				return;
			}

			if (config != null && config.hasMetrics() &&
					config.dynamicConfiguration.dynamicMetricsConfig.enable != null) {
				if (!metricsEnabled && config.dynamicConfiguration.dynamicMetricsConfig.enable.value) {
					enableMetricsInternal(metricsPolicy);
				}
				else if (metricsEnabled && !config.dynamicConfiguration.dynamicMetricsConfig.enable.value) {
					disableMetricsInternal();
				}
			}
		}
	}

	private MetricsPolicy mergeMetricsPolicyWithConfig(MetricsPolicy mp) {
		if (mp == null) {
			mp = new MetricsPolicy();
		}
        return new MetricsPolicy(mp, config, metricsEnabled);
	}

	public final void enableMetrics(MetricsPolicy policy) {
		if (config != null) {
			if (config.hasMetrics() && config.dynamicConfiguration.dynamicMetricsConfig.enable != null) {
				if (!config.dynamicConfiguration.dynamicMetricsConfig.enable.value) {
					Log.warn("When a config exists, metrics can not be enabled via enableMetrics unless they" +
							" are enabled in the config provider.");
					this.metricsPolicy = mergeMetricsPolicyWithConfig(policy);
					return;
				}
			}
		}

		synchronized(metricsLock) {
			enableMetricsInternal(policy);
		}
	}

	private void enableMetricsInternal(MetricsPolicy policy) {
		MetricsPolicy mergedMP = mergeMetricsPolicyWithConfig(policy);
		MetricsListener listener = mergedMP.listener;

		if (listener == null) {
			listener = new MetricsWriter(mergedMP.reportDir);
		}

		this.metricsListener = listener;
		this.metricsPolicy = mergedMP;

		if (metricsEnabled) {
			this.metricsListener.onDisable(this);
		}

		Node[] nodeArray = nodes;

		for (Node node : nodeArray) {
			node.enableMetrics(this.metricsPolicy);
		}

		this.metricsListener.onEnable(this, this.metricsPolicy);
		metricsEnabled = true;
		Log.info("Metrics have been enabled.");
	}

	public final void disableMetrics() {
		if (config != null) {
			if (config.hasMetrics() && config.dynamicConfiguration.dynamicMetricsConfig.enable != null &&
				config.dynamicConfiguration.dynamicMetricsConfig.enable.value) {
				Log.warn("Metrics can not be disabled via disableMetrics() when they are enabled via config.");
				return;
			}
		}

		synchronized(metricsLock) {
			disableMetricsInternal();
		}
	}

	private void disableMetricsInternal() {
		if (metricsEnabled) {
			metricsEnabled = false;
			metricsListener.onDisable(this);
			Log.info("Metrics have been disabled.");
		}
	}

	public EventLoop[] getEventLoopArray() {
		if (eventLoops == null) {
			return null;
		}
		return eventLoops.getArray();
	}

	public final ClusterStats getStats() {
		// Get sync statistics.
		final Node[] nodeArray = nodes;
		NodeStats[] nodeStats = new NodeStats[nodeArray.length];
		int count = 0;

		for (Node node : nodeArray) {
			nodeStats[count++] = new NodeStats(node);
		}

		// Get async statistics.
		EventLoopStats[] eventLoopStats = null;

		if (eventLoops != null) {
			EventLoop[] eventLoopArray = eventLoops.getArray();

			for (EventLoop eventLoop : eventLoopArray) {
				if (eventLoop.inEventLoop()) {
					// We can't block in an eventloop thread, so use non-blocking approximate statistics.
					// The statistics might be less accurate because cross-thread references are made
					// without a lock for the pool's queue size and other counters.
					eventLoopStats = new EventLoopStats[eventLoopArray.length];

					for (int i = 0; i < eventLoopArray.length; i++) {
						eventLoopStats[i] = new EventLoopStats(eventLoopArray[i]);
					}

					for (int i = 0; i < nodeArray.length; i++) {
						nodeStats[i].async = nodeArray[i].getAsyncConnectionStats();
					}
					return new ClusterStats(this, nodeStats, eventLoopStats);
				}
			}

			final EventLoopStats[] loopStats = new EventLoopStats[eventLoopArray.length];
			final ConnectionStats[][] connStats = new ConnectionStats[nodeArray.length][eventLoopArray.length];
			final AtomicInteger eventLoopCount = new AtomicInteger(eventLoopArray.length);
			final Monitor monitor = new Monitor();

			for (EventLoop eventLoop : eventLoopArray) {
				eventLoop.execute(new Runnable() {
					public void run() {
						int index = eventLoop.getIndex();
						loopStats[index] = new EventLoopStats(eventLoop);

						for (int i = 0; i < nodeArray.length; i++) {
							AsyncPool pool = nodeArray[i].getAsyncPool(index);
							int inPool = pool.queue.size();
							connStats[i][index] = new ConnectionStats(pool.total - inPool, inPool, pool.opened, pool.closed);
						}

						if (eventLoopCount.decrementAndGet() == 0) {
							monitor.notifyComplete();
						}
					}
				});
			}
			// Not running in eventloop thread, so it's ok to wait for results.
			monitor.waitTillComplete();
			eventLoopStats = loopStats;

			for (int i = 0; i < nodeArray.length; i++) {
				int inUse = 0;
				int inPool = 0;
				int opened = 0;
				int closed = 0;

				for (EventLoop eventLoop : eventLoopArray) {
					ConnectionStats cs = connStats[i][eventLoop.getIndex()];
					inUse += cs.inUse;
					inPool += cs.inPool;
					opened += cs.opened;
					closed += cs.closed;
				}
				nodeStats[i].async = new ConnectionStats(inUse, inPool, opened, closed);
			}
		}
		return new ClusterStats(this, nodeStats, eventLoopStats);
	}

	public final void getStats(ClusterStatsListener listener) {
		try {
			// Get sync statistics.
			final Node[] nodeArray = nodes;
			NodeStats[] nodeStats = new NodeStats[nodeArray.length];
			int count = 0;

			for (Node node : nodeArray) {
				nodeStats[count++] = new NodeStats(node);
			}

			if (eventLoops == null) {
				try {
					listener.onSuccess(new ClusterStats(this, nodeStats, null));
				}
				catch (Throwable e) {
				}
				return;
			}

			// Get async statistics.
			EventLoop[] eventLoopArray = eventLoops.getArray();
			final EventLoopStats[] loopStats = new EventLoopStats[eventLoopArray.length];
			final ConnectionStats[][] connStats = new ConnectionStats[nodeArray.length][eventLoopArray.length];
			final AtomicInteger eventLoopCount = new AtomicInteger(eventLoopArray.length);
			final Cluster cluster = this;

			for (EventLoop eventLoop : eventLoopArray) {
				Runnable fetch = new Runnable() {
					public void run() {
						int index = eventLoop.getIndex();
						loopStats[index] = new EventLoopStats(eventLoop);

						for (int i = 0; i < nodeArray.length; i++) {
							AsyncPool pool = nodeArray[i].getAsyncPool(index);
							int inPool = pool.queue.size();
							connStats[i][index] = new ConnectionStats(pool.total - inPool, inPool, pool.opened, pool.closed);
						}

						if (eventLoopCount.decrementAndGet() == 0) {
							// All eventloops reported. Pass results to listener.
							for (int i = 0; i < nodeArray.length; i++) {
								int inUse = 0;
								int inPool = 0;
								int opened = 0;
								int closed = 0;

								for (EventLoop eventLoop : eventLoopArray) {
									ConnectionStats cs = connStats[i][eventLoop.getIndex()];
									inUse += cs.inUse;
									inPool += cs.inPool;
									opened += cs.opened;
									closed += cs.closed;
								}
								nodeStats[i].async = new ConnectionStats(inUse, inPool, opened, closed);
							}

							try {
								listener.onSuccess(new ClusterStats(cluster, nodeStats, loopStats));
							}
							catch (Throwable e) {
							}
						}
					}
				};

				if (eventLoop.inEventLoop()) {
					fetch.run();
				}
				else {
					eventLoop.execute(fetch);
				}
			}
		}
		catch (AerospikeException ae) {
			listener.onFailure(ae);
		}
		catch (Throwable e) {
			listener.onFailure(new AerospikeException(e));
		}
	}

	public final void interruptTendSleep() {
		// Interrupt tendThread's sleep(), so node refreshes will be performed sooner.
		tendThread.interrupt();
	}

	public final void printPartitionMap() {
		for (Entry<String,Partitions> entry : partitionMap.entrySet()) {
			String namespace = entry.getKey();
			Partitions partitions = entry.getValue();
			AtomicReferenceArray<Node>[] replicas = partitions.replicas;

			for (int i = 0; i < replicas.length; i++) {
				AtomicReferenceArray<Node> nodeArray = replicas[i];
				int max = nodeArray.length();

				for (int j = 0; j < max; j++) {
					Node node = nodeArray.get(j);

					if (node != null) {
						Log.info(namespace + ',' + i + ',' + j + ',' + node);
					}
				}
			}
		}
	}

	public void changePassword(byte[] user, byte[] password, byte[] passwordHash) {
		if (this.user != null && Arrays.equals(user, this.user)) {
			this.passwordHash = passwordHash;

			// Only store clear text password if external authentication is used.
			if (authMode != AuthMode.INTERNAL) {
				this.password = password;
			}
		}
	}

	/**
	 * Set max errors allowed within configurable window for all nodes.
	 * For performance reasons, maxErrorRate is not declared volatile,
	 * so we are relying on cache coherency for other threads to
	 * recognize this change.
	 */
	public final void setMaxErrorRate(int rate) {
		this.maxErrorRate = rate;
	}

	/**
	 * The number of cluster tend iterations that defines the window for maxErrorRate.
	 * For performance reasons, errorRateWindow is not declared volatile,
	 * so we are relying on cache coherency for other threads to
	 * recognize this change.
	 */
	public final void setErrorRateWindow(int window) {
		this.errorRateWindow = window;
	}

	private static boolean supportsPartitionQuery(Node[] nodes) {
		if (nodes.length == 0) {
			return false;
		}

		for (Node node : nodes) {
			if (! node.hasPartitionQuery()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Return cluster name.
	 */
	public String getClusterName() {
		return clusterName;
	}

	public boolean validateClusterName() {
		return validateClusterName && clusterName != null && clusterName.length() > 0;
	}

	public final byte[] getUser() {
		return user;
	}

	public final byte[] getPassword() {
		return password;
	}

	public final byte[] getPasswordHash() {
		return passwordHash;
	}

	public final boolean isActive() {
		return tendValid;
	}

	/**
	 * Increment command count when metrics are enabled.
	 */
	public final void addCommandCount() {
		if (metricsEnabled) {
			commandCount.getAndIncrement();
		}
	}

	/**
	 * Return command count. The value is cumulative and not reset per metrics interval.
	 */
	public final long getCommandCount() {
		return commandCount.get();
	}

	/**
	 * Return command count. The value is cumulative and not reset per metrics interval.
	 * This function is left for backwards compatibility. Use {@link #getCommandCount()} instead.
	 */
	public final long getTranCount() {
		return commandCount.get();
	}

	/**
	 * Increment command retry count. There can be multiple retries for a single command.
	 */
	public final void addRetry() {
		retryCount.getAndIncrement();
	}

	/**
	 * Add command retry count. There can be multiple retries for a single command.
	 */
	public final void addRetries(int count) {
		retryCount.getAndAdd(count);
	}

	/**
	 * Return command retry count. The value is cumulative and not reset per metrics interval.
	 */
	public final long getRetryCount() {
		return retryCount.get();
	}

	/**
	 * Increment async delay queue timeout count.
	 */
	public final void addDelayQueueTimeout() {
		delayQueueTimeoutCount.getAndIncrement();
	}

	/**
	 * Increment async delay queue timeout count.
	 */
	public final long getDelayQueueTimeoutCount() {
		return delayQueueTimeoutCount.get();
	}

	/**
	 * Return connection recoverQueue size. The queue contains connections that have timed out and
	 * need to be drained before returning the connection to a connection pool. The recoverQueue
	 * is only used when {@link com.aerospike.client.policy.Policy#timeoutDelay} is true.
	 * <p>
	 * Since recoverQueue is a linked list where the size() calculation is expensive, a separate
	 * counter is used to track recoverQueue.size().
	 */
	public final int getRecoverQueueSize() {
		return recoverCount.get();
	}

	/**
	 * Return count of add node failures in the most recent cluster tend iteration.
	 */
	public final int getInvalidNodeCount() {
		return invalidNodeCount;
	}

	/**
	 * Return the current Metrics Policy
	 */
	public MetricsPolicy getMetricsPolicy() {
		return metricsPolicy;
	}

	public void close() {
		if (! closed.compareAndSet(false, true)) {
			// close() has already been called.
			return;
		}

		// Stop cluster tend thread.
		tendValid = false;
		tendThread.interrupt();

		synchronized(metricsLock) {
			try {
				disableMetricsInternal();
			}
			catch (Throwable e) {
				Log.warn("DisableMetrics failed: " + Util.getErrorMessage(e));
			}
		}

		if (eventLoops == null) {
			// Close synchronous node connections.
			Node[] nodeArray = nodes;
			for (Node node : nodeArray) {
				node.closeSyncConnections();
			}
		}
		else {
			// Send cluster close notification to async event loops.
			final long deadline = (closeTimeout > 0)? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(closeTimeout) : 0L;
			final AtomicInteger eventLoopCount = new AtomicInteger(eventState.length);
			final AtomicBoolean closedWithPending = new AtomicBoolean();
			boolean inEventLoop = false;

			// Send close node notification to async event loops.
			for (final EventState state : eventState) {
				if (state.eventLoop.inEventLoop()) {
					inEventLoop = true;
				}

				state.eventLoop.execute(new Runnable() {
					public void run() {
						if (state.closed) {
							// Cluster's event loop connections are already closed.
							return;
						}

						if (state.pending > 0) {
							// Cluster has pending commands.
							if (closeTimeout >= 0 && (closeTimeout == 0 || deadline - System.nanoTime() > 0)) {
								// Check again in 200ms.
								state.eventLoop.schedule(this, 200, TimeUnit.MILLISECONDS);
								return;
							}
							closedWithPending.set(true);
						}

						// Cluster's event loop connections can now be closed.
						closeEventLoop(eventLoopCount, state);
					}
				});
			}

			// Deadlock would occur if we wait from an event loop thread.
			// Only wait when not in event loop thread.
			if (! inEventLoop) {
				waitAsyncComplete();
			}

			if (closedWithPending.get()) {
				Log.warn("Cluster closed with pending async commands");
			}
		}
	}

	/**
	 * Wait until all event loops have finished processing pending cluster commands.
	 * Must be called from an event loop thread.
	 */
	private final void closeEventLoop(AtomicInteger eventLoopCount, EventState state) {
		// Prevent future cluster commands on this event loop.
		state.closed = true;

		// Close asynchronous node connections for single event loop.
		Node[] nodeArray = nodes;
		for (Node node : nodeArray) {
			node.closeAsyncConnections(state.index);
		}

		if (eventLoopCount.decrementAndGet() == 0) {
			// All event loops have reported.
			// Close synchronous node connections.
			for (Node node : nodeArray) {
				node.closeSyncConnections();
			}
			notifyAsyncComplete();
		}
	}

	private synchronized void waitAsyncComplete() {
		while (! asyncComplete) {
			try {
				super.wait();
			}
			catch (InterruptedException ie) {
			}
		}
	}

	private synchronized void notifyAsyncComplete() {
		asyncComplete = true;
		super.notify();
	}
}

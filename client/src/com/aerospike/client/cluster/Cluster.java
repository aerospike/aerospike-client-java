/*
 * Copyright 2012-2018 Aerospike, Inc.
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.Value;
import com.aerospike.client.admin.AdminCommand;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventState;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Environment;
import com.aerospike.client.util.Util;

public class Cluster implements Runnable, Closeable {
	private static final int MaxSocketIdleSecondLimit = 60 * 60 * 24; // Limit maxSocketIdle to 24 hours
	
	// Expected cluster name.
	protected final String clusterName;
	
	// Initial host nodes specified by user.
	private volatile Host[] seeds;
	
	// All host aliases for all nodes in cluster.
	// Only accessed within cluster tend thread.
	protected final HashMap<Host,Node> aliases;

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
	protected final TlsPolicy tlsPolicy;

    // Authentication mode.
	public final AuthMode authMode;

    // User name in UTF-8 encoded bytes.
	protected final byte[] user;

	// Password in UTF-8 encoded bytes.
	private byte[] password;

	// Password in hashed format in bytes.
	private byte[] passwordHash;

	// Random node index.
	private final AtomicInteger nodeIndex;
	
	// Random partition replica index. 
	private final AtomicInteger replicaIndex;

	// Thread pool used in synchronous batch, scan and query commands.
	private final ExecutorService threadPool;
	
	// Optional event loops for async mode.
	public final EventLoops eventLoops;
	
	// Extra event loop state for this cluster.
	public final EventState[] eventState;
	
	// Maximum socket idle in nanoseconds.
	public final long maxSocketIdleNanos;

	// Size of node's synchronous connection pool.
	protected final int connectionQueueSize;
	
	// Sync connection pools per node. 
	protected final int connPoolsPerNode;

	// Initial connection timeout.
	public final int connectionTimeout;

	// Login timeout.
	public final int loginTimeout;

	// Interval in milliseconds between cluster tends.
	private final int tendInterval;

	// Tend thread variables.
	private Thread tendThread;
	protected volatile boolean tendValid;
	
	// Is threadPool shared with other client instances?
	private final boolean sharedThreadPool;
	
	// Request prole replicas in addition to master replicas?
	protected boolean requestProleReplicas;

	// Should use "services-alternate" instead of "services" in info request?
	protected final boolean useServicesAlternate;

	private boolean asyncComplete;

	public Cluster(ClientPolicy policy, Host[] hosts) throws AerospikeException {
		this.clusterName = policy.clusterName;
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
			if (authMode == AuthMode.EXTERNAL) {
				throw new AerospikeException("TLS is required for authentication mode: " + authMode);		
			}
		}
		
		this.seeds = hosts;
		
		if (policy.user != null && policy.user.length() > 0) {
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

			if (! (pass.length() == 60 && pass.startsWith("$2a$")))
			{
				pass = AdminCommand.hashPassword(pass);
			}
			this.passwordHash = Buffer.stringToUtf8(pass);
		}
		else {
			this.user = null;
		}

		connectionQueueSize = policy.maxConnsPerNode;
		connPoolsPerNode = policy.connPoolsPerNode;
		connectionTimeout = policy.timeout;
		loginTimeout = policy.loginTimeout;
		maxSocketIdleNanos = TimeUnit.SECONDS.toNanos((policy.maxSocketIdle <= MaxSocketIdleSecondLimit)? policy.maxSocketIdle : MaxSocketIdleSecondLimit);
		tendInterval = policy.tendInterval;
		ipMap = policy.ipMap;
		
		if (policy.threadPool == null) {
			threadPool = Executors.newCachedThreadPool(new ThreadDaemonFactory());
		}
		else {
			threadPool = policy.threadPool;
		}
		sharedThreadPool = policy.sharedThreadPool;
		requestProleReplicas = policy.requestProleReplicas;
		useServicesAlternate = policy.useServicesAlternate;

		aliases = new HashMap<Host,Node>();
		nodesMap = new HashMap<String,Node>();
		nodes = new Node[0];	
		partitionMap = new HashMap<String,Partitions>();		
		nodeIndex = new AtomicInteger();
		replicaIndex = new AtomicInteger();

		eventLoops = policy.eventLoops;
		
		if (eventLoops != null) {
			EventLoop[] loops = eventLoops.getArray();
			eventState = new EventState[loops.length];
			
			for (int i = 0; i < loops.length; i++) {
				eventState[i] = loops[i].createState();
			}
			
			if (policy.tlsPolicy != null) {
				eventLoops.initTlsContext(policy.tlsPolicy);
			}
		}
		else {
			eventState = null;
		}

		if (policy.forceSingleNode) {
			// Communicate with the first seed node only.
			// Do not run cluster tend thread.
			try {
				forceSingleNode();
			}
			catch (RuntimeException e) {
				close();
				throw e;
			}
		}
		else {
			initTendThread(policy.failIfNotConnected);
		}
	}
	
	public void forceSingleNode() {
		// Initialize tendThread, but do not start it.
		tendValid = true;
		tendThread = new Thread(this);
		
		// Validate first seed.
		Host seed = seeds[0];
		NodeValidator nv = new NodeValidator();
		HashMap<String,Node> nodesToAdd = new HashMap<String,Node>();
		
		try {
			nv.seedNodes(this, seed, nodesToAdd);
		}
		catch (Exception e) {
			throw new AerospikeException("Seed " + seed + " failed: " + e.getMessage(), e);
		}
		
		// Add seed node to nodes.
		addNodes(nodesToAdd);
		Node node = nodes[0];
		
		// Initialize partitionMaps.
		Peers peers = new Peers(nodes.length + 16, 16);
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
	
	public void initTendThread(boolean failIfNotConnected) throws AerospikeException {
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
			
			// Disable double type support if some nodes don't support it.
			if (Value.UseDoubleType && ! node.hasDouble()) {
				if (Log.warnEnabled()) {
					Log.warn("Some nodes don't support new double type.  Disabling.");
				}
				Value.UseDoubleType = false;
			}

			// Disable prole requests if some nodes don't support it.
			if (requestProleReplicas && ! node.hasReplicasAll()) {
				if (Log.warnEnabled()) {
					Log.warn("Some nodes don't support 'replicas-all'.  Use 'replicas-master' for all nodes.");
				}
				requestProleReplicas = false;
			}
		}
		
		if (seedsToAdd.size() > 0) {
			addSeeds(seedsToAdd.toArray(new Host[seedsToAdd.size()]));
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
	 *
	 * At least two cluster tends are necessary. The first cluster
	 * tend finds a seed node and obtains the seed's partition maps 
	 * and peer nodes.  The second cluster tend requests partition 
	 * maps from the peer nodes.
	 *
	 * A third cluster tend is allowed if some peers nodes can't
	 * be contacted.  If peer nodes are still unreachable, an
	 * exception is thrown.
	 */
    private final void waitTillStabilized(boolean failIfNotConnected) throws AerospikeException {
		int count = -1;

		for (int i = 0; i < 3; i++) {
			tend(failIfNotConnected);

			// Check to see if cluster has changed since the last Tend().
			// If not, assume cluster has stabilized and return.
			if (count == nodes.length) {
				return;
			}

			Util.sleep(1);
			count = nodes.length;
		}

		String message = "Cluster not stabilized after multiple tend attempts";

		if (failIfNotConnected) {
			throw new AerospikeException(message);
		}
		else {
			Log.warn(message);
		}
    }
    	
	public final void run() {
		while (tendValid) {			
			// Tend cluster.
			try {
				tend(false);
			}
			catch (Exception e) {
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
	private final void tend(boolean failIfNotConnected) throws AerospikeException {
		// All node additions/deletions are performed in tend thread.		
		// If active nodes don't exist, seed cluster.
		if (nodes.length == 0) {
			seedNodes(failIfNotConnected);
		}

		// Initialize tend iteration node statistics.
		Peers peers = new Peers(nodes.length + 16, 16);
		
		// Clear node reference counts.
		for (Node node : nodes) {
			node.referenceCount = 0;
			node.partitionChanged = false;
			
			if (! node.hasPeers()) {
				peers.usePeers = false;
			}
		}
		
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
		}
		
		// Refresh partition map when necessary.
		for (Node node : nodes) {			
			if (node.partitionChanged) {
				node.refreshPartitions(peers);
			}
		}

		if (peers.genChanged || ! peers.usePeers) {
			// Handle nodes changes determined from refreshes.
			ArrayList<Node> removeList = findNodesToRemove(peers.refreshCount);
			
			// Remove nodes in a batch.
			if (removeList.size() > 0) {
				removeNodes(removeList);
			}
		}

		// Add nodes in a batch.
		if (peers.nodes.size() > 0) {
			addNodes(peers.nodes);
		}
	}
	
	private final boolean seedNodes(boolean failIfNotConnected) throws AerospikeException {
		// Must copy array reference for copy on write semantics to work.
		Host[] seedArray = seeds;
		Exception[] exceptions = null;
		
		// Add all nodes at once to avoid copying entire array multiple times.
		HashMap<String,Node> nodesToAdd = new HashMap<String,Node>(seedArray.length + 16);

		for (int i = 0; i < seedArray.length; i++) {
			Host seed = seedArray[i];

			try {
				NodeValidator nv = new NodeValidator();
				nv.seedNodes(this, seed, nodesToAdd);	
			}
			catch (Exception e) {
				// Store exception and try next host
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

		if (nodesToAdd.size() > 0) {
			addNodes(nodesToAdd);
			return true;
		}
		else if (failIfNotConnected) {
			StringBuilder sb = new StringBuilder(500);
			sb.append("Failed to connect to host(s): ");
			sb.append(Environment.Newline);

			for (int i = 0; i < seedArray.length; i++)
			{
				sb.append(seedArray[i]);
				sb.append(' ');

				Exception ex = exceptions == null ? null : exceptions[i];

				if (ex != null)
				{
					sb.append(ex.getMessage());
					sb.append(Environment.Newline);
				}
			}
			throw new AerospikeException.Connection(sb.toString());		
		}
		return false;
	}
	
	protected Node createNode(NodeValidator nv) {
		return new Node(this, nv);
	}
		
	private final ArrayList<Node> findNodesToRemove(int refreshCount) {
		ArrayList<Node> removeList = new ArrayList<Node>();
		
		for (Node node : nodes) {
			if (! node.isActive()) {
				// Inactive nodes must be removed.
				removeList.add(node);
				continue;
			}
			
			if (refreshCount == 0 && node.failures >= 5) {
				// All node info requests failed and this node had 5 consecutive failures.
				// Remove node.  If no nodes are left, seeds will be tried in next cluster
				// tend iteration.
				removeList.add(node);
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
						removeList.add(node);							
					}
				}
				else {
					// Node not responding. Remove it.
					removeList.add(node);
				}		
			}
		}
		return removeList;
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
			if (Log.infoEnabled()) {
				Log.info("Add node " + node);
			}
			
			nodeArray[count++] = node;
			nodesMap.put(node.getName(), node);
			
			// Add node's aliases to global alias set.
			// Aliases are only used in tend thread, so synchronization is not necessary.
			for (Host alias : node.aliases) {
				aliases.put(alias, node);
			}
		}
		
		// Replace nodes with copy.
		nodes = nodeArray;
	}
	
	private final void removeNodes(List<Node> nodesToRemove) {
		// There is no need to delete nodes from partitionWriteMap because the nodes 
		// have already been set to inactive. Further connection requests will result 
		// in an exception and a different node will be tried.
		
		// Cleanup node resources.
		for (Node node : nodesToRemove) {
			// Remove node from map.
			nodesMap.remove(node.getName());
			
			// Remove node's aliases from cluster alias set.
			// Aliases are only used in tend thread, so synchronization is not necessary.
			for (Host alias : node.aliases) {
				// Log.debug("Remove alias " + alias);
				aliases.remove(alias);
			}		
			node.close();
		}

		// Remove all nodes at once to avoid copying entire array multiple times.
		removeNodesCopy(nodesToRemove);
	}
			
	/**
	 * Remove nodes using copy on write semantics.
	 */
	private final void removeNodesCopy(List<Node> nodesToRemove) {
		// Create temporary nodes array.
		// Since nodes are only marked for deletion using node references in the nodes array,
		// and the tend thread is the only thread modifying nodes, we are guaranteed that nodes
		// in nodesToRemove exist.  Therefore, we know the final array size. 
		Node[] nodeArray = new Node[nodes.length - nodesToRemove.size()];
		int count = 0;
		
		// Add nodes that are not in remove list.
		for (Node node : nodes) {
			if (findNode(node, nodesToRemove)) {				
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
			
		// Replace nodes with copy.
		nodes = nodeArray;
	}

	private final static boolean findNode(Node search, List<Node> nodeList) {
		for (Node node : nodeList) {
			if (node.equals(search)) {
				return true;
			}
		}
		return false;
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

	public final Node getMasterNode(Partition partition) throws AerospikeException.InvalidNode {		
		// Must copy hashmap reference for copy on write semantics to work.
		HashMap<String,Partitions> map = partitionMap;
		Partitions partitions  = map.get(partition.namespace);

		if (partitions == null) {
			throw new AerospikeException("Invalid namespace: " + partition.namespace);
		}

		Node node = partitions.replicas[0].get(partition.partitionId);
		
		if (node != null && node.isActive()) {
			return node;
		}
		
		// When master only specified, should never get random nodes.
		throw new AerospikeException.InvalidNode();
	}

	public final Node getMasterProlesNode(Partition partition) throws AerospikeException.InvalidNode {		
		// Must copy hashmap reference for copy on write semantics to work.
		HashMap<String,Partitions> map = partitionMap;
		Partitions partitions  = map.get(partition.namespace);

		if (partitions == null) {
			throw new AerospikeException("Invalid namespace: " + partition.namespace);
		}

		AtomicReferenceArray<Node>[] replicas = partitions.replicas;

		for (int i = 0; i < replicas.length; i++) {
			int index = Math.abs(replicaIndex.getAndIncrement() % replicas.length);						
			Node node = replicas[index].get(partition.partitionId);

			if (node != null && node.isActive()) {
				return node;
			}
		}

		if (partitions.cpMode) {
			throw new AerospikeException.InvalidNode();
		}
		return getRandomNode();
	}

	public final Node getRandomNode() throws AerospikeException.InvalidNode {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;
				
		for (int i = 0; i < nodeArray.length; i++) {			
			// Must handle concurrency with other non-tending threads, so nodeIndex is consistent.
			int index = Math.abs(nodeIndex.getAndIncrement() % nodeArray.length);						
			Node node = nodeArray[index];
			
			if (node.isActive()) {
				//if (Log.debugEnabled()) {
				//	Log.debug("Node " + node + " is active. index=" + index);
				//}
				return node;
			}
		}
		throw new AerospikeException.InvalidNode();		
	}

	public final Node[] getNodes() {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;
		return nodeArray;
	}

	public final Node getNode(String nodeName) throws AerospikeException.InvalidNode {
		Node node = findNode(nodeName);
		
		if (node == null) {			
			throw new AerospikeException.InvalidNode();
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

	public final ExecutorService getThreadPool() {
		return threadPool;
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

	public void close() {
		if (! sharedThreadPool) {
			// Shutdown synchronous thread pool.
			threadPool.shutdown();
		}
		
		// Stop cluster tend thread.
		tendValid = false;
		tendThread.interrupt();
		
		if (eventLoops == null) {
			// Close synchronous node connections.
			Node[] nodeArray = nodes;
			for (Node node : nodeArray) {
				node.closeSyncConnections();
			}
		}
		else {
			// Send cluster close notification to async event loops.
			final AtomicInteger eventLoopCount = new AtomicInteger(eventState.length);
			boolean inEventLoop = false;
			
			// Send close node notification to async event loops.
			for (final EventState state : eventState) {
				if (state.eventLoop.inEventLoop()) {
					inEventLoop = true;
				}
				
				state.eventLoop.execute(new Runnable() {
					public void run() {
						if (state.pending < 0) {
							// Cluster's event loop connections are already closed.
							return;
						}
						
						if (state.pending > 0) {
							// Cluster has pending commands.
							// Check again in 200ms.
							state.eventLoop.schedule(this, 200, TimeUnit.MILLISECONDS);
							return;
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
		}
	}

	/**
	 * Wait until all event loops have finished processing pending cluster commands.
	 * Must be called from an event loop thread.
	 */
	private final void closeEventLoop(AtomicInteger eventLoopCount, EventState state) {		
		// Prevent future cluster commands on this event loop.
		state.pending = -1;

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

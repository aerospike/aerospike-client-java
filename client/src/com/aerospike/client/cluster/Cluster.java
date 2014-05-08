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
package com.aerospike.client.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.util.Util;

public class Cluster implements Runnable {
	// Initial host nodes specified by user.
	private volatile Host[] seeds;
	
	// All aliases for all nodes in cluster.
	private final HashMap<Host,Node> aliases;

	// Active nodes in cluster.
	private volatile Node[] nodes;	

	// Hints for best node for a partition
	private volatile HashMap<String,Node[]> partitionWriteMap;
	
	// Random node index.
	private final AtomicInteger nodeIndex;
	
	// Thread pool used in batch, scan and query commands.
	private final ExecutorService threadPool;
	
	// Size of node's synchronous connection pool.
	protected final int connectionQueueSize;
	
	// Initial connection timeout.
	private final int connectionTimeout;

	// Maximum socket idle in seconds.
	protected final int maxSocketIdle;

	// Tend thread variables.
	private Thread tendThread;
	private volatile boolean tendValid;
	
	// Is threadPool shared with other client instances?
	private final boolean sharedThreadPool;
	
	public Cluster(ClientPolicy policy, Host[] hosts) throws AerospikeException {
		this.seeds = hosts;
		connectionQueueSize = policy.maxThreads + 1;  // Add one connection for tend thread.
		connectionTimeout = policy.timeout;
		maxSocketIdle = policy.maxSocketIdle;
		
		if (policy.threadPool == null) {
			// Create cached thread pool with daemon threads.
			// Daemon threads automatically terminate when the program terminates.
			threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
				public final Thread newThread(Runnable runnable) {
					Thread thread = new Thread(runnable);
					thread.setDaemon(true);
					return thread;
				}
			});
		}
		else {
			threadPool = policy.threadPool;
		}
		sharedThreadPool = policy.sharedThreadPool;
		
		aliases = new HashMap<Host,Node>();
		nodes = new Node[0];	
		partitionWriteMap = new HashMap<String,Node[]>();		
		nodeIndex = new AtomicInteger();
	}
	
	public void initTendThread() {		
		// Tend cluster until all nodes identified.
        waitTillStabilized();
        
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
     * If the cluster has not stabilized by the timeout, return
     * control as well.  Do not return an error since future 
     * database requests may still succeed.
     */
    private final void waitTillStabilized() {
		long limit = System.currentTimeMillis() + connectionTimeout;
		int count = -1;
		
		do {
			tend();
		
			// Check to see if cluster has changed since the last Tend().
			// If not, assume cluster has stabilized and return.
			if (count == nodes.length)
				return;
			
			Util.sleep(1);			
			count = nodes.length;
		} while (System.currentTimeMillis() < limit);
    }
    	
	public final void run() {
		while (tendValid) {			
			// Tend cluster.
			try {
				tend();
			}
			catch (Exception e) {
				if (Log.warnEnabled()) {
					Log.warn("Cluster tend failed: " + Util.getErrorMessage(e));
				}
			}
			// Sleep for 2 seconds.
			Util.sleep(2000);
		}
	}
	
    /**
     * Check health of all nodes in the cluster.
     */
	private final void tend() {
		// All node additions/deletions are performed in tend thread.		
		// If active nodes don't exist, seed cluster.
		if (nodes.length == 0) {
			seedNodes();
		}

		// Clear node reference counts.
		for (Node node : nodes) {
			node.referenceCount = 0;
			node.responded = false;
		}
		
		// Refresh all known nodes.
		ArrayList<Host> friendList = new ArrayList<Host>();
		int refreshCount = 0;
		
		for (Node node : nodes) {
			try {
				if (node.isActive()) {
					node.refresh(friendList);
					refreshCount++;
				}
			}
			catch (Exception e) {
				if (Log.debugEnabled()) {
					Log.debug("Node " + node + " refresh failed: " + Util.getErrorMessage(e));
				}
			}
		}
		
		// Handle nodes changes determined from refreshes.
		ArrayList<Node> addList = findNodesToAdd(friendList);
		ArrayList<Node> removeList = findNodesToRemove(refreshCount);
		
		// Remove nodes in a batch.
		if (removeList.size() > 0) {
			removeNodes(removeList);
		}

		// Add nodes in a batch.
		if (addList.size() > 0) {
			addNodes(addList);
		}
	}
	
	protected final Node findAlias(Host alias) {
		return aliases.get(alias);
	}
	
	protected final void updatePartitions(Connection conn, Node node) throws AerospikeException {
		HashMap<String,Node[]> map;
		
		if (node.useNewInfo) {
			PartitionTokenizerNew tokens = new PartitionTokenizerNew(conn);
			map = tokens.updatePartition(partitionWriteMap, node);
		}
		else {
			PartitionTokenizerOld tokens = new PartitionTokenizerOld(conn);
			map = tokens.updatePartition(partitionWriteMap, node);
		}
		
		if (map != null) {		
			partitionWriteMap = map;
		}
	}

	private final void seedNodes() {
		// Must copy array reference for copy on write semantics to work.
		Host[] seedArray = seeds;
		
		// Add all nodes at once to avoid copying entire array multiple times.
		ArrayList<Node> list = new ArrayList<Node>();

		for (Host seed : seedArray) {
			try {
				NodeValidator seedNodeValidator = new NodeValidator(seed, connectionTimeout);
				
				// Seed host may have multiple aliases in the case of round-robin dns configurations.
				for (Host alias : seedNodeValidator.aliases) {
					NodeValidator nv;
					
					if (alias.equals(seed)) {
						nv = seedNodeValidator;
					}
					else {
						nv = new NodeValidator(alias, connectionTimeout);
					}
										
					if (! findNodeName(list, nv.name)) {
						Node node = createNode(nv);
						addAliases(node);
						list.add(node);
					}
				}
			}
			catch (Exception e) {
				// Try next host
				if (Log.debugEnabled()) {
					Log.debug("Seed " + seed + " failed: " + Util.getErrorMessage(e));
				}
			}
		}

		if (list.size() > 0) {
			addNodesCopy(list);
		}
	}
	
	private final static boolean findNodeName(ArrayList<Node> list, String name) {
		for (Node node : list) {
			if (node.getName().equals(name)) {
				return true;
			}
		}
		return false;
	}
	
	private final ArrayList<Node> findNodesToAdd(List<Host> hosts) {
		ArrayList<Node> list = new ArrayList<Node>(hosts.size());
		
		for (Host host : hosts) {
			try {
				NodeValidator nv = new NodeValidator(host, connectionTimeout);
				Node node = findNode(nv.name);
				
				if (node != null) {
					// Duplicate node name found.  This usually occurs when the server 
					// services list contains both internal and external IP addresses 
					// for the same node.  Add new host to list of alias filters
					// and do not add new node.
					node.referenceCount++;
					node.addAlias(host);
					aliases.put(host, node);
					continue;
				}
				node = createNode(nv);		
				list.add(node);
			}
			catch (Exception e) {
				if (Log.warnEnabled()) {
					Log.warn("Add node " + host + " failed: " + Util.getErrorMessage(e));
				}
			}
		}
		return list;
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
			
			switch (nodes.length) {
			case 1:
				// Single node clusters rely solely on node health.
				if (node.isUnhealthy()) {
					removeList.add(node);						
				}
				break;
				
			case 2:
				// Two node clusters require at least one successful refresh before removing.
				if (refreshCount == 1 && node.referenceCount == 0 && ! node.responded) {
					// Node is not referenced nor did it respond.
					removeList.add(node);
				}
				break;
				
			default:
				// Multi-node clusters require two successful node refreshes before removing.
				if (refreshCount >= 2 && node.referenceCount == 0) {
					// Node is not referenced by other nodes.
					// Check if node responded to info request.
					if (node.responded) {
						// Node is alive, but not referenced by other nodes.  Check if mapped.
						if (! findNodeInPartitionMap(node)) {
							// Node doesn't have any partitions mapped to it.
							// There is not point in keeping it in the cluster.
							removeList.add(node);							
						}
					}
					else {
						// Node not responding. Remove it.
						removeList.add(node);
					}		
				}
				break;
			}
		}
		return removeList;
	}
	
	private final boolean findNodeInPartitionMap(Node filter) {
		for (Node[] nodeArray : partitionWriteMap.values()) {
			for (Node node : nodeArray) {
				// Use reference equality for performance.
				if (node == filter) {
					return true;
				}
			}
		}
		return false;
	}
	
	private final void addNodes(List<Node> nodesToAdd) {
		// Add all nodes at once to avoid copying entire array multiple times.		
		for (Node node : nodesToAdd) {
			addAliases(node);
		}
		addNodesCopy(nodesToAdd);
	}
	
	private final void addAliases(Node node) {		
		// Add node's aliases to global alias set.
		// Aliases are only used in tend thread, so synchronization is not necessary.
		for (Host alias : node.getAliases()) {
			aliases.put(alias, node);
		}
	}
	
	/**
	 * Add nodes using copy on write semantics.
	 */
	private final void addNodesCopy(List<Node> nodesToAdd) {
		// Create temporary nodes array.
		Node[] nodeArray = new Node[nodes.length + nodesToAdd.size()];
		int count = 0;
		
		// Add existing nodes.
		for (Node node : nodes) {
			nodeArray[count++] = node;
		}
		
		// Add new nodes.
		for (Node node : nodesToAdd) {			
			if (Log.infoEnabled()) {
				Log.info("Add node " + node);
			}
			nodeArray[count++] = node;
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
			// Remove node's aliases from cluster alias set.
			// Aliases are only used in tend thread, so synchronization is not necessary.
			for (Host alias : node.getAliases()) {
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
		return nodeArray.length > 0 && tendValid;
	}
	
	public final Node getNode(Partition partition) throws AerospikeException.InvalidNode {
		// Must copy hashmap reference for copy on write semantics to work.
		HashMap<String,Node[]> map = partitionWriteMap;
		Node[] nodeArray = map.get(partition.namespace);
		
		if (nodeArray != null) {
			Node node = nodeArray[partition.partitionId];
			
			if (node != null && node.isActive()) {
				return node;
			}
		}
		/*
		if (Log.debugEnabled()) {
			Log.debug("Choose random node for " + partition);
		}
		*/
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

	private final Node findNode(String nodeName) {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;
		
		for (Node node : nodeArray) {
			if (node.getName().equals(nodeName)) {
				return node;
			}
		}
		return null;
	}

	public final ExecutorService getThreadPool() {
		return threadPool;
	}

	public final int getMaxSocketIdle() {
		return maxSocketIdle;
	}

	public void close() {
		if (! sharedThreadPool) {
			threadPool.shutdown();
		}
		
		tendValid = false;
		tendThread.interrupt();
		
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;
		
		for (Node node : nodeArray) {
			node.close();
		}	
	}
}

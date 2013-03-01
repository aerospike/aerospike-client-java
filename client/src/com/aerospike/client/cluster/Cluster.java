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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.Log;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.util.Util;

public final class Cluster implements Runnable {
	// Initial host nodes specified by user.
	private volatile Host[] seeds;
	
	// All aliases for all nodes in cluster.
	private final HashSet<Host> aliases;

	// Active nodes in cluster.
	private volatile Node[] nodes;	

	// Hints for best node for a partition
	private final ConcurrentHashMap<Partition,Node> partitionWriteMap;
	
	// Random node index.
	private volatile int nodeIndex;
	
	// Size of node's connection pool.
	private final int connectionLimit;
	
	// Initial connection timeout.
	private final int connectionTimeout;

	// Tend thread variables.
	private final Thread tendThread;
	private volatile boolean tendValid;
	
	public Cluster(ClientPolicy policy, Host[] hosts) throws AerospikeException {
		this.seeds = hosts;
		connectionLimit = policy.maxThreads + 1;
		connectionTimeout = policy.timeout;		
		
		aliases = new HashSet<Host>();
		nodes = new Node[0];
		
		// Preallocate 2 namespaces each with default number of partitions.
		partitionWriteMap = new ConcurrentHashMap<Partition,Node>(Node.PARTITIONS * 2); 

		// Tend cluster until all nodes identified.
        waitTillStabilized();
        
        if (Log.debugEnabled()) {
        	for (Host host : seeds) {
				Log.debug("Add seed " + host);
        	}
        }
        
        // Add other nodes as seeds, if they don't already exist.
        ArrayList<Host> seedsToAdd = new ArrayList<Host>(nodes.length + 1);
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
	
	public void addSeeds(Host[] hosts) {
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

	private boolean findSeed(Host search) {
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
    private void waitTillStabilized() {
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
    	
	public void run() {
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
	private void tend() {
		// All node additions/deletions are performed in tend thread.
		// Remove sick nodes.
		ArrayList<Node> removeList = new ArrayList<Node>();
		
		for (Node node : nodes) {
			if (! node.isActive()) {
				removeList.add(node);
			}
		}
		
		if (removeList.size() > 0) {
			removeNodes(removeList);
		}

		// If active nodes don't exist, seed cluster.
		if (nodes.length == 0) {
			seedNodes();
		}

		// Refresh all known nodes.
		ArrayList<Host> friendList = new ArrayList<Host>();

		for (Node node : nodes) {
			try {
				if (node.isActive()) {
					node.refresh(friendList);
				}
			}
			catch (Exception e) {
				if (Log.debugEnabled()) {
					Log.debug("Node " + node + " refresh failed: " + Util.getErrorMessage(e));
				}
			}
		}
		
		// Add friends to cluster.
		if (friendList.size() > 0) {
			addNodes(friendList);
		}
	}
	
	protected boolean findAlias(Host alias) {
		return aliases.contains(alias);
	}
	
	protected void updatePartition(Partition key, Node node) {
		// Log.debug(key.toString() + ',' + node.getName());
		partitionWriteMap.put(key, node);
	}

	private void seedNodes() {
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
						Node node = new Node(this, nv, connectionLimit);
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
	
	private static boolean findNodeName(ArrayList<Node> list, String name) {
		for (Node node : list) {
			if (node.getName().equals(name)) {
				return true;
			}
		}
		return false;
	}
		
	private void addNodes(List<Host> hosts) {
		// Add all nodes at once to avoid copying entire array multiple times.
		ArrayList<Node> list = new ArrayList<Node>(hosts.size());
		
		for (Host host : hosts) {
			try {
				NodeValidator nv = new NodeValidator(host, connectionTimeout);
				Node node = new Node(this, nv, connectionLimit);				
				addAliases(node);			
				list.add(node);
			}
			catch (Exception e) {
				if (Log.warnEnabled()) {
					Log.warn("Add node " + host + " failed: " + Util.getErrorMessage(e));
				}
			}
		}
		
		if (list.size() > 0) {
			addNodesCopy(list);
		}
	}
	
	private void addAliases(Node node) {		
		// Add node's aliases to global alias set.
		// Aliases are only used in tend thread, so synchronization is not necessary.
		for (Host alias : node.getAliases()) {
			aliases.add(alias);
		}
	}
	
	/**
	 * Add nodes using copy on write semantics.
	 */
	private void addNodesCopy(List<Node> nodesToAdd) {
		// Create temporary nodes array.
		Node[] nodeArray = new Node[nodes.length + nodesToAdd.size()];
		int count = 0;
		
		// Add existing nodes.
		for (Node node : nodes) {
			nodeArray[count++] = node;
		}
		
		// Add new Nodes
		for (Node node : nodesToAdd) {			
			if (Log.debugEnabled()) {
				Log.debug("Add node " + node);
			}
			nodeArray[count++] = node;
		}
		
		// Replace nodes with copy.
		nodes = nodeArray;
	}
	
	private void removeNodes(List<Node> nodesToRemove) {
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
	private void removeNodesCopy(List<Node> nodesToRemove) {
		// Create temporary nodes array.
		// Since nodes are only marked for deletion using node references in the nodes array,
		// and the tend thread is the only thread modifying nodes, we are guaranteed that nodes
		// in nodesToRemove exist.  Therefore, we know the final array size. 
		Node[] nodeArray = new Node[nodes.length - nodesToRemove.size()];
		int count = 0;
		
		// Add nodes that are not in remove list.
		for (Node node : nodes) {
			if (findNode(node, nodesToRemove)) {				
				if (Log.debugEnabled()) {
					Log.debug("Remove node " + node);
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

	private static boolean findNode(Node search, List<Node> nodeList) {
		for (Node node : nodeList) {
			if (node.equals(search)) {
				return true;
			}
		}
		return false;
	}
	
	public boolean isConnected() {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;
		return nodeArray.length > 0 && tendValid;
	}
	
	public Node getNode(Partition partition) throws AerospikeException.InvalidNode {
		Node node = partitionWriteMap.get(partition);
		
		if (node == null || ! node.isActive()) {
			if (Log.debugEnabled()) {
				Log.debug("Choose random node " + node + " for " + partition);
			}
			node = getRandomNode();
		}
		/*
		else {
			if (Log.debugEnabled()) {
				Log.debug("Map node " + node + " for " + partition);
			}
		}
		*/
		return node;
	}

	private Node getRandomNode() throws AerospikeException.InvalidNode {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;
		int index;
				
		for (int i = 0; i < nodeArray.length; i++) {			
			// Must synchronize with other non-tending threads, so nodeIndex is consistent.
			synchronized (this) {
				if (nodeIndex >= nodeArray.length) {
					nodeIndex = 0;
				}
				index = nodeIndex; 
				nodeIndex++;
			}
			
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

	public Node[] getNodes() {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;
		return nodeArray;
	}

	public Node getNode(String nodeName) throws AerospikeException.InvalidNode {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;
		
		for (Node node : nodeArray) {
			if (node.getName().equals(nodeName)) {
				return node;
			}
		}
		throw new AerospikeException.InvalidNode();
	}

	public void close() {
		tendValid = false;
		tendThread.interrupt();
		
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;
		
		for (Node node : nodeArray) {
			node.close();
		}
	}
}

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
	private final ArrayList<Host> seeds;
	
	// All aliases for all nodes in cluster.
	private final HashSet<Host> aliases;

	// All node names in cluster.
	private final HashSet<String> nodeNames;

	// Active nodes in cluster.
	private volatile Node[] nodes;	

	// Hints for best node for a partition
	private final ConcurrentHashMap<Partition,Node> partitionWriteMap;
	
	// Tend thread variables.
	private Thread tendThread;
	private volatile boolean tendValid;
	
	// Random node index.
	private int nodeIndex;
	
	// Size of node's connection pool.
	private final int connectionLimit;
	
	// Initial connection timeout.
	private final int connectionTimeout;

	public Cluster(ClientPolicy policy, Host host) throws AerospikeException {
		// Verify host node exists and responds to info requests.
		new NodeValidator(host, policy.timeout);
		
		seeds = new ArrayList<Host>();
		seeds.add(host);

		aliases = new HashSet<Host>();
		nodeNames = new HashSet<String>();
		nodes = new Node[0];
		connectionLimit = policy.maxThreads + 1;
		connectionTimeout = policy.timeout;
		
		// Preallocate 2 namespaces each with default number of partitions.
		partitionWriteMap = new ConcurrentHashMap<Partition,Node>(Node.PARTITIONS * 2); 
		
        waitTillStabilized(policy.timeout);
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
    private void waitTillStabilized(int timeoutMillis) {
		long limit = System.currentTimeMillis() + timeoutMillis;
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
    
	public void activate(Host[] hosts) {
		// Add all hosts to cluster as potential seeds in case of complete network failure.
		Node[] nodeArray = nodes;
		HashSet<Host> hostSet = new HashSet<Host>(nodeArray.length + hosts.length);			
		
		for (Node node : nodeArray) {
			hostSet.add(node.getHost());
		}

		for (Host host : hosts) {
			hostSet.add(host);
		}
		seeds.clear();
		
		for (Host host : hostSet) {
			// Log.debug("Add seed " + host);
			seeds.add(host);
		}
	
        // Run cluster tend thread.
        tendValid = true;
        tendThread = new Thread(this);
        tendThread.setName("tend");
        tendThread.setDaemon(true);
        tendThread.start();
	}
	
	public void run() {
		while (tendValid) {			
			// Tend cluster.
			try {
				tend();
			}
			catch (Exception e) {
				if (Log.warnEnabled()) {
					Log.warn("Cluster tend failed: " + e.getMessage());
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
			seedNode();
		}

		// Refresh all known nodes.
		ArrayList<Host> friendList = new ArrayList<Host>();

		for (Node node : nodes) {
			try {
				node.refresh(friendList);
			}
			catch (Exception e) {
				if (Log.warnEnabled()) {
					Log.warn("Node " + node.getName() + " refresh failed: " + e.getMessage());
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

	private void seedNode() {
		for (Host host : seeds) {
			try {
				Node node = new Node(this, host, connectionLimit, connectionTimeout);
				addNodeNameAndAliases(node);
				
				ArrayList<Node> list = new ArrayList<Node>(1);
				list.add(node);
				addNodesCopy(list);
				return;
			}
			catch (Exception e) {
				// Try next host
			}
		}
	}
	
	private void addNodes(List<Host> hosts) {
		// Add all nodes at once to avoid copying entire array multiple times.
		ArrayList<Node> list = new ArrayList<Node>(hosts.size());
		
		for (Host host : hosts) {
			try {
				Node node = new Node(this, host, connectionLimit, connectionTimeout);
				
				if (! nodeNames.contains(node.getName())) {
					addNodeNameAndAliases(node);
					
					list.add(node);					
				}
			}
			catch (Exception e) {
				if (Log.warnEnabled()) {
					Log.warn("Host " + host + " add failed: " + e.getMessage());
				}
			}
		}
		
		if (list.size() > 0) {
			addNodesCopy(list);
		}
	}
	
	private void addNodeNameAndAliases(Node node) {
		// Log.debug("Add node " + node);

		// Add node's name to global name set.
		// Names are only used in tend thread, so synchronization is not necessary.
		nodeNames.add(node.getName());
		
		// Add node's aliases to global alias set.
		// Aliases are only used in tend thread, so synchronization is not necessary.
		for (Host alias : node.getAliases()) {
			// Log.debug("Add alias " + alias);
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
			// Remove node name from cluster set. 
			// Log.debug("Remove node " + node);
			nodeNames.remove(node.getName());
			
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
			if (! findNode(node, nodesToRemove)) {
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
		
		if (node == null) {
			node = getRandomNode();
		}
		return node;
	}

	private synchronized Node getRandomNode() throws AerospikeException.InvalidNode {
		// Must synchronize with other non-tending threads, so nodeIndex is consistent.
		// Must also copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;

		if (nodeIndex >= nodeArray.length) {
			if (nodeArray.length == 0) {
				throw new AerospikeException.InvalidNode();
			}
			nodeIndex = 0;
		}
		return nodeArray[nodeIndex++];
	}

	public Node[] getNodes() {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;
		return nodeArray;
	}

	public Node getNode(String nodeName) throws AerospikeException.InvalidNode {
		// Must copy array reference for copy on write semantics to work.
		Node[] nodeArray = nodes;
		
		// Do not use nodeNames hashset because that is only modified by tend thread.
		// This method is called from other threads.
		for (Node node : nodeArray) {
			if (node.getName().equals(nodeName)) {
				return node;
			}
		}
		throw new AerospikeException.InvalidNode();
	}

	public void close() {
		tendValid = false;
	}
}

/*
 * Aerospike Client - Java Library
 *
 * Copyright 2012 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.command;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.Policy;

public final class BatchExecutor extends Thread {
	
	private final Policy policy;
	private final BatchNode batchNode;
	private final HashSet<String> binNames;
	private final HashMap<Key,BatchItem> keyMap;
	private final Record[] records;
	private final boolean[] existsArray;
	private final int readAttr;
	private Exception exception;

	public BatchExecutor(
		Policy policy,
		BatchNode batchNode,
		HashMap<Key,BatchItem> keyMap,
		HashSet<String> binNames,
		Record[] records,
		boolean[] existsArray,
		int readAttr
	) {
		this.policy = policy;
		this.batchNode = batchNode;
		this.keyMap = keyMap;
		this.binNames = binNames;
		this.records = records;
		this.existsArray = existsArray;
		this.readAttr = readAttr;
	}
	
	public void run() {
		try {
			for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
				
				if (records != null) {
					BatchCommandGet command = new BatchCommandGet(batchNode.node, keyMap, binNames, records, readAttr);
					command.executeBatch(policy, batchNamespace);
				}
				else {
					BatchCommandExists command = new BatchCommandExists(batchNode.node, keyMap, existsArray);
					command.executeBatch(policy, batchNamespace);
				}
			}
		}
		catch (Exception e) {
			exception = e;
		}
	}
	
	public Exception getException() {
		return exception;
	}

	public static void executeBatch
	(
		Cluster cluster,
		Policy policy, 
		Key[] keys,
		boolean[] existsArray, 
		Record[] records, 
		HashSet<String> binNames,
		int readAttr
	) throws AerospikeException {
		
		HashMap<Key,BatchItem> keyMap = new HashMap<Key,BatchItem>(keys.length);
		
		for (int i = 0; i < keys.length; i++) {
			Key key = keys[i];
			BatchItem item = keyMap.get(key);
			
			if (item == null) {
				item = new BatchItem(i);
				keyMap.put(key, item);
			}
			else {
				item.addDuplicate(i);
			}
		}
		
		int nodeCount = cluster.getNodes().length;
		int keysPerNode = keys.length / nodeCount + 10;

		// Split keys by server node.
		List<BatchNode> batchNodes = new ArrayList<BatchNode>(nodeCount+1);
				
		for (int i = 0; i < keys.length; i++) {
			Key key = keys[i];
			Partition partition = new Partition(key);			
			BatchNode batchNode;
			
			Node node = cluster.getNode(partition);
			batchNode = findBatchNode(batchNodes, node);
			
			if (batchNode == null) {
				batchNodes.add(new BatchNode(node, keysPerNode, key));
			}
			else {
				batchNode.addKey(key);
			}
		}
		
		// Dispatch the work to each node on a different thread.
		ArrayList<BatchExecutor> threads = new ArrayList<BatchExecutor>(batchNodes.size());

		for (BatchNode batchNode : batchNodes) {
			BatchExecutor thread = new BatchExecutor(policy, batchNode, keyMap, binNames, records, existsArray, readAttr);
			threads.add(thread);
			thread.start();
		}
		
		// Wait for all the threads to finish their work and return results.
		for (BatchExecutor thread : threads) {
			try {
				thread.join();
			}
			catch (Exception e) {
			}
		}
		
		// Throw an exception if an error occurred.
		for (BatchExecutor thread : threads) {
			Exception e = thread.getException();
			
			if (e != null) {
				if (e instanceof AerospikeException) {
					throw (AerospikeException)e;					
				}
				else {
					throw new AerospikeException(e);
				}
			}
		}
	}

	private static BatchNode findBatchNode(List<BatchNode> nodes, Node node) {
		for (BatchNode batchNode : nodes) {
			// Note: using pointer equality for performance.
			if (batchNode.node == node) {
				return batchNode;
			}
		}
		return null;
	}

	public static final class BatchNode {
		public final Node node;
		public final List<BatchNamespace> batchNamespaces;
		public final int keyCapacity;

		public BatchNode(Node node, int keyCapacity, Key key) {
			this.node = node;
			this.keyCapacity = keyCapacity;
			batchNamespaces = new ArrayList<BatchNamespace>(4);
			batchNamespaces.add(new BatchNamespace(key.namespace, keyCapacity, key));
		}
		
		public void addKey(Key key) {
			BatchNamespace batchNamespace = findNamespace(key.namespace);
			
			if (batchNamespace == null) {
				batchNamespaces.add(new BatchNamespace(key.namespace, keyCapacity, key));
			}
			else {
				batchNamespace.keys.add(key);
			}
		}
		
		private BatchNamespace findNamespace(String ns) {
			for (BatchNamespace batchNamespace : batchNamespaces) {
				// Note: use both pointer equality and equals.
				if (batchNamespace.namespace == ns || batchNamespace.namespace.equals(ns)) {
					return batchNamespace;
				}
			}
			return null;
		}
	}

	public static final class BatchNamespace {
		public final String namespace;
		public final ArrayList<Key> keys;

		public BatchNamespace(String namespace, int capacity, Key key) {
			this.namespace = namespace;
			keys = new ArrayList<Key>(capacity);
			keys.add(key);
		}
	}	
}

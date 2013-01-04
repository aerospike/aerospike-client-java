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
import java.util.HashSet;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.KeyStatus;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.Policy;

public final class BatchExecutor extends Thread {
	
	public static void executeBatch
	(
		Cluster cluster,
		Policy policy, 
		Key[] keys, 
		List<KeyStatus> keyStatusList, 
		List<Record> records, 
		HashSet<String> binNames
	) throws AerospikeException {
		
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
			BatchExecutor thread = new BatchExecutor(policy, batchNode, keyStatusList, records, binNames);
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
	
	private final Policy policy;
	private final BatchNode batchNode;
	private final List<KeyStatus> keyStatusList;
	private final List<Record> records;
	private final HashSet<String> binNames;
	private Exception exception;

	public BatchExecutor(
		Policy policy,
		BatchNode batchNode,
		List<KeyStatus> keyStatusList,
		List<Record> records,
		HashSet<String> binNames
	) {
		this.policy = policy;
		this.batchNode = batchNode;
		this.keyStatusList = keyStatusList;
		this.binNames = binNames;
		this.records = records;
	}
	
	public void run() {
		try {
			for (BatchNamespace batchNamespace : batchNode.batchNamespaces) {
				BatchCommand command;
				
				if (records != null) {
					command = new BatchCommandGet(batchNode.node, binNames, records);
				}
				else {
					command = new BatchCommandExists(batchNode.node, keyStatusList);
				}
				command.executeBatch(policy, batchNamespace);
			}
		}
		catch (Exception e) {
			exception = e;
		}
	}
	
	public Exception getException() {
		return exception;
	}
}

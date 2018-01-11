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
package com.aerospike.client.command;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.BatchPolicy;

public final class BatchNode {
	
	public static List<BatchNode> generateList(Cluster cluster, BatchPolicy policy, Key[] keys) throws AerospikeException {
		Node[] nodes = cluster.getNodes();
		
		if (nodes.length == 0) {
			throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
		}
		
		// Create initial key capacity for each node as average + 25%.
		int keysPerNode = keys.length / nodes.length;
		keysPerNode += keysPerNode >>> 2;

		// The minimum key capacity is 10.
		if (keysPerNode < 10) {
			keysPerNode = 10;
		}

		// Split keys by server node.
		List<BatchNode> batchNodes = new ArrayList<BatchNode>(nodes.length);
				
		for (int i = 0; i < keys.length; i++) {
			Partition partition = new Partition(keys[i]);						
			Node node = cluster.getMasterNode(partition);
			BatchNode batchNode = findBatchNode(batchNodes, node);
			
			if (batchNode == null) {
				batchNodes.add(new BatchNode(node, keysPerNode, i));
			}
			else {
				batchNode.addKey(i);
			}
		}
		return batchNodes;
	}

	public static List<BatchNode> generateList(Cluster cluster, BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
		Node[] nodes = cluster.getNodes();
		
		if (nodes.length == 0) {
			throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
		}
		
		// Create initial key capacity for each node as average + 25%.
		int max = records.size();
		int keysPerNode = max / nodes.length;
		keysPerNode += keysPerNode >>> 2;

		// The minimum key capacity is 10.
		if (keysPerNode < 10) {
			keysPerNode = 10;
		}

		// Split keys by server node.
		List<BatchNode> batchNodes = new ArrayList<BatchNode>(nodes.length);
				
		for (int i = 0; i < max; i++) {
			Partition partition = new Partition(records.get(i).key);						
			Node node = cluster.getMasterNode(partition);
			BatchNode batchNode = findBatchNode(batchNodes, node);
			
			if (batchNode == null) {
				batchNodes.add(new BatchNode(node, keysPerNode, i));
			}
			else {
				batchNode.addKey(i);
			}
		}
		return batchNodes;
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

	public final Node node;
	public int[] offsets;
	public int offsetsSize;
	public List<BatchNamespace> batchNamespaces;  // used by old batch only

	public BatchNode(Node node, int capacity, int offset) {
		this.node = node;
		this.offsets = new int[capacity];
		this.offsets[0] = offset;
		this.offsetsSize = 1;
	}
	
	public void addKey(int offset) {
		if (offsetsSize >= offsets.length) {
			int[] copy = new int[offsetsSize * 2];	        
			System.arraycopy(offsets, 0, copy, 0, offsetsSize);
			offsets = copy;
		}
		offsets[offsetsSize++] = offset;
	}
	
	public void splitByNamespace(Key[] keys) {
		String first = keys[offsets[0]].namespace;
		
		// Optimize for single namespace.
		if (isSingleNamespace(keys, first)) {
			batchNamespaces = new ArrayList<BatchNamespace>(1);
			batchNamespaces.add(new BatchNamespace(first, offsets, offsetsSize));
			return;
		}		
		
		// Process multiple namespaces.
		batchNamespaces = new ArrayList<BatchNamespace>(4);

		for (int i = 0; i < offsetsSize; i++) {
			int offset = offsets[i];
			String ns = keys[offset].namespace;
			BatchNamespace batchNamespace = findNamespace(batchNamespaces, ns);
			
			if (batchNamespace == null) {
				batchNamespaces.add(new BatchNamespace(ns, offsetsSize, offset));
			}
			else {
				batchNamespace.add(offset);
			}
		}
	}

	private boolean isSingleNamespace(Key[] keys, String first) {	
		for (int i = 1; i < offsetsSize; i++) {
			String ns = keys[offsets[i]].namespace;
			
			if (!(ns == first || ns.equals(first))) {
				return false;
			}
		}
		return true;
	}
	
	private BatchNamespace findNamespace(List<BatchNamespace> batchNamespaces, String ns) {
		for (BatchNamespace batchNamespace : batchNamespaces) {
			// Note: use both pointer equality and equals.
			if (batchNamespace.namespace == ns || batchNamespace.namespace.equals(ns)) {
				return batchNamespace;
			}
		}
		return null;
	}

	public static final class BatchNamespace {
		public final String namespace;
		public int[] offsets;
		public int offsetsSize;

		public BatchNamespace(String namespace, int capacity, int offset) {
			this.namespace = namespace;
			this.offsets = new int[capacity];
			this.offsets[0] = offset;
			this.offsetsSize = 1;
		}
		
		public BatchNamespace(String namespace, int[] offsets, int offsetsSize) {
			this.namespace = namespace;
			this.offsets = offsets;
			this.offsetsSize = offsetsSize;
		}

		public void add(int offset) {
			offsets[offsetsSize++] = offset;
		}
	}	 
}

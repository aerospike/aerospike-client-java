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
package com.aerospike.client.command;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;

public final class BatchNode {
	
	public static List<BatchNode> generateList(Cluster cluster, Key[] keys) throws AerospikeException {
		Node[] nodes = cluster.getNodes();
		
		if (nodes.length == 0) {
			throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
		}
		
		int nodeCount = nodes.length;
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
		return batchNodes;
	}

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

	private static BatchNode findBatchNode(List<BatchNode> nodes, Node node) {
		for (BatchNode batchNode : nodes) {
			// Note: using pointer equality for performance.
			if (batchNode.node == node) {
				return batchNode;
			}
		}
		return null;
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

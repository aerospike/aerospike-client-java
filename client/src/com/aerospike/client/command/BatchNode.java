/*
 * Copyright 2012-2020 Aerospike, Inc.
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

import com.aerospike.client.BatchRead;
import com.aerospike.client.Key;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Replica;

public final class BatchNode {

	public static List<BatchNode> generateList(Cluster cluster, BatchPolicy policy, Key[] keys) {
		Node[] nodes = cluster.validateNodes();

		// Create initial key capacity for each node as average + 25%.
		int keysPerNode = keys.length / nodes.length;
		keysPerNode += keysPerNode >>> 2;

		// The minimum key capacity is 10.
		if (keysPerNode < 10) {
			keysPerNode = 10;
		}

		final Replica replica = policy.replica;
		final Replica replicaSC = Partition.getReplicaSC(policy);

		// Split keys by server node.
		List<BatchNode> batchNodes = new ArrayList<BatchNode>(nodes.length);

		for (int i = 0; i < keys.length; i++) {
			Node node = Partition.getNodeBatchRead(cluster, keys[i], replica, replicaSC, 0, 0);
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

	public static List<BatchNode> generateList(
		Cluster cluster,
		BatchPolicy policy,
		Key[] keys,
		int sequenceAP,
		int sequenceSC,
		BatchNode batchSeed
	) {
		Node[] nodes = cluster.validateNodes();

		// Create initial key capacity for each node as average + 25%.
		int keysPerNode = batchSeed.offsetsSize / nodes.length;
		keysPerNode += keysPerNode >>> 2;

		// The minimum key capacity is 10.
		if (keysPerNode < 10) {
			keysPerNode = 10;
		}

		final Replica replica = policy.replica;
		final Replica replicaSC = Partition.getReplicaSC(policy);

		// Split keys by server node.
		List<BatchNode> batchNodes = new ArrayList<BatchNode>(nodes.length);

		for (int i = 0; i < batchSeed.offsetsSize; i++) {
			int offset = batchSeed.offsets[i];

			Node node = Partition.getNodeBatchRead(cluster, keys[offset], replica, replicaSC, sequenceAP, sequenceSC);
			BatchNode batchNode = findBatchNode(batchNodes, node);

			if (batchNode == null) {
				batchNodes.add(new BatchNode(node, keysPerNode, offset));
			}
			else {
				batchNode.addKey(offset);
			}
		}
		return batchNodes;
	}

	public static List<BatchNode> generateList(Cluster cluster, BatchPolicy policy, List<BatchRead> records) {
		Node[] nodes = cluster.validateNodes();

		// Create initial key capacity for each node as average + 25%.
		int max = records.size();
		int keysPerNode = max / nodes.length;
		keysPerNode += keysPerNode >>> 2;

		// The minimum key capacity is 10.
		if (keysPerNode < 10) {
			keysPerNode = 10;
		}

		final Replica replica = policy.replica;
		final Replica replicaSC = Partition.getReplicaSC(policy);

		// Split keys by server node.
		List<BatchNode> batchNodes = new ArrayList<BatchNode>(nodes.length);

		for (int i = 0; i < max; i++) {
			Node node = Partition.getNodeBatchRead(cluster, records.get(i).key, replica, replicaSC, 0, 0);
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

	public static List<BatchNode> generateList(
		Cluster cluster,
		BatchPolicy policy,
		List<BatchRead> records,
		int sequenceAP,
		int sequenceSC,
		BatchNode batchSeed
	) {
		Node[] nodes = cluster.validateNodes();

		// Create initial key capacity for each node as average + 25%.
		int keysPerNode = batchSeed.offsetsSize / nodes.length;
		keysPerNode += keysPerNode >>> 2;

		// The minimum key capacity is 10.
		if (keysPerNode < 10) {
			keysPerNode = 10;
		}

		final Replica replica = policy.replica;
		final Replica replicaSC = Partition.getReplicaSC(policy);

		// Split keys by server node.
		List<BatchNode> batchNodes = new ArrayList<BatchNode>(nodes.length);

		for (int i = 0; i < batchSeed.offsetsSize; i++) {
			int offset = batchSeed.offsets[i];

			Node node = Partition.getNodeBatchRead(cluster, records.get(offset).key, replica, replicaSC, sequenceAP, sequenceSC);
			BatchNode batchNode = findBatchNode(batchNodes, node);

			if (batchNode == null) {
				batchNodes.add(new BatchNode(node, keysPerNode, offset));
			}
			else {
				batchNode.addKey(offset);
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
}

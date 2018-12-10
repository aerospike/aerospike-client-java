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
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Replica;

public final class BatchNode {

	public static List<BatchNode> generateList(Cluster cluster, BatchPolicy policy, Key[] keys) {
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
			Node node = getNode(cluster, partition, policy.replica);
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

	public static List<BatchNode> generateList(Cluster cluster, BatchPolicy policy, List<BatchRead> records) {
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
			Node node = getNode(cluster, partition, policy.replica);
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

	private static Node getNode(Cluster cluster, Partition partition, Replica replica) {
		switch (replica) {
		default:
		case MASTER:
		case SEQUENCE:
			// Sequence uses master node because it always starts at master
			// and keys can't be transferred to other nodes on batch retry.
			return cluster.getMasterNode(partition);

		case PREFER_RACK:
			return getRackNode(cluster, partition);

		case MASTER_PROLES:
			return cluster.getMasterProlesNode(partition);

		case RANDOM:
			return cluster.getRandomNode();
		}
	}

	private static Node getRackNode(Cluster cluster, Partition partition) {
		// Must copy hashmap reference for copy on write semantics to work.
		HashMap<String,Partitions> map = cluster.partitionMap;
		Partitions partitions = map.get(partition.namespace);

		if (partitions == null) {
			throw new AerospikeException.InvalidNamespace(partition.namespace, map.size());
		}

		AtomicReferenceArray<Node>[] replicas = partitions.replicas;
		Node fallback = null;
		int sequence = 0;

		for (int i = 0; i < replicas.length; i++) {
			int index = Math.abs(sequence % replicas.length);
			Node node = replicas[index].get(partition.partitionId);

			if (node != null && node.isActive()) {
				if (node.hasRack(partition.namespace, cluster.rackId)) {
					return node;
				}

				if (fallback == null) {
					fallback = node;
				}
			}
			sequence++;
		}

		if (fallback != null) {
			return fallback;
		}

		Node[] nodeArray = cluster.getNodes();
		throw new AerospikeException.InvalidNode(nodeArray.length, partition);
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

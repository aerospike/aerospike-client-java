/*
 * Copyright 2012-2024 Aerospike, Inc.
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
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Replica;

public final class BatchNodeList {

	public interface IBatchStatus {
		public void batchKeyError(Key key, int index, AerospikeException ae, boolean inDoubt, boolean hasWrite);
		public void batchKeyError(AerospikeException ae);
	}

	/**
	 * Assign keys to nodes in initial batch attempt.
	 */
	public static List<BatchNode> generate(
		Cluster cluster,
		BatchPolicy policy,
		Key[] keys,
		BatchRecord[] records,
		boolean hasWrite,
		IBatchStatus status
	) {
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
		AerospikeException except = null;

		for (int i = 0; i < keys.length; i++) {
			Key key = keys[i];

			try {
				Node node;

				if (hasWrite) {
					node = Partition.getNodeBatchWrite(cluster, key, replica, null, 0);

					if (policy.tran != null) {
						policy.tran.addWrite(key);
					}
				}
				else {
					node = Partition.getNodeBatchRead(cluster, key, replica, replicaSC, null, 0, 0);
				}

				BatchNode batchNode = findBatchNode(batchNodes, node);

				if (batchNode == null) {
					batchNodes.add(new BatchNode(node, keysPerNode, i));
				}
				else {
					batchNode.addKey(i);
				}
			}
			catch (AerospikeException ae) {
				// This method only called on initialization, so inDoubt must be false.
				if (records != null) {
					records[i].setError(ae.getResultCode(), false);
				}
				else {
					status.batchKeyError(key, i, ae, false, hasWrite);
				}

				if (except == null) {
					except = ae;
				}
			}
		}

		if (except != null) {
			// Fatal if no key requests were generated on initialization.
			if (batchNodes.size() == 0) {
				throw except;
			}
			else {
				status.batchKeyError(except);
			}
		}
		return batchNodes;
	}

	/**
	 * Assign keys to nodes in batch node retry.
	 */
	public static List<BatchNode> generate(
		Cluster cluster,
		BatchPolicy policy,
		Key[] keys,
		BatchRecord[] records,
		int sequenceAP,
		int sequenceSC,
		BatchNode batchSeed,
		boolean hasWrite,
		IBatchStatus status
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
		AerospikeException except = null;

		for (int i = 0; i < batchSeed.offsetsSize; i++) {
			int offset = batchSeed.offsets[i];

			if (records[offset].resultCode != ResultCode.NO_RESPONSE) {
				// Do not retry keys that already have a response.
				continue;
			}

			Key key = keys[offset];

			try {
				Node node = hasWrite ?
					Partition.getNodeBatchWrite(cluster, key, replica, batchSeed.node, sequenceAP) :
					Partition.getNodeBatchRead(cluster, key, replica, replicaSC, batchSeed.node, sequenceAP, sequenceSC);

				BatchNode batchNode = findBatchNode(batchNodes, node);

				if (batchNode == null) {
					batchNodes.add(new BatchNode(node, keysPerNode, offset));
				}
				else {
					batchNode.addKey(offset);
				}
			}
			catch (AerospikeException ae) {
				// This method only called on retry, so commandSentCounter(2) will be greater than 1.
				records[offset].setError(ae.getResultCode(), Command.batchInDoubt(hasWrite, 2));

				if (except == null) {
					except = ae;
				}
			}
		}

		if (except != null) {
			status.batchKeyError(except);
		}
		return batchNodes;
	}

	/**
	 * Assign keys to nodes in batch node retry for async sequence listeners.
	 */
	public static List<BatchNode> generate(
		Cluster cluster,
		BatchPolicy policy,
		Key[] keys,
		boolean[] sent,
		int sequenceAP,
		int sequenceSC,
		BatchNode batchSeed,
		boolean hasWrite,
		IBatchStatus status
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
		AerospikeException except = null;

		for (int i = 0; i < batchSeed.offsetsSize; i++) {
			int offset = batchSeed.offsets[i];

			if (sent[offset]) {
				// Do not retry keys that already have a response.
				continue;
			}

			Key key = keys[offset];

			try {
				Node node = hasWrite ?
					Partition.getNodeBatchWrite(cluster, key, replica, batchSeed.node, sequenceAP) :
					Partition.getNodeBatchRead(cluster, key, replica, replicaSC, batchSeed.node, sequenceAP, sequenceSC);

				BatchNode batchNode = findBatchNode(batchNodes, node);

				if (batchNode == null) {
					batchNodes.add(new BatchNode(node, keysPerNode, offset));
				}
				else {
					batchNode.addKey(offset);
				}
			}
			catch (AerospikeException ae) {
				status.batchKeyError(key, offset, ae, Command.batchInDoubt(hasWrite, 2), hasWrite);

				if (except == null) {
					except = ae;
				}
			}
		}

		if (except != null) {
			status.batchKeyError(except);
		}
		return batchNodes;
	}

	/**
	 * Assign keys to nodes in batch node retry for batch reads.
	 */
	public static List<BatchNode> generate(
		Cluster cluster,
		BatchPolicy policy,
		Key[] keys,
		int sequenceAP,
		int sequenceSC,
		BatchNode batchSeed,
		boolean hasWrite,
		IBatchStatus status
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
		AerospikeException except = null;

		for (int i = 0; i < batchSeed.offsetsSize; i++) {
			int offset = batchSeed.offsets[i];
			Key key = keys[offset];

			// This method is only used to retry batch reads and the resultCode is not stored, so
			// retry all keys assigned to this node. Fortunately, it's rare to retry a node after
			// already receiving records and it's harmless to read the same record twice.

			try {
				Node node = hasWrite ?
					Partition.getNodeBatchWrite(cluster, key, replica, batchSeed.node, sequenceAP) :
					Partition.getNodeBatchRead(cluster, key, replica, replicaSC, batchSeed.node, sequenceAP, sequenceSC);

				BatchNode batchNode = findBatchNode(batchNodes, node);

				if (batchNode == null) {
					batchNodes.add(new BatchNode(node, keysPerNode, offset));
				}
				else {
					batchNode.addKey(offset);
				}
			}
			catch (AerospikeException ae) {
				if (except == null) {
					except = ae;
				}
			}
		}

		if (except != null) {
			status.batchKeyError(except);
		}
		return batchNodes;
	}

	/**
	 * Assign keys to nodes in initial batch attempt.
	 */
	public static List<BatchNode> generate(
		Cluster cluster,
		BatchPolicy policy,
		List<? extends BatchRecord> records,
		IBatchStatus status
	) {
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
		AerospikeException except = null;

		for (int i = 0; i < max; i++) {
			BatchRecord b = records.get(i);

			try {
				b.prepare();

				Node node;

				if (b.hasWrite) {
					node = Partition.getNodeBatchWrite(cluster, b.key, replica, null, 0);

					if (policy.tran != null) {
						policy.tran.addWrite(b.key);
					}
				}
				else {
					node = Partition.getNodeBatchRead(cluster, b.key, replica, replicaSC, null, 0, 0);
				}

				BatchNode batchNode = findBatchNode(batchNodes, node);

				if (batchNode == null) {
					batchNodes.add(new BatchNode(node, keysPerNode, i));
				}
				else {
					batchNode.addKey(i);
				}
			}
			catch (AerospikeException ae) {
				// This method only called on initialization, so inDoubt must be false.
				b.setError(ae.getResultCode(), false);

				if (except == null) {
					except = ae;
				}
			}
		}

		if (except != null) {
			// Fatal if no key requests were generated on initialization.
			if (batchNodes.size() == 0) {
				throw except;
			}
			else {
				status.batchKeyError(except);
			}
		}
		return batchNodes;
	}

	/**
	 * Assign keys to nodes in batch node retry.
	 */
	public static List<BatchNode> generate(
		Cluster cluster,
		BatchPolicy policy,
		List<? extends BatchRecord> records,
		int sequenceAP,
		int sequenceSC,
		BatchNode batchSeed,
		IBatchStatus status
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
		AerospikeException except = null;

		for (int i = 0; i < batchSeed.offsetsSize; i++) {
			int offset = batchSeed.offsets[i];
			BatchRecord b = records.get(offset);

			if (b.resultCode != ResultCode.NO_RESPONSE) {
				// Do not retry keys that already have a response.
				continue;
			}

			try {
				Node node = b.hasWrite ?
					Partition.getNodeBatchWrite(cluster, b.key, replica, batchSeed.node, sequenceAP) :
					Partition.getNodeBatchRead(cluster, b.key, replica, replicaSC, batchSeed.node, sequenceAP, sequenceSC);

				BatchNode batchNode = findBatchNode(batchNodes, node);

				if (batchNode == null) {
					batchNodes.add(new BatchNode(node, keysPerNode, offset));
				}
				else {
					batchNode.addKey(offset);
				}
			}
			catch (AerospikeException ae) {
				// This method only called on retry, so commandSentCounter(2) will be greater than 1.
				b.setError(ae.getResultCode(), Command.batchInDoubt(b.hasWrite, 2));

				if (except == null) {
					except = ae;
				}
			}
		}

		if (except != null) {
			status.batchKeyError(except);
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
}

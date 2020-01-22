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
package com.aerospike.client.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.Policy;

public final class PartitionTracker {
	private final PartitionStatus[] partitionsAll;
	private final int partitionBegin;
	private final int partitionCount;
	private final int nodeCapacity;
	private final Node nodeFilter;
	private List<NodePartitions> nodePartitionsList;
	private int partitionsCapacity;
	private int partitionsRequested;
	private int sleepBetweenRetries;
	public int socketTimeout;
	public int totalTimeout;
	public int iteration = 1;
	private long deadline;

	public PartitionTracker(Policy policy, Node[] nodes) {
		this.partitionBegin = 0;
		this.partitionCount = Node.PARTITIONS;
		this.nodeCapacity = nodes.length;
		this.nodeFilter = null;

		// Create initial partition capacity for each node as average + 25%.
		int ppn = Node.PARTITIONS / nodes.length;
		ppn += ppn >>> 2;
		this.partitionsCapacity = ppn;
		this.partitionsAll = init(policy);
	}

	public PartitionTracker(Policy policy, Node nodeFilter) {
		this.partitionBegin = 0;
		this.partitionCount = Node.PARTITIONS;
		this.nodeCapacity = 1;
		this.nodeFilter = nodeFilter;
		this.partitionsCapacity = this.partitionCount;
		this.partitionsAll = init(policy);
	}

	public PartitionTracker(Policy policy, Node[] nodes, PartitionFilter filter) {
		// Validate here instead of initial PartitionFilter constructor because total number of
		// cluster partitions may change on the server and PartitionFilter will never have access
		// to Cluster instance.  Use fixed number of partitions for now.
		if (! (filter.begin >= 0 && filter.begin < Node.PARTITIONS)) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid partition begin " + filter.begin +
				". Valid range: 0-" + (Node.PARTITIONS-1));
		}

		if (filter.count <= 0) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid partition count " + filter.count);
		}

		if (filter.begin + filter.count > Node.PARTITIONS) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid partition range (" + filter.begin +
				',' + filter.count + ')');
		}

		this.partitionBegin = filter.begin;
		this.partitionCount = filter.count;
		this.nodeCapacity = nodes.length;
		this.nodeFilter = null;
		this.partitionsCapacity = this.partitionCount;
		this.partitionsAll = init(policy);
	}

	private PartitionStatus[] init(Policy policy) {
		PartitionStatus[] partsAll = new PartitionStatus[partitionCount];

		for (int i = 0; i < partitionCount; i++) {
			partsAll[i] = new PartitionStatus(partitionBegin + i);
		}

		sleepBetweenRetries = policy.sleepBetweenRetries;
		socketTimeout = policy.socketTimeout;
		totalTimeout = policy.totalTimeout;

		if (totalTimeout > 0) {
			deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(totalTimeout);

			if (socketTimeout == 0 || socketTimeout > totalTimeout) {
				socketTimeout = totalTimeout;
			}
		}
		return partsAll;
	}

	public void setSleepBetweenRetries(int sleepBetweenRetries) {
		this.sleepBetweenRetries = sleepBetweenRetries;
	}

	public static boolean hasPartitionScan(Node[] nodes) {
		for (Node node : nodes) {
			if (! node.hasPartitionScan()) {
				return false;
			}
		}
		return true;
	}

	public List<NodePartitions> assignPartitionsToNodes(Cluster cluster, String namespace) {
		// System.out.println("Round " + iteration);
		List<NodePartitions> list = new ArrayList<NodePartitions>(nodeCapacity);

		HashMap<String,Partitions> map = cluster.partitionMap;
		Partitions partitions = map.get(namespace);

		if (partitions == null) {
			throw new AerospikeException.InvalidNamespace(namespace, map.size());
		}

		AtomicReferenceArray<Node> master = partitions.replicas[0];
		partitionsRequested = 0;

		for (PartitionStatus part : partitionsAll) {
			if (! part.done) {
				Node node = master.get(part.id);

				if (node == null) {
					throw new AerospikeException.InvalidNode(part.id);
				}

				// Use node name to check for single node equality because
				// partition map may be in transitional state between
				// the old and new node with the same name.
				if (nodeFilter != null && ! nodeFilter.getName().equals(node.getName())) {
					continue;
				}

				NodePartitions nps = findNode(list, node);

				if (nps == null) {
					// If the partition map is in a transitional state, multiple
					// NodePartitions instances (each with different partitions)
					// may be created for a single node.
					nps = new NodePartitions(node, partitionsCapacity);
					list.add(nps);
				}
				nps.addPartition(part);
				partitionsRequested++;
			}
		}
		nodePartitionsList = list;
		return list;
	}

	private NodePartitions findNode(List<NodePartitions> list, Node node) {
		for (NodePartitions nodePartition : list) {
			// Use pointer equality for performance.
			if (nodePartition.node == node) {
				return nodePartition;
			}
		}
		return null;
	}

	public void partitionDone(NodePartitions nodePartitions, int partitionId) {
		partitionsAll[partitionId - partitionBegin].done = true;
		nodePartitions.partsReceived++;
	}

	public void setDigest(Key key) {
		int partitionId = (Buffer.littleBytesToInt(key.digest, 0) & 0xFFFF) % Node.PARTITIONS;
		partitionsAll[partitionId - partitionBegin].digest = key.digest;
	}

	public boolean isComplete(Policy policy) {
		int partitionsReceived = 0;

		for (NodePartitions np : nodePartitionsList) {
			partitionsReceived += np.partsReceived;
			// System.out.println("Node " + np.node + ' ' + " partsFull=" + np.partsFull.size() + " partsPartial=" + np.partsPartial.size() + " partsReceived=" + np.partsReceived);
		}

		if (partitionsReceived >= partitionsRequested) {
			return true;
		}

		// Check if limits have been reached.
		if (iteration > policy.maxRetries) {
			AerospikeException ae = new AerospikeException(ResultCode.MAX_RETRIES_EXCEEDED, "Max retries exceeded: " + policy.maxRetries);
			ae.setPolicy(policy);
			ae.setIteration(iteration);
			throw ae;
		}

		if (policy.totalTimeout > 0) {
			// Check for total timeout.
			long remaining = deadline - System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(sleepBetweenRetries);

			if (remaining <= 0) {
				throw new AerospikeException.Timeout(policy, iteration);
			}

			// Convert back to milliseconds for remaining check.
			remaining = TimeUnit.NANOSECONDS.toMillis(remaining);

			if (remaining < totalTimeout) {
				totalTimeout = (int)remaining;

				if (socketTimeout > totalTimeout) {
					socketTimeout = totalTimeout;
				}
			}
		}

		// Prepare for next iteration.
		partitionsCapacity = partitionsRequested - partitionsReceived;
		iteration++;
		return false;
	}

	public boolean shouldRetry(AerospikeException ae) {
		switch (ae.getResultCode()) {
		case ResultCode.SERVER_NOT_AVAILABLE:
		case ResultCode.PARTITION_UNAVAILABLE:
		case ResultCode.TIMEOUT:
			return true;

		default:
			return false;
		}
	}

	public static class NodePartitions {
		public final Node node;
		public final List<PartitionStatus> partsFull;
		public final List<PartitionStatus> partsPartial;
		public int partsReceived;

		public NodePartitions(Node node, int capacity) {
			this.node = node;
			this.partsFull = new ArrayList<PartitionStatus>(capacity);
			this.partsPartial = new ArrayList<PartitionStatus>(capacity);
		}

		public void addPartition(PartitionStatus part) {
			if (part.digest == null) {
				partsFull.add(part);
			}
			else {
				partsPartial.add(part);
			}
		}
	}

	public static class PartitionStatus {
		public byte[] digest;
		public final int id;
		public boolean done;

		public PartitionStatus(int id) {
			this.id = id;
		}
	}
}

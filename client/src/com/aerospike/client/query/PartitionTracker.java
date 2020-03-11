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
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;

public final class PartitionTracker {
	private final PartitionStatus[] partitionsAll;
	private final int partitionsCapacity;
	private final int partitionBegin;
	private final int nodeCapacity;
	private final Node nodeFilter;
	private List<NodePartitions> nodePartitionsList;
	private long maxRecords;
	private int sleepBetweenRetries;
	public int socketTimeout;
	public int totalTimeout;
	public int iteration = 1;
	private long deadline;

	public PartitionTracker(ScanPolicy policy, Node[] nodes) {
		this((Policy)policy, nodes);
		this.maxRecords = policy.maxRecords;
	}

	public PartitionTracker(QueryPolicy policy, Node[] nodes) {
		this((Policy)policy, nodes);
		this.maxRecords = policy.maxRecords;
	}

	public PartitionTracker(Policy policy, Node[] nodes) {
		this.partitionBegin = 0;
		this.nodeCapacity = nodes.length;
		this.nodeFilter = null;

		// Create initial partition capacity for each node as average + 25%.
		int ppn = Node.PARTITIONS / nodes.length;
		ppn += ppn >>> 2;
		this.partitionsCapacity = ppn;
		this.partitionsAll = init(policy, Node.PARTITIONS, null);
	}

	public PartitionTracker(ScanPolicy policy, Node nodeFilter) {
		this((Policy)policy, nodeFilter);
		this.maxRecords = policy.maxRecords;
	}

	public PartitionTracker(QueryPolicy policy, Node nodeFilter) {
		this((Policy)policy, nodeFilter);
		this.maxRecords = policy.maxRecords;
	}

	public PartitionTracker(Policy policy, Node nodeFilter) {
		this.partitionBegin = 0;
		this.nodeCapacity = 1;
		this.nodeFilter = nodeFilter;
		this.partitionsCapacity = Node.PARTITIONS;
		this.partitionsAll = init(policy, Node.PARTITIONS, null);
	}

	public PartitionTracker(ScanPolicy policy, Node[] nodes, PartitionFilter filter) {
		this((Policy)policy, nodes, filter);
		this.maxRecords = policy.maxRecords;
	}

	public PartitionTracker(QueryPolicy policy, Node[] nodes, PartitionFilter filter) {
		this((Policy)policy, nodes, filter);
		this.maxRecords = policy.maxRecords;
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
		this.nodeCapacity = nodes.length;
		this.nodeFilter = null;
		this.partitionsCapacity = filter.count;
		this.partitionsAll = init(policy, filter.count, filter.digest);
	}

	private PartitionStatus[] init(Policy policy, int partitionCount, byte[] digest) {
		PartitionStatus[] partsAll = new PartitionStatus[partitionCount];

		for (int i = 0; i < partitionCount; i++) {
			partsAll[i] = new PartitionStatus(partitionBegin + i);
		}

		if (digest != null) {
			partsAll[0].digest = digest;
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

	public List<NodePartitions> assignPartitionsToNodes(Cluster cluster, String namespace) {
		//System.out.println("Round " + iteration);
		List<NodePartitions> list = new ArrayList<NodePartitions>(nodeCapacity);

		HashMap<String,Partitions> map = cluster.partitionMap;
		Partitions partitions = map.get(namespace);

		if (partitions == null) {
			throw new AerospikeException.InvalidNamespace(namespace, map.size());
		}

		AtomicReferenceArray<Node> master = partitions.replicas[0];

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

				NodePartitions np = findNode(list, node);

				if (np == null) {
					// If the partition map is in a transitional state, multiple
					// NodePartitions instances (each with different partitions)
					// may be created for a single node.
					np = new NodePartitions(node, partitionsCapacity);
					list.add(np);
				}
				np.addPartition(part);
			}
		}

		if (maxRecords > 0) {
			// Distribute maxRecords across nodes.
			int nodeSize = list.size();

			if (maxRecords < nodeSize) {
				// Only include nodes that have at least 1 record requested.
				nodeSize = (int)maxRecords;
				list = list.subList(0, nodeSize);
			}

			long max = maxRecords / nodeSize;
			int rem = (int)(maxRecords - (max * nodeSize));

			for (int i = 0; i < nodeSize; i++) {
				NodePartitions np = list.get(i);
				np.recordMax = i < rem ? max + 1 : max;
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

	public void setDigest(NodePartitions nodePartitions, Key key) {
		int partitionId = Partition.getPartitionId(key.digest);
		partitionsAll[partitionId - partitionBegin].digest = key.digest;
		nodePartitions.recordCount++;
	}

	public boolean isComplete(Policy policy) {
		long recordCount = 0;
		int partsRequested = 0;
		int partsReceived = 0;

		for (NodePartitions np : nodePartitionsList) {
			recordCount += np.recordCount;
			partsRequested += np.partsRequested;
			partsReceived += np.partsReceived;
			//System.out.println("Node " + np.node + " partsFull=" + np.partsFull.size() + " partsPartial=" + np.partsPartial.size() +
			//	" partsReceived=" + np.partsReceived + " recordsRequested=" + np.recordMax + " recordsReceived=" + np.recordCount);
		}

		if (partsReceived >= partsRequested || (maxRecords > 0 && recordCount >= maxRecords)) {
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
		if (maxRecords > 0) {
			maxRecords -= recordCount;
		}
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

	public static final class NodePartitions {
		public final Node node;
		public final List<PartitionStatus> partsFull;
		public final List<PartitionStatus> partsPartial;
		public long recordCount;
		public long recordMax;
		public int partsRequested;
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
			partsRequested++;
		}
	}

	public static final class PartitionStatus {
		public byte[] digest;
		public final int id;
		public boolean done;

		public PartitionStatus(int id) {
			this.id = id;
		}
	}
}

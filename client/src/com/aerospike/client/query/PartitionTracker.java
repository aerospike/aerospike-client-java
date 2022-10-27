/*
 * Copyright 2012-2022 Aerospike, Inc.
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

@SuppressWarnings("deprecation")
public final class PartitionTracker {
	private final PartitionStatus[] partitions;
	private final int partitionsCapacity;
	private final int partitionBegin;
	private final int nodeCapacity;
	private final Node nodeFilter;
	private final PartitionFilter partitionFilter;
	private List<NodePartitions> nodePartitionsList;
	private List<AerospikeException> exceptions;
	private long maxRecords;
	private int sleepBetweenRetries;
	public int socketTimeout;
	public int totalTimeout;
	public int iteration = 1;
	private long deadline;

	public PartitionTracker(ScanPolicy policy, Node[] nodes) {
		this((Policy)policy, nodes);
		setMaxRecords(policy.maxRecords);
	}

	public PartitionTracker(QueryPolicy policy, Statement stmt, Node[] nodes) {
		this((Policy)policy, nodes);
		setMaxRecords(policy, stmt);
	}

	private PartitionTracker(Policy policy, Node[] nodes) {
		this.partitionBegin = 0;
		this.nodeCapacity = nodes.length;
		this.nodeFilter = null;
		this.partitionFilter = null;

		// Create initial partition capacity for each node as average + 25%.
		int ppn = Node.PARTITIONS / nodes.length;
		ppn += ppn >>> 2;
		this.partitionsCapacity = ppn;
		this.partitions = initPartitions(Node.PARTITIONS, null);
		initTimeout(policy);
	}

	public PartitionTracker(ScanPolicy policy, Node nodeFilter) {
		this((Policy)policy, nodeFilter);
		setMaxRecords(policy.maxRecords);
	}

	public PartitionTracker(QueryPolicy policy, Statement stmt, Node nodeFilter) {
		this((Policy)policy, nodeFilter);
		setMaxRecords(policy, stmt);
	}

	private PartitionTracker(Policy policy, Node nodeFilter) {
		this.partitionBegin = 0;
		this.nodeCapacity = 1;
		this.nodeFilter = nodeFilter;
		this.partitionFilter = null;
		this.partitionsCapacity = Node.PARTITIONS;
		this.partitions = initPartitions(Node.PARTITIONS, null);
		initTimeout(policy);
	}

	public PartitionTracker(ScanPolicy policy, Node[] nodes, PartitionFilter filter) {
		this((Policy)policy, nodes, filter, policy.maxRecords);
	}

	public PartitionTracker(QueryPolicy policy, Statement stmt, Node[] nodes, PartitionFilter filter) {
		this((Policy)policy, nodes, filter, (stmt.maxRecords > 0)? stmt.maxRecords : policy.maxRecords);
	}

	private PartitionTracker(Policy policy, Node[] nodes, PartitionFilter filter, long maxRecords) {
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

		setMaxRecords(maxRecords);
		this.partitionBegin = filter.begin;
		this.nodeCapacity = nodes.length;
		this.nodeFilter = null;
		this.partitionsCapacity = filter.count;

		if (filter.partitions == null) {
			filter.partitions = initPartitions(filter.count, filter.digest);
			filter.retry = true;
		}
		else {
			// Retry all partitions when maxRecords not specified.
			if (maxRecords == 0) {
				filter.retry = true;
			}
		}
		this.partitions = filter.partitions;
		this.partitionFilter = filter;
		initTimeout(policy);
	}

	private void setMaxRecords(QueryPolicy policy, Statement stmt) {
		setMaxRecords((stmt.maxRecords > 0)? stmt.maxRecords : policy.maxRecords);
	}

	private void setMaxRecords(long maxRecords) {
		if (maxRecords < 0) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid maxRecords: " + maxRecords);
		}
		this.maxRecords = maxRecords;
	}

	private PartitionStatus[] initPartitions(int partitionCount, byte[] digest) {
		PartitionStatus[] partsAll = new PartitionStatus[partitionCount];

		for (int i = 0; i < partitionCount; i++) {
			partsAll[i] = new PartitionStatus(partitionBegin + i);
		}

		if (digest != null) {
			partsAll[0].digest = digest;
		}
		return partsAll;
	}

	private void initTimeout(Policy policy) {
		sleepBetweenRetries = policy.sleepBetweenRetries;
		socketTimeout = policy.socketTimeout;
		totalTimeout = policy.totalTimeout;

		if (totalTimeout > 0) {
			deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(totalTimeout);

			if (socketTimeout == 0 || socketTimeout > totalTimeout) {
				socketTimeout = totalTimeout;
			}
		}
	}

	public void setSleepBetweenRetries(int sleepBetweenRetries) {
		this.sleepBetweenRetries = sleepBetweenRetries;
	}

	public List<NodePartitions> assignPartitionsToNodes(Cluster cluster, String namespace) {
		//System.out.println("Round " + iteration);
		List<NodePartitions> list = new ArrayList<NodePartitions>(nodeCapacity);

		HashMap<String,Partitions> map = cluster.partitionMap;
		Partitions parts = map.get(namespace);

		if (parts == null) {
			throw new AerospikeException.InvalidNamespace(namespace, map.size());
		}

		AtomicReferenceArray<Node> master = parts.replicas[0];
		boolean retry = (partitionFilter == null || partitionFilter.retry) && iteration == 1;

		for (PartitionStatus part : partitions) {
			if (retry || part.retry) {
				Node masterNode = master.get(part.id);
				Node node;

				if (iteration == 1 || ! part.unavailable || part.masterNode != masterNode) {
					node = part.masterNode = masterNode;
					part.replicaIndex = 0;
				}
				else {
					// Partition was unavailable in the previous iteration and the
					// master node has not changed. Switch replica.
					part.replicaIndex++;

					if (part.replicaIndex >= parts.replicas.length) {
						part.replicaIndex = 0;
					}

					node = parts.replicas[part.replicaIndex].get(part.id);
				}

				if (node == null) {
					throw new AerospikeException.InvalidNode(part.id);
				}

				part.unavailable = false;
				part.retry = false;

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

		int nodeSize = list.size();

		if (nodeSize <= 0) {
			throw new AerospikeException.InvalidNode("No nodes were assigned");
		}

		// Set global retry to true because scan/query may terminate early and all partitions
		// will need to be retried if the PartitionFilter instance is reused in a new scan/query.
		// Global retry will be set to false if the scan/query completes normally and maxRecords
		// is specified.
		if (partitionFilter != null) {
			partitionFilter.retry = true;
		}

		if (maxRecords > 0) {
			// Distribute maxRecords across nodes.
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

	public void partitionUnavailable(NodePartitions nodePartitions, int partitionId) {
		PartitionStatus ps = partitions[partitionId - partitionBegin];
		ps.unavailable = true;
		ps.retry = true;
		nodePartitions.partsUnavailable++;
	}

	public void setDigest(NodePartitions nodePartitions, Key key) {
		int partitionId = Partition.getPartitionId(key.digest);
		partitions[partitionId - partitionBegin].digest = key.digest;
		nodePartitions.recordCount++;
	}

	public void setLast(NodePartitions nodePartitions, Key key, long bval) {
		int partitionId = Partition.getPartitionId(key.digest);
		PartitionStatus ps = partitions[partitionId - partitionBegin];
		ps.digest = key.digest;
		ps.bval = bval;
		nodePartitions.recordCount++;
	}

	public boolean isComplete(Cluster cluster, Policy policy) {
		long recordCount = 0;
		int partsUnavailable = 0;

		for (NodePartitions np : nodePartitionsList) {
			recordCount += np.recordCount;
			partsUnavailable += np.partsUnavailable;

			//System.out.println("Node " + np.node + " partsFull=" + np.partsFull.size() +
			//  " partsPartial=" + np.partsPartial.size() + " partsUnavailable=" + np.partsUnavailable +
			//  " recordsRequested=" + np.recordMax + " recordsReceived=" + np.recordCount);
		}

		if (partsUnavailable == 0) {
			if (maxRecords == 0) {
				if (partitionFilter != null) {
					partitionFilter.done = true;
				}
			}
			else if (iteration > 1) {
				if (partitionFilter != null) {
					// If errors occurred on a node, only that node's partitions are retried in the
					// next iteration. If that node finally succeeds, the other original nodes still
					// need to be retried if partition state is reused in the next scan/query command.
					// Force retry on all node partitions.
					partitionFilter.retry = true;
					partitionFilter.done = false;
				}
			}
			else {
				if (cluster.hasPartitionQuery) {
					// Server version >= 6.0 will return all records for each node up to
					// that node's max. If node's record count reached max, there still
					// may be records available for that node.
					boolean done = true;

					for (NodePartitions np : nodePartitionsList) {
						if (np.recordCount >= np.recordMax) {
							markRetry(np);
							done = false;
						}
					}

					if (partitionFilter != null) {
						// Set global retry to false because only specific node partitions
						// should be retried.
						partitionFilter.retry = false;
						partitionFilter.done = done;
					}
				}
				else {
					// Servers version < 6.0 can return less records than max and still
					// have more records for each node, so the node is only done if no
					// records were retrieved for that node.
					for (NodePartitions np : nodePartitionsList) {
						if (np.recordCount > 0) {
							markRetry(np);
						}
					}

					if (partitionFilter != null) {
						// Set global retry to false because only specific node partitions
						// should be retried.
						partitionFilter.retry = false;
						partitionFilter.done = recordCount == 0;
					}
				}
			}
			return true;
		}

		if (maxRecords > 0 && recordCount >= maxRecords) {
			return true;
		}

		// Check if limits have been reached.
		if (iteration > policy.maxRetries) {
			StringBuilder sb = new StringBuilder(2048);
			sb.append("Max retries exceeded: ");
			sb.append(policy.maxRetries);
			sb.append(System.lineSeparator());

			if (exceptions != null) {
				sb.append("sub-exceptions:");
				sb.append(System.lineSeparator());

				for (AerospikeException ae : exceptions) {
					sb.append(ae.getMessage());
					sb.append(System.lineSeparator());
				}
			}

			AerospikeException ae = new AerospikeException(ResultCode.MAX_RETRIES_EXCEEDED, sb.toString());
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

	public boolean shouldRetry(NodePartitions nodePartitions, AerospikeException ae) {
		ae.setIteration(iteration);

		switch (ae.getResultCode()) {
		case ResultCode.SERVER_NOT_AVAILABLE:
		case ResultCode.TIMEOUT:
		case ResultCode.INDEX_NOTFOUND:
		case ResultCode.INDEX_NOTREADABLE:
			// Multiple scan/query threads may call this method, so exception
			// list must be modified under lock.
			synchronized(this) {
				if (exceptions == null) {
					exceptions = new ArrayList<AerospikeException>();
				}
				exceptions.add(ae);
			}
			markRetry(nodePartitions);
			nodePartitions.partsUnavailable = nodePartitions.partsFull.size() + nodePartitions.partsPartial.size();
			return true;

		default:
			return false;
		}
	}

	private void markRetry(NodePartitions nodePartitions) {
		for (PartitionStatus ps : nodePartitions.partsFull) {
			ps.retry = true;
		}

		for (PartitionStatus ps : nodePartitions.partsPartial) {
			ps.retry = true;
		}
	}

	public void partitionError() {
		// Mark all partitions for retry on fatal errors.
		if (partitionFilter != null) {
			partitionFilter.retry = true;
		}
	}

	public static final class NodePartitions {
		public final Node node;
		public final List<PartitionStatus> partsFull;
		public final List<PartitionStatus> partsPartial;
		public long recordCount;
		public long recordMax;
		public int partsUnavailable;

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
}

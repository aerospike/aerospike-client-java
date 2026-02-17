/*
 * Copyright 2012-2023 Aerospike, Inc.
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
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.cluster.Partition;
import com.aerospike.client.cluster.Partitions;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.Replica;
import com.aerospike.client.policy.ScanPolicy;

@SuppressWarnings("deprecation")
public final class PartitionTracker {
	private final PartitionStatus[] partitions;
	private final int partitionsCapacity;
	private final int partitionBegin;
	private final int nodeCapacity;
	private final Node nodeFilter;
	private final PartitionFilter partitionFilter;
	private final Replica replica;
	private List<NodePartitions> nodePartitionsList;
	private List<AerospikeException> exceptions;
	private AtomicLong recordCount;
	private long maxRecords;
	private int sleepBetweenRetries;
	public int socketTimeout;
	public int totalTimeout;
	public int iteration = 1;
	private long deadline;
	private double sleepMultiplier;

	public PartitionTracker(ScanPolicy policy, Node[] nodes) {
		this((Policy)policy, nodes.length);
		setMaxRecords(policy.maxRecords);
	}

	public PartitionTracker(ScanPolicy policy, int nodeCapacity) {
		this((Policy)policy, nodeCapacity);
		setMaxRecords(policy.maxRecords);
	}

	public PartitionTracker(QueryPolicy policy, Statement stmt, Node[] nodes) {
		this((Policy)policy, nodes.length);
		setMaxRecords(policy, stmt);
	}

	public PartitionTracker(QueryPolicy policy, Statement stmt, int nodeCapacity) {
		this((Policy)policy, nodeCapacity);
		setMaxRecords(policy, stmt);
	}

	private PartitionTracker(Policy policy, int nodeCapacity) {
		this.partitionBegin = 0;
		this.nodeCapacity = nodeCapacity;
		this.nodeFilter = null;
		this.partitionFilter = null;
		this.replica = policy.replica;

		// Create initial partition capacity for each node as average + 25%.
		int ppn = Node.PARTITIONS / nodeCapacity;
		ppn += ppn >>> 2;
		this.partitionsCapacity = ppn;
		this.partitions = initPartitions(Node.PARTITIONS, null);
		init(policy);
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
		this.replica = policy.replica;
		this.partitions = initPartitions(Node.PARTITIONS, null);
		init(policy);
	}

	public PartitionTracker(ScanPolicy policy, Node[] nodes, PartitionFilter filter) {
		this((Policy)policy, nodes, filter, policy.maxRecords);
	}

	public PartitionTracker(ScanPolicy policy, int nodeCapacity, PartitionFilter filter) {
		this((Policy)policy, nodeCapacity, filter, policy.maxRecords);
	}

	public PartitionTracker(QueryPolicy policy, Statement stmt, Node[] nodes, PartitionFilter filter) {
		this((Policy)policy, nodes, filter, (stmt.maxRecords > 0) ? stmt.maxRecords : policy.maxRecords);
	}

	public PartitionTracker(QueryPolicy policy, Statement stmt, int nodeCapacity, PartitionFilter filter) {
		this((Policy)policy, nodeCapacity, filter, (stmt.maxRecords > 0) ?
			stmt.maxRecords : policy.maxRecords);
	}

	private PartitionTracker(Policy policy, Node[] nodes, PartitionFilter filter, long maxRecords) {
		this(policy, nodes.length, filter, maxRecords);
	}

	private PartitionTracker(Policy policy, int nodeCapacity, PartitionFilter filter, long maxRecords) {
		// Validate here instead of initial PartitionFilter constructor because total number of
		// cluster partitions may change on the server and PartitionFilter will never have access
		// to Cluster instance.  Use fixed number of partitions for now.
		if (!(filter.begin >= 0 && filter.begin < Node.PARTITIONS)) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid partition begin " + filter.begin +
				". Valid range: 0-" + (Node.PARTITIONS - 1));
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
		this.nodeCapacity = nodeCapacity;
		this.nodeFilter = null;
		this.partitionsCapacity = filter.count;
		this.replica = policy.replica;

		if (filter.partitions == null) {
			filter.partitions = initPartitions(filter.count, filter.digest);
			filter.retry = true;
		}
		else {
			// Retry all partitions when maxRecords not specified.
			if (maxRecords == 0) {
				filter.retry = true;
			}

			// Reset replica sequence and last node used.
			for (PartitionStatus part : filter.partitions) {
				part.sequence = 0;
				part.node = null;
			}
		}
		this.partitions = filter.partitions;
		this.partitionFilter = filter;
		init(policy);
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

	private void init(Policy policy) {
		sleepBetweenRetries = policy.sleepBetweenRetries;
		socketTimeout = policy.socketTimeout;
		totalTimeout = policy.totalTimeout;
		sleepMultiplier = policy.sleepMultiplier;

		if (totalTimeout > 0) {
			deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(totalTimeout);

			if (socketTimeout == 0 || socketTimeout > totalTimeout) {
				socketTimeout = totalTimeout;
			}
		}

		if (replica == Replica.RANDOM) {
			throw new AerospikeException(ResultCode.PARAMETER_ERROR, "Invalid replica: " + replica.toString());
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

		Partition p = new Partition(namespace, replica);
		boolean retry = (partitionFilter == null || partitionFilter.retry) && iteration == 1;

		for (PartitionStatus part : partitions) {
			if (retry || part.retry) {
				Node node = p.getNodeQuery(cluster, parts, part);

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

		recordCount = null;

		if (maxRecords > 0) {
			if (maxRecords >= nodeSize) {
				// Distribute maxRecords across nodes.
				long max = maxRecords / nodeSize;
				int rem = (int)(maxRecords - (max * nodeSize));

				for (int i = 0; i < nodeSize; i++) {
					NodePartitions np = list.get(i);
					np.recordMax = i < rem ? max + 1 : max;
				}
			}
			else {
				// If maxRecords < nodeSize, the scan/query could consistently return 0 records even
				// when some records are still available in nodes that were not included in the
				// maxRecords distribution. Therefore, ensure each node receives at least one max record
				// allocation and filter out excess records when receiving records from the server.
				for (int i = 0; i < nodeSize; i++) {
					NodePartitions np = list.get(i);
					np.recordMax = 1;
				}

				// Track records returned for this iteration.
				recordCount = new AtomicLong();
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
		ps.retry = true;
		ps.sequence++;
		nodePartitions.partsUnavailable++;
		addException(nodePartitions.node, new AerospikeException(ResultCode.PARTITION_UNAVAILABLE,
				"Partition " + partitionId + " unavailable"));
	}

	/**
	 * Update the last seen digest for a partition.
	 * Internal use only.
	 *
	 * @param partitionId partition id
	 * @param digest      the last seen digest.
	 */
	void setDigest(int partitionId, byte[] digest) {
		for (PartitionStatus ps : partitions) {
			if (ps.id == partitionId) {
				ps.digest = digest;
			}
		}
	}

	public void setDigest(NodePartitions nodePartitions, Key key) {
		int partitionId = Partition.getPartitionId(key.digest);
		partitions[partitionId - partitionBegin].digest = key.digest;
		nodePartitions.recordCount++;
	}

	/**
	 * Update the last seen value for a partition.
	 * Internal use only.
	 *
	 * @param partitionId partition id
	 * @param digest      the record digest
	 * @param bval        the last seen value.
	 * @param retry       indicates if this partition should be retried.
	 */
	void setLast(int partitionId, byte[] digest, long bval, boolean retry) {
		int pIndex = partitionId - partitionBegin;
		if (pIndex < partitions.length) {
			PartitionStatus ps = partitions[pIndex];
			assert ps.id == partitionId;
			ps.bval = bval;
			ps.digest = digest;
			ps.retry = retry;
		}
	}

	public void setLast(NodePartitions nodePartitions, Key key, long bval) {
		int partitionId = Partition.getPartitionId(key.digest);
		PartitionStatus ps = partitions[partitionId - partitionBegin];
		ps.digest = key.digest;
		ps.bval = bval;
		nodePartitions.recordCount++;
	}

	public boolean allowRecord(NodePartitions nodePartitions) {
		if (recordCount == null || recordCount.incrementAndGet() <= maxRecords) {
			return true;
		}

		// Record was returned, but would exceed maxRecords. Discard record
		// and mark node for retry on next scan/query page.
		nodePartitions.retry = true;
		return false;
	}

	public boolean isComplete(Cluster cluster, Policy policy) {
		return isComplete(cluster.hasPartitionQuery, policy, nodePartitionsList);
	}

	public boolean isComplete(boolean hasPartitionQuery, Policy policy, List<NodePartitions> nodePartitionsList) {
		long recCount = 0;
		int partsUnavailable = 0;

		for (NodePartitions np : nodePartitionsList) {
			recCount += np.recordCount;
			partsUnavailable += np.partsUnavailable;

			//System.out.println("Node " + np.node + " partsFull=" + np.partsFull.size() +
			//  " partsPartial=" + np.partsPartial.size() + " partsUnavailable=" + np.partsUnavailable +
			//  " recordsRequested=" + np.recordMax + " recordsReceived=" + np.recordCount);
		}

		if (partsUnavailable == 0) {
			if (maxRecords == 0) {
				if (partitionFilter != null) {
					partitionFilter.retry = false;
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
				if (hasPartitionQuery) {
					// Server version >= 6.0 will return all records for each node up to
					// that node's max. If node's record count reached max, there still
					// may be records available for that node.
					boolean done = true;

					for (NodePartitions np : nodePartitionsList) {
						if (np.retry || np.recordCount >= np.recordMax) {
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
						if (np.retry || np.recordCount > 0) {
							markRetry(np);
						}
					}

					if (partitionFilter != null) {
						// Set global retry to false because only specific node partitions
						// should be retried.
						partitionFilter.retry = false;
						partitionFilter.done = recCount == 0;
					}
				}
			}
			return true;
		}

		if (maxRecords > 0 && recCount >= maxRecords) {
			return true;
		}

		// Check if limits have been reached.
		if (iteration > policy.maxRetries) {
			// Use last sub-error code received.
			AerospikeException last = exceptions.get(exceptions.size() - 1);

			// Include all sub-errors in error message.
			StringBuilder sb = new StringBuilder(2048);
			sb.append(last.getBaseMessage());
			sb.append(System.lineSeparator());
			sb.append("sub-exceptions:");
			sb.append(System.lineSeparator());

			for (AerospikeException ae : exceptions) {
				sb.append(ae.getMessage());
				sb.append(System.lineSeparator());
			}

			AerospikeException ae = new AerospikeException(last.getResultCode(), sb.toString());
			ae.setNode(last.getNode());
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
			maxRecords -= recCount;
		}
		if (sleepMultiplier > 1) {
			sleepBetweenRetries = (int) Math.round(sleepBetweenRetries * sleepMultiplier);
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
			addException(nodePartitions.node, ae);
			markRetrySequence(nodePartitions);
			nodePartitions.partsUnavailable = nodePartitions.partsFull.size() + nodePartitions.partsPartial.size();
			return true;

		default:
			return false;
		}
	}

	private void addException(Node node, AerospikeException ae) {
		// Multiple scan/query threads may call this method, so exception
		// list must be modified under lock.
		synchronized(this) {
			if (exceptions == null) {
				exceptions = new ArrayList<AerospikeException>();
			}
			ae.setNode(node);
			ae.setIteration(iteration);
			exceptions.add(ae);
		}
	}

	private void markRetrySequence(NodePartitions nodePartitions) {
		// Mark retry for next replica.
		for (PartitionStatus ps : nodePartitions.partsFull) {
			ps.retry = true;
			ps.sequence++;
		}

		for (PartitionStatus ps : nodePartitions.partsPartial) {
			ps.retry = true;
			ps.sequence++;
		}
	}

	private void markRetry(NodePartitions nodePartitions) {
		// Mark retry for same replica.
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
		public boolean retry;

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

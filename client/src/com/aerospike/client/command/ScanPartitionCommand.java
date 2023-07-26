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
package com.aerospike.client.command;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.LatencyType;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.PartitionTracker;
import com.aerospike.client.query.PartitionTracker.NodePartitions;

public final class ScanPartitionCommand extends MultiCommand {
	private final ScanPolicy scanPolicy;
	private final String setName;
	private final String[] binNames;
	private final ScanCallback callback;
	private final long taskId;
	private final PartitionTracker tracker;
	private final NodePartitions nodePartitions;

	public ScanPartitionCommand(
		Cluster cluster,
		ScanPolicy scanPolicy,
		String namespace,
		String setName,
		String[] binNames,
		ScanCallback callback,
		long taskId,
		PartitionTracker tracker,
		NodePartitions nodePartitions
	) {
		super(cluster, scanPolicy, nodePartitions.node, namespace, tracker.socketTimeout, tracker.totalTimeout);
		this.scanPolicy = scanPolicy;
		this.setName = setName;
		this.binNames = binNames;
		this.callback = callback;
		this.taskId = taskId;
		this.tracker = tracker;
		this.nodePartitions = nodePartitions;
	}

	@Override
	public void execute() {
		try {
			executeCommand();
		}
		catch (AerospikeException ae) {
			if (! tracker.shouldRetry(nodePartitions, ae)) {
				throw ae;
			}
		}
	}

	@Override
	protected LatencyType getLatencyType() {
		return LatencyType.QUERY;
	}

	@Override
	protected void writeBuffer() {
		setScan(cluster, scanPolicy, namespace, setName, binNames, taskId, nodePartitions);
	}

	@Override
	protected boolean parseRow() {
		Key key = parseKey(fieldCount, null);

		if ((info3 & Command.INFO3_PARTITION_DONE) != 0) {
			// When an error code is received, mark partition as unavailable
			// for the current round. Unavailable partitions will be retried
			// in the next round. Generation is overloaded as partitionId.
			if (resultCode != 0) {
				tracker.partitionUnavailable(nodePartitions, generation);
			}
			return true;
		}

		if (resultCode != 0) {
			throw new AerospikeException(resultCode);
		}

		Record record = parseRecord();

		if (! valid) {
			throw new AerospikeException.ScanTerminated();
		}

		if (tracker.allowRecord()) {
			callback.scanCallback(key, record);
			tracker.setDigest(nodePartitions, key);
		}
		return true;
	}
}

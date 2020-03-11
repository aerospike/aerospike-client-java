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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.cluster.Cluster;
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
			if (! tracker.shouldRetry(ae)) {
				throw ae;
			}
		}
	}

	@Override
	protected void writeBuffer() {
		setScan(scanPolicy, namespace, setName, binNames, taskId, nodePartitions);
	}

	@Override
	protected void parseRow(Key key) {
		if ((info3 & Command.INFO3_PARTITION_DONE) != 0) {
			tracker.partitionDone(nodePartitions, generation);
			return;
		}
		tracker.setDigest(nodePartitions, key);

		Record record = parseRecord();

		if (! valid) {
			throw new AerospikeException.ScanTerminated();
		}

		callback.scanCallback(key, record);
	}
}

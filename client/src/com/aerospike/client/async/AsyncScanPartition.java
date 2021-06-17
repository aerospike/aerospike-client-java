/*
 * Copyright 2012-2021 Aerospike, Inc.
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
package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.command.Command;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.PartitionTracker;
import com.aerospike.client.query.PartitionTracker.NodePartitions;

public final class AsyncScanPartition extends AsyncMultiCommand {
	private final ScanPolicy scanPolicy;
	private final RecordSequenceListener listener;
	private final String namespace;
	private final String setName;
	private final String[] binNames;
	private final long taskId;
	private final PartitionTracker tracker;
	private final NodePartitions nodePartitions;

	public AsyncScanPartition(
		AsyncMultiExecutor parent,
		ScanPolicy scanPolicy,
		RecordSequenceListener listener,
		String namespace,
		String setName,
		String[] binNames,
		long taskId,
		PartitionTracker tracker,
		NodePartitions nodePartitions
	) {
		super(parent, nodePartitions.node, scanPolicy, tracker.socketTimeout, tracker.totalTimeout);
		this.scanPolicy = scanPolicy;
		this.listener = listener;
		this.namespace = namespace;
		this.setName = setName;
		this.binNames = binNames;
		this.taskId = taskId;
		this.tracker = tracker;
		this.nodePartitions = nodePartitions;
		deserializeKeys = true;
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
		listener.onRecord(key, record);
	}

	@Override
	protected void onFailure(AerospikeException ae) {
		if (tracker.shouldRetry(ae)) {
			parent.childSuccess(node);
			return;
		}
		parent.childFailure(ae);
	}
}

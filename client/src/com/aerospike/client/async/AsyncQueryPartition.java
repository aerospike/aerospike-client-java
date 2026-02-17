/*
 * Copyright 2012-2025 Aerospike, Inc.
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
import com.aerospike.client.metrics.LatencyType;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.BVal;
import com.aerospike.client.query.PartitionTracker;
import com.aerospike.client.query.PartitionTracker.NodePartitions;
import com.aerospike.client.query.Statement;

public final class AsyncQueryPartition extends AsyncMultiCommand {
	private final AsyncMultiExecutor parent;
	private final RecordSequenceListener listener;
	private final Statement statement;
	private final long taskId;
	private final PartitionTracker tracker;
	private final NodePartitions nodePartitions;

	public AsyncQueryPartition(
		AsyncMultiExecutor parent,
		QueryPolicy policy,
		RecordSequenceListener listener,
		Statement statement,
		long taskId,
		PartitionTracker tracker,
		NodePartitions nodePartitions
	) {
		super(nodePartitions.node, policy, tracker.socketTimeout, tracker.totalTimeout, statement.getNamespace());
		this.parent = parent;
		this.listener = listener;
		this.statement = statement;
		this.taskId = taskId;
		this.tracker = tracker;
		this.nodePartitions = nodePartitions;
	}

	@Override
	protected LatencyType getLatencyType() {
		return LatencyType.QUERY;
	}

	@Override
	protected void writeBuffer() {
		setQuery(parent.cluster, policy, statement, taskId, false, nodePartitions);
	}

	@Override
	protected void parseRow() {
		BVal bval = new BVal();
		Key key = parseKey(fieldCount, bval);

		if ((info3 & Command.INFO3_PARTITION_DONE) != 0) {
			// When an error code is received, mark partition as unavailable
			// for the current round. Unavailable partitions will be retried
			// in the next round. Generation is overloaded as partitionId.
			if (resultCode != 0) {
				tracker.partitionUnavailable(nodePartitions, generation);
			}
			return;
		}

		if (resultCode != 0) {
			throw new AerospikeException(resultCode);
		}

		Record record = parseRecord();

		if (tracker.allowRecord(nodePartitions)) {
			listener.onRecord(key, record);
			tracker.setLast(nodePartitions, key, bval.val);
		}
	}

	@Override
	protected void onSuccess() {
		parent.childSuccess(node);
	}

	@Override
	protected void onFailure(AerospikeException ae) {
		if (tracker.shouldRetry(nodePartitions, ae)) {
			parent.childSuccess(node);
			return;
		}
		parent.childFailure(ae);
	}
}

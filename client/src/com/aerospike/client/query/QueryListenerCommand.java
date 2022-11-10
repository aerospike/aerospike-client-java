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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.PartitionTracker.NodePartitions;

public final class QueryListenerCommand extends MultiCommand {

	private final Statement statement;
	private final long taskId;
	private final QueryListener listener;
	private final PartitionTracker tracker;
	private final NodePartitions nodePartitions;

	public QueryListenerCommand(
		Cluster cluster,
		Policy policy,
		Statement statement,
		long taskId,
		QueryListener listener,
		PartitionTracker tracker,
		NodePartitions nodePartitions
	) {
		super(cluster, policy, nodePartitions.node, statement.namespace, tracker.socketTimeout, tracker.totalTimeout);
		this.statement = statement;
		this.taskId = taskId;
		this.listener = listener;
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
	protected final void writeBuffer() {
		setQuery(cluster, policy, statement, taskId, false, nodePartitions);
	}

	@Override
	protected boolean parseRow() {
		BVal bval = new BVal();
		Key key = parseKey(fieldCount, bval);

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
			throw new AerospikeException.QueryTerminated();
		}

		listener.onRecord(key, record);
		tracker.setLast(nodePartitions, key, bval.val);
		return true;
	}
}

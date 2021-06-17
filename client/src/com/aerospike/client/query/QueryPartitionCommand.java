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
package com.aerospike.client.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Command;
import com.aerospike.client.command.MultiCommand;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.PartitionTracker.NodePartitions;

public final class QueryPartitionCommand extends MultiCommand {

	private final Statement statement;
	private final RecordSet recordSet;
	private final PartitionTracker tracker;
	private final NodePartitions nodePartitions;

	public QueryPartitionCommand(
		Cluster cluster,
		Node node,
		Policy policy,
		Statement statement,
		RecordSet recordSet,
		PartitionTracker tracker,
		NodePartitions nodePartitions
	) {
		super(cluster, policy, nodePartitions.node, statement.namespace, tracker.socketTimeout, tracker.totalTimeout);
		this.statement = statement;
		this.recordSet = recordSet;
		this.tracker = tracker;
		this.nodePartitions = nodePartitions;
		deserializeKeys = true;
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
	protected final void writeBuffer() {
		setQuery(policy, statement, false, nodePartitions);
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
			throw new AerospikeException.QueryTerminated();
		}

		if (! recordSet.put(new KeyRecord(key, record))) {
			stop();
			throw new AerospikeException.QueryTerminated();
		}
	}
}

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
package com.aerospike.client.async;

import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.PartitionTracker;
import com.aerospike.client.query.PartitionTracker.NodePartitions;
import com.aerospike.client.query.Statement;
import com.aerospike.client.util.RandomShift;

public final class AsyncQueryPartitionExecutor extends AsyncMultiExecutor {
	private final QueryPolicy policy;
	private final RecordSequenceListener listener;
	private final Statement statement;
	private final PartitionTracker tracker;

	public AsyncQueryPartitionExecutor(
		EventLoop eventLoop,
		RecordSequenceListener listener,
		Cluster cluster,
		QueryPolicy policy,
		Statement statement,
		PartitionTracker tracker
	) {
		super(eventLoop, cluster);
		this.policy = policy;
		this.listener = listener;
		this.statement = statement;
		this.tracker = tracker;

		statement.setReturnData(true);
		tracker.setSleepBetweenRetries(0);
		queryPartitions();
	}

	private void queryPartitions() {
		statement.setTaskId(RandomShift.instance().nextLong());
		List<NodePartitions> nodePartitionsList = tracker.assignPartitionsToNodes(cluster, statement.getNamespace());

		AsyncQueryPartition[] tasks = new AsyncQueryPartition[nodePartitionsList.size()];
		int count = 0;

		for (NodePartitions nodePartitions : nodePartitionsList) {
			tasks[count++] = new AsyncQueryPartition(this, policy, listener, statement, tracker, nodePartitions);
		}
		execute(tasks, policy.maxConcurrentNodes);
	}

	protected void onSuccess() {
		try {
			if (tracker.isComplete(policy)) {
				listener.onSuccess();
				return;
			}

			// Prepare for retry.
			reset();
			queryPartitions();
		}
		catch (AerospikeException ae) {
			onFailure(ae);
		}
		catch (Exception e) {
			onFailure(new AerospikeException(e));
		}
	}

	protected void onFailure(AerospikeException ae) {
		ae.setIteration(tracker.iteration);
		listener.onFailure(ae);
	}
}

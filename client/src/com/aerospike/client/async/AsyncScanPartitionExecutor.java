/*
 * Copyright 2012-2024 Aerospike, Inc.
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
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.PartitionTracker;
import com.aerospike.client.query.PartitionTracker.NodePartitions;
import com.aerospike.client.util.RandomShift;

public final class AsyncScanPartitionExecutor extends AsyncMultiExecutor {
	private final ScanPolicy policy;
	private final RecordSequenceListener listener;
	private final String namespace;
	private final String setName;
	private final String[] binNames;
	private final PartitionTracker tracker;
	private final RandomShift random;

	public AsyncScanPartitionExecutor(
		EventLoop eventLoop,
		Cluster cluster,
		ScanPolicy policy,
		RecordSequenceListener listener,
		String namespace,
		String setName,
		String[] binNames,
		PartitionTracker tracker
	) throws AerospikeException {
		super(eventLoop, cluster, 0);
		this.policy = policy;
		this.listener = listener;
		this.namespace = namespace;
		this.setName = setName;
		this.binNames = binNames;
		this.tracker = tracker;
		this.random = new RandomShift();

		cluster.addCommandCount();
		tracker.setSleepBetweenRetries(0);
		scanPartitions();
	}

	private void scanPartitions() {
		long taskId = random.nextLong();
		List<NodePartitions> nodePartitionsList = tracker.assignPartitionsToNodes(cluster, namespace);

		AsyncScanPartition[] tasks = new AsyncScanPartition[nodePartitionsList.size()];
		int count = 0;

		for (NodePartitions nodePartitions : nodePartitionsList) {
			tasks[count++] = new AsyncScanPartition(this, policy, listener, namespace, setName, binNames, taskId, tracker, nodePartitions);
		}
		execute(tasks, policy.maxConcurrentNodes);
	}

	protected void onSuccess() {
		try {
			if (tracker.isComplete(cluster, policy)) {
				listener.onSuccess();
				return;
			}

			// Prepare for retry.
			if (policy.sleepBetweenRetries > 0) {
				// Schedule retry at a future time.
				eventLoop.schedule(new Runnable() {
					@Override
					public void run() {
						try {
							reset();
							scanPartitions();
						}
						catch (AerospikeException ae) {
							onFailure(ae);
						}
						catch (Throwable e) {
							onFailure(new AerospikeException(e));
						}
					}
				}, policy.sleepBetweenRetries, TimeUnit.MILLISECONDS);
			}
			else {
				reset();
				scanPartitions();
			}
		}
		catch (AerospikeException ae) {
			onFailure(ae);
		}
		catch (Throwable e) {
			onFailure(new AerospikeException(e));
		}
	}

	protected void onFailure(AerospikeException ae) {
		tracker.partitionError();
		ae.setIteration(tracker.iteration);
		listener.onFailure(ae);
	}
}

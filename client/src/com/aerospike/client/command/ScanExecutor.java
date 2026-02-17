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
package com.aerospike.client.command;

import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.PartitionTracker;
import com.aerospike.client.query.PartitionTracker.NodePartitions;
import com.aerospike.client.util.RandomShift;
import com.aerospike.client.util.Util;

public final class ScanExecutor {
	public static void scanPartitions(
		Cluster cluster,
		ScanPolicy policy,
		String namespace,
		String setName,
		String[] binNames,
		ScanCallback callback,
		PartitionTracker tracker
	) {
		cluster.addCommandCount();
		int retryInterval = policy.sleepBetweenRetries;
		double sleepMultiplier = policy.sleepMultiplier;

		RandomShift random = new RandomShift();

		while (true) {
			long taskId = random.nextLong();

			try {
				List<NodePartitions> list = tracker.assignPartitionsToNodes(cluster, namespace);

				if (policy.concurrentNodes && list.size() > 1) {
					Executor executor = new Executor(cluster, list.size());

					for (NodePartitions nodePartitions : list) {
						ScanPartitionCommand command = new ScanPartitionCommand(cluster, policy, namespace, setName, binNames, callback, taskId, tracker, nodePartitions);
						executor.addCommand(command);
					}

					executor.execute(policy.maxConcurrentNodes);
				}
				else {
					for (NodePartitions nodePartitions : list) {
						ScanPartitionCommand command = new ScanPartitionCommand(cluster, policy, namespace, setName, binNames, callback, taskId, tracker, nodePartitions);
						command.execute();
					}
				}
			}
			catch (AerospikeException ae) {
				tracker.partitionError();
				ae.setIteration(tracker.iteration);
				throw ae;
			}

			if (tracker.isComplete(cluster, policy)) {
				// Scan is complete.
				return;
			}

			if (retryInterval > 0) {
				// Sleep before trying again.
				Util.sleep(retryInterval);
				if (sleepMultiplier > 1) {
					retryInterval = (int) Math.round(retryInterval * sleepMultiplier);
				}
 			}
		}
	}
}

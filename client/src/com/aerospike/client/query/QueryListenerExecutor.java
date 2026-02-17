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
package com.aerospike.client.query;

import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.command.Executor;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.PartitionTracker.NodePartitions;
import com.aerospike.client.util.Util;

public final class QueryListenerExecutor {
	public static void execute(
		Cluster cluster,
		QueryPolicy policy,
		Statement statement,
		QueryListener listener,
		PartitionTracker tracker
	) {
		cluster.addCommandCount();

		TaskGen task = new TaskGen(statement);
		long taskId = task.getId();
		int retryInterval = policy.sleepBetweenRetries;
		double sleepMultiplier = policy.sleepMultiplier;

		while (true) {
			try {
				List<NodePartitions> list = tracker.assignPartitionsToNodes(cluster, statement.namespace);

				if (policy.maxConcurrentNodes > 0 && list.size() > 1) {
					Executor executor = new Executor(cluster, list.size());

					for (NodePartitions nodePartitions : list) {
						QueryListenerCommand command = new QueryListenerCommand(cluster, policy, statement, taskId, listener, tracker, nodePartitions);
						executor.addCommand(command);
					}

					executor.execute(policy.maxConcurrentNodes);
				}
				else {
					for (NodePartitions nodePartitions : list) {
						QueryListenerCommand command = new QueryListenerCommand(cluster, policy, statement, taskId, listener, tracker, nodePartitions);
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
				return;
			}

			if (retryInterval > 0) {
				// Sleep before trying again.
				Util.sleep(retryInterval);
				if (sleepMultiplier > 1) {
					retryInterval = (int) Math.round(retryInterval * sleepMultiplier);
				}
			}

			// taskId must be reset on next pass to avoid server duplicate query detection.
			taskId = task.nextId();
		}
	}
}

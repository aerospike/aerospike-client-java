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
package com.aerospike.client.task;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.Statement;

/**
 * Task used to poll for long running server execute job completion.
 */
public final class ExecuteTask extends Task {
	private final long taskId;
	private final boolean scan;

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public ExecuteTask(Cluster cluster, Policy policy, Statement statement) {
		super(cluster, policy);
		this.taskId = statement.getTaskId();
		this.scan = statement.isScan();
	}

	/**
	 * Query all nodes for task completion status.
	 */
	@Override
	public int queryStatus() throws AerospikeException {
		// All nodes must respond with complete to be considered done.
		Node[] nodes = cluster.validateNodes();

		String module = (scan) ? "scan" : "query";
		String command = "jobs:module=" + module + ";cmd=get-job;trid=" + taskId;

		for (Node node : nodes) {
			String response = Info.request(policy, node, command);

			if (response.startsWith("ERROR:2")) {
				return Task.NOT_FOUND;
			}

			if (response.startsWith("ERROR:")) {
				// Throw exception immediately.
				throw new AerospikeException(command + " failed: " + response);
			}

			String find = "status=";
			int index = response.indexOf(find);

			if (index < 0) {
				// Store exception and keep waiting.
				throw new AerospikeException(command + " failed: " + response);
			}

			int begin = index + find.length();
			int end = response.indexOf(':', begin);
			String status = response.substring(begin, end);

			// Newer servers use "done" while older servers use "DONE"
			if (! (status.startsWith("done") || status.startsWith("DONE"))) {
				return Task.IN_PROGRESS;
			}

			// Newer servers use "active(ok)" while older servers use "IN_PROGRESS"
			//if (status.startsWith("active") || status.startsWith("IN_PROGRESS")) {
			//	return false;
			//}
		}
		return Task.COMPLETE;
	}
}

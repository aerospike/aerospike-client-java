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
	public ExecuteTask(Cluster cluster, Policy policy, Statement statement, long taskId) {
		super(cluster, policy);
		this.taskId = taskId;
		this.scan = statement.isScan();
	}

	/**
	 * Return task id.
	 */
	public long getTaskId() {
		return taskId;
	}

	/**
	 * Query all nodes for task completion status.
	 */
	@Override
	public int queryStatus() throws AerospikeException {
		// All nodes must respond with complete to be considered done.
		Node[] nodes = cluster.validateNodes();

		for (Node node : nodes) {
			String command = buildCommandString(node);

			String response = Info.request(policy, node, command);

			if (response.startsWith("ERROR:2")) {
				// Query not found.
				if (node.hasPartitionQuery()) {
					// Server >= 6.0: Query has completed.
					// Continue checking other nodes.
					continue;
				}

				// Server < 6.0: Query could be complete or has not started yet.
				// Return NOT_FOUND and let the calling methods handle it.
				return Task.NOT_FOUND;
			}

			if (response.startsWith("ERROR:")) {
				throw new AerospikeException(command + " failed: " + response);
			}

			String status = extractStatusFromResponse(response, command);
			// Newer servers use "done" while older servers use "DONE"
			if (!(status.startsWith("done") || status.startsWith("DONE"))) {
				return Task.IN_PROGRESS;
			}
		}

		return Task.COMPLETE;
	}

	private String buildCommandString(Node node) {
		String taskID = Long.toUnsignedString(taskId);
		String module = (scan) ? "scan" : "query";
		String queryShowCommand = module + "-show:trid=" + taskID;
		String jobsCommand = "jobs:module=" + module + ";cmd=get-job;trid=" + taskID;

		if (node.hasPartitionQuery()) {
			return "query-show:trid=" + taskID;
		}

		return node.hasQueryShow() ? queryShowCommand : jobsCommand;
	}

	private String extractStatusFromResponse(String response, String command) throws AerospikeException {
		String find = "status=";
		int index = response.indexOf(find);

		if (index < 0) {
			throw new AerospikeException(command + " failed: " + response);
		}

		int begin = index + find.length();
		int end = response.indexOf(':', begin);
		return response.substring(begin, end);
	}

}

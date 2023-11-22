/*
 * Copyright 2012-2023 Aerospike, Inc.
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
 * Task used to poll for long-running server execute job completion.
 */
public class ExecuteTask extends Task {
	private final long taskId;
	private final boolean scan;

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public ExecuteTask(Cluster cluster, Policy policy, Statement statement, long taskId) {
		this(cluster, policy, taskId, statement.isScan());
	}

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public ExecuteTask(Cluster cluster, Policy policy, long taskId, boolean isScan) {
		super(cluster, policy);
		this.taskId = taskId;
		this.scan = isScan;
	}

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	protected ExecuteTask(long taskId, boolean isScan) {
		super(null, new Policy());
		this.taskId = taskId;
		this.scan = isScan;
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

		String tid = Long.toUnsignedString(taskId);
		String module = (scan) ? "scan" : "query";
		String cmd1 = "query-show:trid=" + tid;
		String cmd2 = module + "-show:trid=" + tid;
		String cmd3 = "jobs:module=" + module + ";cmd=get-job;trid=" + tid;

		for (Node node : nodes) {
			String command;

			if (node.hasPartitionQuery()) {
				// query-show works for both scan and query.
				command = cmd1;
			}
			else if (node.hasQueryShow()) {
				// scan-show and query-show are separate.
				command = cmd2;
			}
			else {
				// old job monitor syntax.
				command = cmd3;
			}

			String response = Info.request(policy, node, command);

			if (response.startsWith("ERROR:2")) {
				// Query not found.
				if (node.hasPartitionQuery()) {
					// Server >= 6.0:  Query has completed.
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

			String find = "status=";
			int index = response.indexOf(find);

			if (index < 0) {
				throw new AerospikeException(command + " failed: " + response);
			}

			int begin = index + find.length();
			int end = response.indexOf(':', begin);
			String status = response.substring(begin, end);

			// Newer servers use "done" while older servers use "DONE"
			if (! (status.startsWith("done") || status.startsWith("DONE"))) {
				return Task.IN_PROGRESS;
			}
		}
		return Task.COMPLETE;
	}
}

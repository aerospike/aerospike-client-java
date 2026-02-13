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
import com.aerospike.client.util.Version;

/**
 * Task that polls for completion of a background query or scan execute job started by
 * {@link com.aerospike.client.AerospikeClient#execute} (UDF or Operation overload).
 *
 * <p>Use {@link #waitTillComplete()} to block until the job finishes on all nodes, or
 * {@link #queryStatus()} / {@link com.aerospike.client.task.Task#isDone()} to poll.
 *
 * <p><b>Example (background execute with Operations, wait for completion):</b>
 * <pre>{@code
 * Statement stmt = new Statement();
 * stmt.setNamespace("test");
 * stmt.setSetName("users");
 * stmt.setFilter(Filter.equal("status", "pending"));
 * ExecuteTask task = client.execute(writePolicy, stmt, Operation.put(new Bin("processed", true)));
 * task.waitTillComplete();  // blocks until job finishes on all nodes
 * }</pre>
 *
 * <p><b>Example (background execute with UDF, poll status):</b>
 * <pre>{@code
 * Statement stmt = new Statement();
 * stmt.setNamespace("test");
 * stmt.setSetName("events");
 * ExecuteTask task = client.execute(writePolicy, stmt, "myudfs", "processRecord", Value.get("arg"));
 * while (!task.isDone()) {
 *     int status = task.queryStatus();  // NOT_FOUND, IN_PROGRESS, or COMPLETE
 *     Thread.sleep(500);
 * }
 * }</pre>
 *
 * @see com.aerospike.client.task.Task
 * @see com.aerospike.client.AerospikeClient#execute
 */
public class ExecuteTask extends Task {
	private final long taskId;
	private final boolean scan;

	/**
	 * Constructs an execute task for the given statement and task ID (from the execute call).
	 *
	 * @param cluster the cluster; must not be {@code null}
	 * @param policy policy for polling (timeout, etc.); must not be {@code null}
	 * @param statement the statement that was executed
	 * @param taskId the task ID returned by the execute call
	 */
	public ExecuteTask(Cluster cluster, Policy policy, Statement statement, long taskId) {
		this(cluster, policy, taskId, statement.isScan());
	}

	/**
	 * Constructs an execute task with the given task ID and scan flag.
	 *
	 * @param cluster the cluster; must not be {@code null}
	 * @param policy policy for polling; must not be {@code null}
	 * @param taskId the task ID from the execute call
	 * @param isScan {@code true} for scan execute, {@code false} for query execute
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
	 * Returns the task ID for this execute job.
	 *
	 * @return the task ID
	 */
	public long getTaskId() {
		return taskId;
	}

	/**
	 * Queries the cluster for this execute task's completion status.
	 *
	 * @return {@link Task#NOT_FOUND}, {@link Task#IN_PROGRESS}, or {@link Task#COMPLETE}
	 * @throws AerospikeException	when a node request fails (e.g. timeout or connection error).
	 */
	@Override
	public int queryStatus() throws AerospikeException {
		// All nodes must respond with complete to be considered done.
		Node[] nodes = cluster.validateNodes();
		String tid = Long.toUnsignedString(taskId);
		String module = (scan) ? "scan" : "query";

		for (Node node : nodes) {
			Version serverVersion = node.getServerVersion();
			String cmd1 = serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? "query-show:id=" + tid : "query-show:trid=" + tid;
			String cmd2 = serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? module + "-show:id=" + tid : module + "-show:trid=" + tid;
			String cmd3 = serverVersion.isGreaterOrEqual(Version.SERVER_VERSION_8_1) ? "jobs:module=" + module + ";cmd=get-job;id=" + tid : "jobs:module=" + module + ";cmd=get-job;trid=" + tid;

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

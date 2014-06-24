/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
import com.aerospike.client.query.Statement;

/**
 * Task used to poll for long running server execute job completion.
 */
public final class ExecuteTask extends Task {
	private final int taskId;
	private final boolean scan;

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public ExecuteTask(Cluster cluster, Statement statement) {
		super(cluster, false);
		this.taskId = statement.getTaskId();
		this.scan = statement.isScan();
	}

	/**
	 * Query all nodes for task completion status.
	 */
	@Override
	public boolean isDone() throws AerospikeException {
		String command = (scan) ? "scan-list" : "query-list";
		Node[] nodes = cluster.getNodes();
		boolean done = false;

		for (Node node : nodes) {
			String response = Info.request(node, command);
			String find = "job_id=" + taskId + ':';
			int index = response.indexOf(find);

			if (index < 0) {
				done = true;
				continue;
			}

			int begin = index + find.length();
			find = "job_status=";
			index = response.indexOf(find, begin);

			if (index < 0) {
				continue;
			}

			begin = index + find.length();
			int end = response.indexOf(':', begin);
			String status = response.substring(begin, end);
			
			if (status.equals("ABORTED")) {
				throw new AerospikeException.QueryTerminated();
			}
			else if (status.equals("IN PROGRESS")) {
				return false;
			}
			else if (status.equals("DONE")) {
				done = true;
			}
		}
		return done;
	}
}

/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
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

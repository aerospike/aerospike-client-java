/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
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
	private int taskId;
	private boolean scan;

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

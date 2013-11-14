/*
 * Aerospike Client - Java Library
 *
 * Copyright 2013 by Aerospike, Inc. All rights reserved.
 *
 * Availability of this source code to partners and customers includes
 * redistribution rights covered by individual contract. Please check your
 * contract for exact rights and responsibilities.
 */
package com.aerospike.client.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.util.Util;

/**
 * Task used to poll for long running execute job completion.
 */
public final class ExecuteTask {
	private Cluster cluster;
	private int taskId;
	private boolean scan;

	/**
	 * Initialize task with fields needed to query server nodes for query status.
	 */
	public ExecuteTask(Cluster cluster, Statement statement) {
		this.cluster = cluster;
		this.taskId = statement.taskId;
		this.scan = statement.filters == null;
	}

	/**
	 * Wait for asynchronous task to complete using default sleep interval.
	 */
	public void waitTillComplete() throws AerospikeException {
		waitTillComplete(1000);
	}

	/**
	 * Wait for asynchronous task to complete using given sleep interval.
	 */
	public void waitTillComplete(int sleepInterval) throws AerospikeException {
		boolean done = false;
		
		while (! done) {
			Util.sleep(sleepInterval);
			done = requestStatus();
		}
	}

	/**
	 * Query all nodes for task status.
	 */
	private boolean requestStatus() throws AerospikeException {
		String command = (scan) ? "scan-list" : "query-list";
		Node[] nodes = cluster.getNodes();
		boolean done = false;

		for (Node node : nodes) {
			Connection conn = node.getConnection(1000);
			String response = Info.request(conn, command);
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

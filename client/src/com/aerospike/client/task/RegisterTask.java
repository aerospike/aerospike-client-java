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

/**
 * Task used to poll for UDF registration completion.
 */
public final class RegisterTask extends Task {
	private String packageName;

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public RegisterTask(Cluster cluster, String packageName) {
		super(cluster, false);
		this.packageName = packageName;
	}

	/**
	 * Query all nodes for task completion status.
	 */
	public boolean isDone() throws AerospikeException {
		String command = "udf-list";
		Node[] nodes = cluster.getNodes();
		boolean done = false;

		for (Node node : nodes) {
			String response = Info.request(node, command);
			String find = "filename=" + packageName;
			int index = response.indexOf(find);

			if (index < 0) {
				return false;
			}
			done = true;
		}
		return done;
	}
}

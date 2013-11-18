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
 * Task used to poll for long running create index completion.
 */
public final class IndexTask extends Task {
	private String namespace;
	private String indexName;

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public IndexTask(Cluster cluster, String namespace, String indexName) {
		super(cluster, false);
		this.namespace = namespace;
		this.indexName = indexName;
	}

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public IndexTask() {
		super(null, true);
	}

	/**
	 * Query all nodes for task completion status.
	 */
	@Override
	public boolean isDone() throws AerospikeException {
		String command = "sindex/" + namespace + '/' + indexName;
		Node[] nodes = cluster.getNodes();
		boolean complete = false;

		for (Node node : nodes) {
			try {
				String response = Info.request(node, command);
				String find = "load_pct=";
				int index = response.indexOf(find);
	
				if (index < 0) {
					complete = true;
					continue;
				}
	
				int begin = index + find.length();
				int end = response.indexOf(';', begin);
				String str = response.substring(begin, end);
				int pct = Integer.parseInt(str);
				
				if (pct >= 0 && pct < 100) {
					return false;
				}
				complete = true;
			}
			catch (Exception e) {
				complete = true;
			}
		}
		return complete;
	}
}

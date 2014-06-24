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

/**
 * Task used to poll for long running create index completion.
 */
public final class IndexTask extends Task {
	private final String namespace;
	private final String indexName;

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
		namespace = null;
		indexName = null;
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

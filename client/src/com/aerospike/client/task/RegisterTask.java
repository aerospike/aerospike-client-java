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
 * Task used to poll for UDF registration completion.
 */
public final class RegisterTask extends Task {
	private final String packageName;

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

/*
 * Copyright 2012-2021 Aerospike, Inc.
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

/**
 * Task that polls for completion of UDF module registration started by
 * {@link com.aerospike.client.AerospikeClient#register} or
 * {@link com.aerospike.client.AerospikeClient#registerUdfString}.
 *
 * <p>Use {@link #waitTillComplete()} to block until the package is registered on all nodes, or
 * {@link #queryStatus()} / {@link com.aerospike.client.task.Task#isDone()} to poll.
 *
 * <p><b>Example (register UDF from file, wait for completion):</b>
 * <pre>{@code
 * RegisterTask task = client.register(null, "udf/myudfs.lua", "myudfs.lua", Language.LUA);
 * task.waitTillComplete();  // blocks until package is registered on all nodes
 * }</pre>
 *
 * <p><b>Example (register from classpath resource, poll status):</b>
 * <pre>{@code
 * RegisterTask task = client.register(null, MyClass.class.getClassLoader(), "udf/myagg.lua", "myagg.lua", Language.LUA);
 * while (!task.isDone()) {
 *     int status = task.queryStatus();  // IN_PROGRESS or COMPLETE
 *     Thread.sleep(500);
 * }
 * }</pre>
 *
 * @see com.aerospike.client.task.Task
 * @see com.aerospike.client.AerospikeClient#register
 */
public final class RegisterTask extends Task {
	private final String packageName;

	/**
	 * Constructs a register task for the given UDF package name.
	 *
	 * @param cluster the cluster; must not be {@code null}
	 * @param policy policy for polling (timeout, etc.); must not be {@code null}
	 * @param packageName the UDF package name (e.g. "myudfs")
	 */
	public RegisterTask(Cluster cluster, Policy policy, String packageName) {
		super(cluster, policy);
		this.packageName = packageName;
	}

	/**
	 * Queries the cluster for this registration task's completion status.
	 *
	 * @return {@link Task#IN_PROGRESS} or {@link Task#COMPLETE}
	 * @throws AerospikeException	when a node request fails (e.g. timeout or connection error).
	 */
	public int queryStatus() throws AerospikeException {
		// All nodes must respond with package to be considered done.
		Node[] nodes = cluster.validateNodes();

		String command = "udf-list";

		for (Node node : nodes) {
			String response = Info.request(policy, node, command);
			String find = "filename=" + packageName;
			int index = response.indexOf(find);

			if (index < 0) {
				return Task.IN_PROGRESS;
			}
		}
		return Task.COMPLETE;
	}
}

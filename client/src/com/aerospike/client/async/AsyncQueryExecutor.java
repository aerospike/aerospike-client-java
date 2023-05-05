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
package com.aerospike.client.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;

public final class AsyncQueryExecutor extends AsyncMultiExecutor {
	private final RecordSequenceListener listener;

	public AsyncQueryExecutor(
		EventLoop eventLoop,
		RecordSequenceListener listener,
		Cluster cluster,
		QueryPolicy policy,
		Statement statement,
		Node[] nodes
	) {
		super(eventLoop, cluster, policy.infoTimeout);
		this.listener = listener;

		// Create commands.
		long taskId = statement.prepareTaskId();
		AsyncQuery[] tasks = new AsyncQuery[nodes.length];
		int count = 0;

		for (Node node : nodes) {
			tasks[count++] = new AsyncQuery(this, node, policy, listener, statement, taskId);
		}

		// Dispatch commands to nodes.
		if (policy.failOnClusterChange) {
			executeValidate(tasks, policy.maxConcurrentNodes, statement.getNamespace());
		}
		else {
			execute(tasks, policy.maxConcurrentNodes);
		}
	}

	protected void onSuccess() {
		listener.onSuccess();
	}

	protected void onFailure(AerospikeException ae) {
		listener.onFailure(ae);
	}
}

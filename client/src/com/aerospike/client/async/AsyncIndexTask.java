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
package com.aerospike.client.async;

import static com.aerospike.client.task.IndexTask.buildStatusCommand;
import static com.aerospike.client.task.IndexTask.parseStatusResponse;

import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.listener.TaskStatusListener;
import com.aerospike.client.policy.InfoPolicy;

/**
 * Async index task monitor.
 */
public class AsyncIndexTask {
	private final IAerospikeClient client;
	private final String namespace;
	private final String indexName;
	private final boolean isCreate;

	/**
	 * Initialize task with fields needed to query server nodes.
	 */
	public AsyncIndexTask(IAerospikeClient client, String namespace, String indexName, boolean isCreate) {
		this.client = client;
		this.namespace = namespace;
		this.indexName = indexName;
		this.isCreate = isCreate;
	}

	/**
	 * Asynchronously query node for task completion status.
	 * All nodes must respond with load_pct of 100 to be considered done.
	 */
	public void queryStatus(EventLoop eventLoop, InfoPolicy policy, Node node, TaskStatusListener listener) {
		if (client.getNodes().length == 0) {
			listener.onFailure(new AerospikeException("Cluster is empty"));
		}

		String command = buildStatusCommand(namespace, indexName, node.serverVersion);

		client.info(eventLoop, new InfoListener() {
			@Override
			public void onSuccess(Map<String, String> map) {
				try {
					int status = parseStatusResponse(command, map.values().iterator().next(), isCreate);
					listener.onSuccess(status);
				}
				catch (AerospikeException ae) {
					listener.onFailure(ae);
				}
			}

			@Override
			public void onFailure(AerospikeException ae) {
				listener.onFailure(ae);
			}
		}, policy, node, command);
	}
}

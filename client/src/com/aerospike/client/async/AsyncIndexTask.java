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
 * Asynchronous index task returned from {@link IAerospikeClient#createIndex} (and drop) to poll completion.
 * <p>
 * Use {@link #queryStatus(EventLoop, InfoPolicy, Node, TaskStatusListener)} to check a node; all nodes must report 100% load for the task to be done.
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 * InfoPolicy policy = new InfoPolicy();
 * Node node = client.getNodes()[0];
 *
 * client.createIndex(loop, policy, "ns", "set", "idx1", "bin1", IndexType.STRING, new IndexListener() {
 *   public void onSuccess(AsyncIndexTask task) {
 *     task.queryStatus(loop, policy, node, new TaskStatusListener() { ... });
 *   }
 *   public void onFailure(AerospikeException e) { }
 * });
 * }</pre>
 *
 * @see IAerospikeClient#createIndex
 * @see com.aerospike.client.listener.TaskStatusListener
 */
public class AsyncIndexTask {
	private final IAerospikeClient client;
	private final String namespace;
	private final String indexName;
	private final boolean isCreate;

	/**
	 * @param client client (must not be null)
	 * @param namespace namespace
	 * @param indexName index name
	 * @param isCreate true for create, false for drop
	 */
	public AsyncIndexTask(IAerospikeClient client, String namespace, String indexName, boolean isCreate) {
		this.client = client;
		this.namespace = namespace;
		this.indexName = indexName;
		this.isCreate = isCreate;
	}

	/**
	 * Asynchronously queries the given node for this task's completion status; all nodes must report load_pct 100 for the task to be done.
	 * @param eventLoop event loop to run the request on (must not be null)
	 * @param policy info policy (must not be null)
	 * @param node node to query (must not be null)
	 * @param listener callback for status or failure (must not be null)
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

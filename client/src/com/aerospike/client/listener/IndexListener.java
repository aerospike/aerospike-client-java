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
package com.aerospike.client.listener;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.async.AsyncIndexTask;

/**
 * Async callback for create/drop index; receives {@link com.aerospike.client.async.AsyncIndexTask} to poll status or exception.
 * <p>
 * Pass to {@link com.aerospike.client.IAerospikeClient#createIndex} and dropIndex overloads. Use the task in onSuccess to call {@link com.aerospike.client.async.AsyncIndexTask#queryStatus} for completion.
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 *
 * client.createIndex(loop, new InfoPolicy(), "ns", "set", "idx1", "bin1", IndexType.STRING, new IndexListener() {
 *   public void onSuccess(AsyncIndexTask task) {
 *     task.queryStatus(loop, policy, node, new TaskStatusListener() { ... });
 *   }
 *   public void onFailure(AerospikeException e) { }
 * });
 * }</pre>
 *
 * @see com.aerospike.client.async.AsyncIndexTask
 * @see TaskStatusListener
 * @see com.aerospike.client.IAerospikeClient#createIndex
 * @see com.aerospike.client.IAerospikeClient#dropIndex
 */
public interface IndexListener {
	/**
	 * Called when the create/drop index request is accepted; use the task to poll completion.
	 * @param indexTask task monitor for querying index status (must not be null)
	 */
	void onSuccess(AsyncIndexTask indexTask);

	/**
	 * Called when the create/drop index request fails.
	 * @param ae exception cause of failure wrapped into Aerospike exception
	 */
	public void onFailure(AerospikeException ae);
}

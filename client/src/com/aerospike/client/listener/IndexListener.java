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
 * Asynchronous result notifications for create/drop index commands.
 * <p>
 * No built-in implementations; implement this interface in application code and pass to
 * {@link com.aerospike.client.IAerospikeClient#createIndex(com.aerospike.client.async.EventLoop, IndexListener, com.aerospike.client.policy.InfoPolicy, java.lang.String, java.lang.String, java.lang.String, java.lang.String, com.aerospike.client.query.IndexType, com.aerospike.client.query.IndexCollectionType) createIndex(EventLoop, IndexListener, ...)}
 * or dropIndex async methods.
 * <p>Implement and pass to async createIndex/dropIndex to receive AsyncIndexTask on success or exception on failure.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * client.createIndex(eventLoop, new IndexListener() {
 *   public void onSuccess(com.aerospike.client.async.AsyncIndexTask indexTask) { // poll task
 *   }
 *   public void onFailure(AerospikeException ae) { ae.printStackTrace(); }
 * }, null, "ns", "set", "idxName", "bin", IndexType.STRING, IndexCollectionType.DEFAULT);
 * }</pre>
 */
public interface IndexListener {
	/**
	 * This method is called when an asynchronous command completes successfully.
	 *
	 * @param indexTask		task monitor that can be used to query for index command completion.
	 */
	void onSuccess(AsyncIndexTask indexTask);

	/**
	 * This method is called when an asynchronous command fails.
	 */
	public void onFailure(AerospikeException ae);
}

/*
 * Copyright 2012-2022 Aerospike, Inc.
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

import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;

/**
 * Asynchronous result notifications for batch get commands with variable bins per key.
 * The result is sent in a single list.
 * <p>
 * No built-in implementations; implement this interface in application code and pass to
 * {@link com.aerospike.client.IAerospikeClient#get(com.aerospike.client.async.EventLoop, BatchListListener, com.aerospike.client.policy.BatchPolicy, java.util.List) get(EventLoop, BatchListListener, ...)}.
 * <p>Implement and pass to async batch get (variable bins) to receive list of BatchRead on success or exception on failure.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * client.get(eventLoop, new BatchListListener() {
 *   public void onSuccess(java.util.List<BatchRead> records) { // use records
 *   }
 *   public void onFailure(AerospikeException ae) { ae.printStackTrace(); }
 * }, null, batchList);
 * }</pre>
 */
public interface BatchListListener {
	/**
	 * This method is called when the command completes successfully.
	 *
	 * @param records		record instances, {@link com.aerospike.client.BatchRecord#record}
	 *						will be null if the key is not found
	 */
	public void onSuccess(List<BatchRead> records);

	/**
	 * This method is called when the command fails.
	 */
	public void onFailure(AerospikeException ae);
}

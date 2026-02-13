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
import com.aerospike.client.BatchRead;

/**
 * Asynchronous result notifications for batch get commands with variable bins per key.
 * The results are sent one batch record at a time.
 * <p>
 * No built-in implementations; implement this interface in application code and pass to
 * {@link com.aerospike.client.IAerospikeClient#get(com.aerospike.client.async.EventLoop, BatchSequenceListener, com.aerospike.client.policy.BatchPolicy, java.util.List) get(EventLoop, BatchSequenceListener, ...)}.
 * <p>Implement and pass to async batch get (variable bins) to receive each BatchRead via onRecord, then onSuccess or onFailure.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * client.get(eventLoop, new BatchSequenceListener() {
 *   public void onRecord(BatchRead record) { // use record
 *   }
 *   public void onSuccess() { }
 *   public void onFailure(AerospikeException ae) { ae.printStackTrace(); }
 * }, null, batchList);
 * }</pre>
 */
public interface BatchSequenceListener {
	/**
	 * This method is called when an asynchronous batch record is received from the server.
	 * The receive sequence is not ordered.
	 *
	 * @param record	record instance, {@link com.aerospike.client.BatchRecord#record}
	 *					will be null if the key is not found
	 */
	public void onRecord(BatchRead record);

	/**
	 * This method is called when the command completes successfully.
	 */
	public void onSuccess();

	/**
	 * This method is called when the command fails.
	 */
	public void onFailure(AerospikeException ae);
}

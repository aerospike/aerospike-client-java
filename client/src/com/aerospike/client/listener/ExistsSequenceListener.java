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
import com.aerospike.client.Key;

/**
 * Asynchronous result notifications for batch exists commands.
 * The results are sent one record at a time.
 * <p>
 * No built-in implementations; implement this interface in application code and pass to
 * {@link com.aerospike.client.IAerospikeClient#exists(com.aerospike.client.async.EventLoop, ExistsSequenceListener, com.aerospike.client.policy.BatchPolicy, com.aerospike.client.Key[]) exists(EventLoop, ExistsSequenceListener, ...)}.
 * <p>Implement and pass to async batch exists to receive each key and exists flag via onExists, then onSuccess or onFailure.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * client.exists(eventLoop, new ExistsSequenceListener() {
 *   public void onExists(Key key, boolean exists) { // use per key
 *   }
 *   public void onSuccess() { }
 *   public void onFailure(AerospikeException ae) { ae.printStackTrace(); }
 * }, null, keys);
 * }</pre>
 */
public interface ExistsSequenceListener {
	/**
	 * This method is called when an asynchronous batch exists result is received from the server.
	 * The receive sequence is not ordered.
	 *
	 * @param key		unique record identifier
	 * @param exists	whether key exists on server
	 */
	public void onExists(Key key, boolean exists);

	/**
	 * This method is called when the asynchronous batch exists command completes.
	 */
	public void onSuccess();

	/**
	 * This method is called when an asynchronous batch exists command fails.
	 */
	public void onFailure(AerospikeException ae);
}

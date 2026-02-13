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
import com.aerospike.client.Record;

/**
 * Asynchronous result notifications for get or operate commands.
 * <p>
 * No built-in implementations; implement this interface in application code and pass to
 * {@link com.aerospike.client.IAerospikeClient#get(com.aerospike.client.async.EventLoop, RecordListener, com.aerospike.client.policy.Policy, com.aerospike.client.Key) get(EventLoop, RecordListener, ...)}
 * or operate async methods.
 * <p>Implement and pass to async get to receive key and record on success or exception on failure.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * client.get(eventLoop, new RecordListener() {
 *   public void onSuccess(Key key, Record record) { // use record
 *   }
 *   public void onFailure(AerospikeException ae) { ae.printStackTrace(); }
 * }, null, key);
 * }</pre>
 */
public interface RecordListener {
	/**
	 * This method is called when an asynchronous get or operate command completes successfully.
	 *
	 * @param key			unique record identifier
	 * @param record		record instance if found, otherwise null
	 */
	public void onSuccess(Key key, Record record);

	/**
	 * This method is called when an asynchronous get or operate command fails.
	 */
	public void onFailure(AerospikeException ae);
}

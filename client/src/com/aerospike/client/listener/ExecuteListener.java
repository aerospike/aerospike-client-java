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
 * Asynchronous result notifications for execute commands.
 * <p>
 * No built-in implementations; implement this interface in application code and pass to
 * {@link com.aerospike.client.IAerospikeClient#execute(com.aerospike.client.async.EventLoop, ExecuteListener, com.aerospike.client.policy.WritePolicy, com.aerospike.client.Key, java.lang.String, java.lang.String, com.aerospike.client.Value[]) execute(EventLoop, ExecuteListener, ...)}.
 * <p>Implement and pass to async execute to receive key and result object on success or exception on failure.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * client.execute(eventLoop, new ExecuteListener() {
 *   public void onSuccess(Key key, Object obj) { // use obj
 *   }
 *   public void onFailure(AerospikeException ae) { ae.printStackTrace(); }
 * }, null, key, "module", "function", args);
 * }</pre>
 */
public interface ExecuteListener {
	/**
	 * This method is called when an asynchronous execute command completes successfully.
	 *
	 * @param key	unique record identifier
	 * @param obj	returned object
	 */
	public void onSuccess(Key key, Object obj);

	/**
	 * This method is called when an asynchronous execute command fails.
	 */
	public void onFailure(AerospikeException ae);
}

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
 * Async callback for execute (UDF); receives key, return value, or exception.
 * <p>
 * Pass to {@link com.aerospike.client.IAerospikeClient#execute}
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 *
 * client.execute(loop, new ExecuteListener() {
 *   public void onSuccess(Key key, Object obj) { }
 *   public void onFailure(AerospikeException e) { }
 * }, new Policy(), key, "module", "function", null);
 * }</pre>
 *
 * @see com.aerospike.client.IAerospikeClient#execute
 */
public interface ExecuteListener {
	/**
	 * Called when the execute completes successfully.
	 * @param key record key (must not be null)
	 * @param obj value returned by the UDF, or null
	 */
	public void onSuccess(Key key, Object obj);

	/**
	 * Called when the execute fails.
	 * @param ae exception cause of failure wrapped into Aerospike exception
	 */
	public void onFailure(AerospikeException ae);
}

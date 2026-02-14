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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;

/**
 * Async callback for batch exists; receives keys and existence flags in one array (same order as input keys).
 * <p>
 * Pass to {@link com.aerospike.client.IAerospikeClient#exists(com.aerospike.client.async.EventLoop, ExistsArrayListener, com.aerospike.client.policy.BatchPolicy, com.aerospike.client.Key[])}.
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 * Key[] keys = new Key[] { new Key("ns", "set", "k1") };
 *
 * client.exists(loop, new ExistsArrayListener() {
 *   public void onSuccess(Key[] keys, boolean[] exists) { }
 *   public void onFailure(AerospikeException e) { }
 * }, new BatchPolicy(), keys);
 * }</pre>
 *
 * @see com.aerospike.client.IAerospikeClient#exists(com.aerospike.client.async.EventLoop, ExistsArrayListener, com.aerospike.client.policy.BatchPolicy, com.aerospike.client.Key[])
 * @see com.aerospike.client.AerospikeException.BatchExists
 */
public interface ExistsArrayListener {
	/**
	 * Called when the batch exists completes; arrays are in same order as the key array.
	 * @param keys original keys (must not be null)
	 * @param exists existence per key (must not be null)
	 */
	public void onSuccess(Key[] keys, boolean[] exists);

	/**
	 * Called when the batch fails; may be {@link com.aerospike.client.AerospikeException.BatchExists} with partial results.
	 * @param ae exception cause of failure wrapped into Aerospike exception
	 */
	public void onFailure(AerospikeException ae);
}

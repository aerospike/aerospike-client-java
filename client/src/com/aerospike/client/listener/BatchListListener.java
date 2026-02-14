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
 * Async callback for batch get with variable bins; receives a single list of {@link com.aerospike.client.BatchRead} results.
 * <p>
 * Pass to {@link com.aerospike.client.IAerospikeClient#get} is null when the key is not found.
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 * BatchRead[] batchReads = new BatchRead[] { new BatchRead(new Key("ns", "set", "k1"), new String[] { "bin1" }) };
 *
 * client.get(loop, new BatchListListener() {
 *   public void onSuccess(List records) { }
 *   public void onFailure(AerospikeException e) { }
 * }, new BatchPolicy(), batchReads);
 * }</pre>
 *
 * @see com.aerospike.client.IAerospikeClient#get
 */
public interface BatchListListener {
	/**
	 * Called when the batch get completes; list order matches input BatchRead array.
	 * @param records list of BatchRead; record is null when key not found
	 */
	public void onSuccess(List<BatchRead> records);

	/**
	 * Called when the batch get fails.
	 * @param ae exception cause of failure wrapped into Aerospike exception
	 */
	public void onFailure(AerospikeException ae);
}

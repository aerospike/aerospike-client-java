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
import com.aerospike.client.Record;

/**
 * Async callback for batch get; receives keys and records in one array (same order as input keys).
 * <p>
 * Pass to {@link com.aerospike.client.IAerospikeClient#get}. Records align by index with keys; null record means key not found.
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 * Key[] keys = new Key[] { new Key("ns", "set", "k1") };
 *
 * client.get(loop, new RecordArrayListener() {
 *   public void onSuccess(Key[] keys, Record[] records) { }
 *   public void onFailure(AerospikeException e) { }
 * }, new BatchPolicy(), keys);
 * }</pre>
 *
 * @see com.aerospike.client.IAerospikeClient#get
 * @see com.aerospike.client.AerospikeException.BatchRecords
 */
public interface RecordArrayListener {
	/**
	 * Called when the batch get completes; arrays are in same order as the key array.
	 * @param keys original keys (must not be null)
	 * @param records records; null at index i means key i was not found
	 */
	public void onSuccess(Key[] keys, Record[] records);

	/**
	 * Called when the batch fails; may be {@link com.aerospike.client.AerospikeException.BatchRecords} with partial results.
	 * @param ae exception cause of failure wrapped into Aerospike exception
	 */
	public void onFailure(AerospikeException ae);
}

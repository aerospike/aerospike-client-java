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
 * Async callback for single-key get and operate; receives one record or failure.
 * <p>
 * Pass to {@link com.aerospike.client.IAerospikeClient#get} or operate overloads. Called once per key with the record (or null if not found) or with an exception.
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 *
 * client.get(loop, new Policy(), new RecordListener() {
 *   public void onSuccess(Key key, Record record) {
 *     if (record != null) { Object v = record.getValue("bin"); }
 *   }
 *   public void onFailure(AerospikeException e) { }
 * }, key);
 * }</pre>
 *
 * @see com.aerospike.client.IAerospikeClient#get
 * @see com.aerospike.client.IAerospikeClient#operate
 */
public interface RecordListener {
	/**
	 * Called when the get or operate completes successfully.
	 * @param key record key (must not be null)
	 * @param record record if found, otherwise null
	 */
	public void onSuccess(Key key, Record record);

	/**
	 * Called when the get or operate fails.
	 * @param ae exception cause of failure wrapped into Aerospike exception
	 */
	public void onFailure(AerospikeException ae);
}

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
import com.aerospike.client.BatchRecord;

/**
 * Async callback for batch operate; receives full array of {@link com.aerospike.client.BatchRecord} and overall status.
 * <p>
 * Pass to {@link com.aerospike.client.IAerospikeClient#operate(com.aerospike.client.async.EventLoop, BatchRecordArrayListener, com.aerospike.client.policy.BatchPolicy, com.aerospike.client.BatchRecord[])}. Records are in same order as input; check each {@link com.aerospike.client.BatchRecord#resultCode}.
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 * BatchRecord[] records = new BatchRecord[] { new BatchWrite(...) };
 *
 * client.operate(loop, new BatchRecordArrayListener() {
 *   public void onSuccess(BatchRecord[] records, boolean status) { }
 *   public void onFailure(BatchRecord[] records, AerospikeException e) { }
 * }, new BatchPolicy(), records);
 * }</pre>
 *
 * @see com.aerospike.client.BatchRecord
 * @see com.aerospike.client.IAerospikeClient#operate(com.aerospike.client.async.EventLoop, BatchRecordArrayListener, com.aerospike.client.policy.BatchPolicy, com.aerospike.client.BatchRecord[])
 */
public interface BatchRecordArrayListener {
	/**
	 * Called when the batch operate completes; one entry per input record, in order.
	 * @param records batch records, always populated (must not be null)
	 * @param status true if all records succeeded
	 */
	public void onSuccess(BatchRecord[] records, boolean status);

	/**
	 * Called when the batch fails; records may still be populated with per-key results.
	 * @param records batch records (must not be null); check {@link com.aerospike.client.BatchRecord#resultCode}
	 * @param ae exception cause of failure wrapped into Aerospike exception
	 */
	public void onFailure(BatchRecord[] records, AerospikeException ae);
}

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
import com.aerospike.client.BatchRecord;

/**
 * Async callback for batch operate with variable operations per key; receives a list of {@link com.aerospike.client.BatchRecord} and overall status.
 * <p>
 * Pass to {@link com.aerospike.client.IAerospikeClient#operate(com.aerospike.client.async.EventLoop, BatchOperateListListener, com.aerospike.client.policy.BatchPolicy, java.util.List)}. Each record's result is in {@link com.aerospike.client.BatchRecord#record} (null on error).
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 *
 * client.operate(loop, new BatchOperateListListener() {
 *   public void onSuccess(List records, boolean status) { }
 *   public void onFailure(AerospikeException e) { }
 * }, new BatchPolicy(), batchRecordList);
 * }</pre>
 *
 * @see com.aerospike.client.IAerospikeClient#operate(com.aerospike.client.async.EventLoop, BatchOperateListListener, com.aerospike.client.policy.BatchPolicy, java.util.List)
 */
public interface BatchOperateListListener {
	/**
	 * Called when the batch operate completes; list order matches input.
	 * @param records list of BatchRecord; record is null when that key had an error
	 * @param status true if all records succeeded
	 */
	public void onSuccess(List<BatchRecord> records, boolean status);

	/**
	 * Called when the batch operate fails.
	 * @param ae exception cause of failure wrapped into Aerospike exception
	 */
	public void onFailure(AerospikeException ae);
}

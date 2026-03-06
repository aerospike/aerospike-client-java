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
import com.aerospike.client.BatchRecord;

/**
 * Async callback for batch operate; results delivered one {@link com.aerospike.client.BatchRecord} at a time via {@link #onRecord}, then {@link #onSuccess} or {@link #onFailure}.
 * <p>
 * Pass to {@link com.aerospike.client.IAerospikeClient#operate}. Order of onRecord is not guaranteed.
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 * BatchRecord[] records = new BatchRecord[] { new BatchWrite(...) };
 *
 * client.operate(loop, new BatchRecordSequenceListener() {
 *   public void onRecord(BatchRecord record, int index) { }
 *   public void onSuccess() { }
 *   public void onFailure(AerospikeException e) { }
 * }, new BatchPolicy(), records);
 * }</pre>
 *
 * @see com.aerospike.client.IAerospikeClient#operate
 */
public interface BatchRecordSequenceListener {
	/**
	 * Called for each batch record when received; order is not guaranteed.
	 * @param record batch record (must not be null)
	 * @param index index into the original BatchRecord array
	 */
	public void onRecord(BatchRecord record, int index);

	/** Called when the batch operate completes successfully. */
	public void onSuccess();

	/**
	 * Called when the batch operate fails.
	 * @param ae exception cause of failure wrapped into Aerospike exception
	 */
	public void onFailure(AerospikeException ae);
}

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
 * Asynchronous callback for batch get and scan/query; results are delivered one record at a time via {@link #onRecord}, then {@link #onSuccess} or {@link #onFailure}.
 * <p>
 * Pass to {@link com.aerospike.client.AerospikeClient#query} or {@link com.aerospike.client.AerospikeClient#get} async overloads. For scan/query, throw {@link com.aerospike.client.AerospikeException.QueryTerminated} or {@link com.aerospike.client.AerospikeException.ScanTerminated} to abort; the exception is reported in {@link #onFailure}.
 *
 * <pre>{@code
 * EventLoops eventLoops = new NioEventLoops(4);
 * ClientPolicy clientPolicy = new ClientPolicy();
 * clientPolicy.eventLoops = eventLoops;
 * IAerospikeClient client = new AerospikeClient(clientPolicy, "localhost", 3000);
 * EventLoop loop = eventLoops.next();
 * Statement stmt = new Statement();
 * stmt.setNamespace("test");
 * stmt.setSetName("set1");
 * client.query(loop, new RecordSequenceListener() {
 *   public void onRecord(Key key, Record record) {
 *     if (record != null) { Object v = record.getValue("mybin"); }
 *   }
 *   public void onSuccess() { }
 *   public void onFailure(AerospikeException e) { }
 * }, null, stmt);
 * }</pre>
 *
 * @see #onSuccess
 * @see #onFailure(AerospikeException)
 * @see com.aerospike.client.AerospikeClient#query
 * @see com.aerospike.client.AerospikeClient#get
 */
public interface RecordSequenceListener {
	/**
	 * Called when an asynchronous record is received; order is not guaranteed. For scan/query, throw QueryTerminated or ScanTerminated to abort (reported in onFailure). For batch, a thrown exception stops only the current node.
	 *
	 * @param key    unique record identifier; never null
	 * @param record record instance; null if the key is not found
	 * @throws AerospikeException when an error occurs or the scan/query should be terminated (e.g. throw ScanTerminated or QueryTerminated to abort).
	 */
	public void onRecord(Key key, Record record) throws AerospikeException;

	/** Called when the asynchronous batch get or scan/query completes successfully. */
	public void onSuccess();

	/** Called when the asynchronous batch get or scan/query fails; receives the exception. */
	public void onFailure(AerospikeException ae);
}

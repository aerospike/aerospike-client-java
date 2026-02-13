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
 * Asynchronous result notifications for batch get and scan/query commands.
 * The results are sent one record at a time.
 */
public interface RecordSequenceListener {
	/**
	 * This method is called when an asynchronous record is received from the server.
	 * The receive sequence is not ordered.
	 * <p>
	 * If this listener is used in a scan/query command, the user may throw
	 * {@link AerospikeException.QueryTerminated} or {@link AerospikeException.ScanTerminated}
	 * to abort. If any exception is thrown, parallel command threads to other nodes will also
	 * be terminated and the exception will be propagated back through {@link #onFailure(AerospikeException)}.
	 * <p>
 * If this listener is used in a batch command, a user-thrown exception will terminate the batch
 * to the current node, but parallel batch command threads to other nodes will continue to run.
 * <p>
 * No built-in implementations; implement this interface in application code and pass to
 * {@link com.aerospike.client.IAerospikeClient#query(com.aerospike.client.async.EventLoop, RecordSequenceListener, com.aerospike.client.policy.QueryPolicy, com.aerospike.client.query.Statement) query(EventLoop, RecordSequenceListener, ...)}
 * or batch get / scan async methods.
 * <p>Implement and pass to async query to receive each record via onRecord, then onSuccess or onFailure.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * client.query(eventLoop, new RecordSequenceListener() {
 *   public void onRecord(Key key, Record record) throws AerospikeException {
 *     if (record != null) {
 *       System.out.println(key + " " + record.getString("name"));
 *     }
 *   }
 *   public void onSuccess() { }
 *   public void onFailure(AerospikeException e) { e.printStackTrace(); }
 * }, null, stmt);
 * }</pre>
 *
	 * @param key					unique record identifier
	 * @param record				record instance; null if the key is not found
	 * @throws AerospikeException	when an error occurs or the scan/query should be terminated (e.g. throw {@link AerospikeException.ScanTerminated} to abort).
	 * @see #onSuccess()
	 * @see #onFailure(AerospikeException)
	 * @see com.aerospike.client.AerospikeClient#query(com.aerospike.client.async.EventLoop, RecordSequenceListener, com.aerospike.client.policy.QueryPolicy, com.aerospike.client.query.Statement)
	 * @see com.aerospike.client.AerospikeClient#get(com.aerospike.client.async.EventLoop, RecordSequenceListener, com.aerospike.client.policy.BatchPolicy, com.aerospike.client.Key[])
	 */
	public void onRecord(Key key, Record record) throws AerospikeException;

	/**
	 * This method is called when the asynchronous batch get or scan command completes.
	 */
	public void onSuccess();

	/**
	 * This method is called when an asynchronous batch get or scan command fails.
	 */
	public void onFailure(AerospikeException ae);
}

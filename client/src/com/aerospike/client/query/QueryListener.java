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
package com.aerospike.client.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

/**
 * Callback invoked for each record returned by a synchronous secondary-index query.
 * Use this listener when you want to process records one at a time instead of buffering
 * a full {@link RecordSet}; results are streamed and the receive order is not guaranteed.
 * <p>
 * No built-in implementations; implement this interface in application code and pass to
 * {@link com.aerospike.client.IAerospikeClient#query(com.aerospike.client.policy.QueryPolicy, Statement, QueryListener)}
 * or the overload with {@link PartitionFilter}. To abort from inside the callback, throw
 * {@link AerospikeException.QueryTerminated}.
 * <p>Implement onRecord and pass to query to process each record as it arrives; close resources in caller when done.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * Statement stmt = new Statement();
 * stmt.setNamespace("test");
 * stmt.setSetName("users");
 * client.query(queryPolicy, stmt, new QueryListener() {
 *   public void onRecord(Key key, Record record) {
 *     // process key, record
 *   }
 * });
 * }</pre>
 *
 * @see com.aerospike.client.AerospikeClient#query(com.aerospike.client.policy.QueryPolicy, Statement, QueryListener)
 * @see com.aerospike.client.AerospikeClient#query(com.aerospike.client.policy.QueryPolicy, Statement, PartitionFilter, QueryListener)
 * @see Statement
 * @see QueryListenerExecutor
 * @see QueryListenerCommand
 */
public interface QueryListener {
	/**
	 * This method is called when a record is received from the server.
	 * The receive sequence is not ordered.
	 * <p>
	 * The user may throw a
	 * {@link com.aerospike.client.AerospikeException.QueryTerminated AerospikeException.QueryTerminated}
	 * exception if the command should be aborted. If an exception is thrown, parallel query command
	 * threads to other nodes will also be terminated.
	 *
	 * @param key					unique record identifier
	 * @param record				record instance
	 * @throws AerospikeException	when an error occurs or the query should be terminated (e.g. throw {@link AerospikeException.QueryTerminated} to terminate).
	 */
	public void onRecord(Key key, Record record);
}

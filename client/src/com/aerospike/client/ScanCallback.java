/*
 * Copyright 2012-2021 Aerospike, Inc.
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
package com.aerospike.client;

/**
 * An object implementing this interface is passed in <code>scan()</code> calls, so the caller can
 * be notified with scan results.
 * @deprecated
 * <p>Use {@link com.aerospike.client.query.QueryListener} with {@link com.aerospike.client.AerospikeClient#query}
 * instead.Create a {@link com.aerospike.client.query.Statement} with the same namespace and set name (no filter),
 * then call {@code query(queryPolicy, statement, queryListener)}. For partition-scoped reads use
 * {@link com.aerospike.client.AerospikeClient#queryPartitions}.
 *
 * <p><b>Example (deprecated; prefer query with QueryListener):</b>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * try {
 *   ScanCallback callback = (key, record) -> {
 *     if (record != null) {
 *       Object val = record.getValue("mybin");
 *     }
 *   };
 *   client.scanAll(null, "test", "set1", callback, "mybin");
 * } finally {
 *   client.close();
 * }
 * }</pre>
 *
 * @see com.aerospike.client.query.QueryListener
 * @see com.aerospike.client.AerospikeClient#query
 * @see com.aerospike.client.AerospikeClient#queryPartitions
 */
@Deprecated
public interface ScanCallback {
	/**
	 * This method will be called for each record returned from a scan. The user may throw a
	 * {@link com.aerospike.client.AerospikeException.ScanTerminated AerospikeException.ScanTerminated}
	 * exception if the scan should be aborted.  If any exception is thrown, parallel scan threads
	 * to other nodes will also be terminated and the exception will be propagated back through the
	 * initiating scan call.
	 * <p>
	 * If {@link com.aerospike.client.policy.ScanPolicy#concurrentNodes} is true and maxConcurrentNodes is not one, the implementation must be thread-safe.
	 *
	 * @param key    unique record identifier; never null
	 * @param record container for bins and metadata; never null
	 * @throws AerospikeException when the callback wishes to abort the scan or report an error (e.g. throw ScanTerminated to abort).
	 */
	public void scanCallback(Key key, Record record) throws AerospikeException;
}

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
 * Callback invoked for each record returned by a legacy scan operation.
 *
 * <p>Implement this interface and pass it to scan methods. Throw {@link AerospikeException.ScanTerminated} to
 * abort the scan. If {@link com.aerospike.client.policy.ScanPolicy#concurrentNodes} is true and
 * maxConcurrentNodes is not 1, the implementation must be thread-safe.
 *
 * <p>Implement a callback and pass it to scanAll to process each record.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * try {
 *     ScanCallback callback = (key, record) -> {
 *         Object val = record.getValue("mybin");
 *         // process key, record; throw new AerospikeException.ScanTerminated() to abort
 *     };
 *     client.scanAll(null, "test", "set1", callback, "mybin");
 * } finally { client.close(); }
 * }</pre>
 *
 * @deprecated Use {@link com.aerospike.client.AerospikeClient#query(com.aerospike.client.policy.QueryPolicy, com.aerospike.client.query.Statement, com.aerospike.client.query.QueryListener)} with a
 *             {@link com.aerospike.client.query.Statement} that has no filter (primary index query) and a {@link com.aerospike.client.query.QueryListener} instead
 */
@Deprecated
public interface ScanCallback {
	/**
	 * Invoked for each record returned from the scan.
	 *
	 * <p>Throw {@link AerospikeException.ScanTerminated} to abort the scan; the exception is propagated to the caller.
	 *
	 * @param key the record key; must not be {@code null}
	 * @param record the record (bins, generation, expiration); may be {@code null} if only digests requested
	 * @throws AerospikeException	when the callback wishes to abort the scan or report an error (e.g. throw {@link AerospikeException.ScanTerminated} to abort).
	 */
	public void scanCallback(Key key, Record record) throws AerospikeException;
}

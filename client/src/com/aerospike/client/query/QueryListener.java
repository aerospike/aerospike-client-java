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
 * Callback for each record in a synchronous secondary-index query; results are streamed one record at a time.
 * <p>
 * Pass to {@link com.aerospike.client.AerospikeClient#query(com.aerospike.client.policy.QueryPolicy, Statement, QueryListener)} when processing records one-by-one instead of buffering a {@link RecordSet}. Throw {@link com.aerospike.client.AerospikeException.QueryTerminated} from {@link #onRecord} to abort.
 * <p>Query with a QueryListener to process each record in a callback.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * Statement stmt = new Statement();
 * stmt.setNamespace("test");
 * stmt.setSetName("set1");
 * stmt.setFilter(Filter.equal("status", "active"));
 * client.query(null, stmt, (key, record) -> {
 *   if (record != null) { Object val = record.getValue("mybin"); }
 * });
 * client.close();
 * }</pre>
 *
 * @see RecordSet
 * @see Statement
 * @see Filter
 * @see com.aerospike.client.AerospikeClient#query(com.aerospike.client.policy.QueryPolicy, Statement, QueryListener)
 * @see com.aerospike.client.AerospikeException.QueryTerminated
 */
public interface QueryListener {
	/**
	 * Called when a record is received; order is not guaranteed. Throw {@link com.aerospike.client.AerospikeException.QueryTerminated} to abort.
	 * @param key record key (must not be null)
	 * @param record record if found, otherwise null
	 * @throws AerospikeException when the query should be terminated (e.g. throw QueryTerminated to abort)
	 */
	public void onRecord(Key key, Record record);
}

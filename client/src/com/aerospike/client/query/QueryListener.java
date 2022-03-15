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
 * Result notification for sync query command.
 * The results are sent one record at a time.
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
	 * @throws AerospikeException	if error occurs or query should be terminated.
	 */
	public void onRecord(Key key, Record record);
}

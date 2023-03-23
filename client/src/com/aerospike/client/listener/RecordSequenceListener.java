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
	 * If this listener is used in a scan/query command, The user may throw a
	 * {@link com.aerospike.client.AerospikeException.QueryTerminated AerospikeException.QueryTerminated}
	 * exception if the command should be aborted. If any exception is thrown, parallel command threads
	 * to other nodes will also be terminated and the exception will be propagated back through the
	 * onFailure() call.
	 * <p>
	 * If this listener is used in a batch command, an user thrown exception will terminate the batch
	 * to the current node, but parallel batch command threads to other nodes will continue to run.
	 *
	 * @param key					unique record identifier
	 * @param record				record instance, will be null if the key is not found
	 * @throws AerospikeException	if error occurs or scan should be terminated.
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

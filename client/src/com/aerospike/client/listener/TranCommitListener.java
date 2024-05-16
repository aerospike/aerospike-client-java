/*
 * Copyright 2012-2024 Aerospike, Inc.
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
 * Asynchronous result notifications for multi-record transaction (MRT) commits.
 */
public interface TranCommitListener {
	/**
	 * This method is called when the records are verified and the commit succeeds.
	 */
	void onSuccess();

	/**
	 * This method is called when the records are verified, but the one or more records in the
	 * commit step fails.
	 */
	void onCommitFailure(BatchRecord[] records, AerospikeException ae);

	/**
	 * This method is called when one or more records in the verify step fails. If this method is called,
	 * the client will attempt to abort the MRT. If the abort succeeds, {@link #onAbort()} is called.
	 * If the abort fails, {@link #onAbortFailure(BatchRecord[], AerospikeException)} is called.
	 */
	void onVerifyFailure(BatchRecord[] records, AerospikeException ae);

	/**
	 * This method is called when the record verification step fails and the abort (rollback) succeeds.
	 */
	void onAbort();

	/**
	 * This method is called when one or more records in the abort step fails.
	 */
	void onAbortFailure(BatchRecord[] records, AerospikeException ae);
}

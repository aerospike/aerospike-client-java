/*
 * Copyright 2012-2025 Aerospike, Inc.
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
 * Transaction abort result returned by {@link com.aerospike.client.AerospikeClient#abort}. OK and ALREADY_ABORTED indicate success; abandoned statuses mean the server will complete asynchronously.
 *
 * <p><b>Example:</b>
 * <p>Abort a transaction and check the returned status.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * try {
 *   Txn txn = new Txn();
 *   try {
 *     client.commit(txn);
 *   } catch (AerospikeException.Commit e) {
 *     AbortStatus status = client.abort(txn);
 *     if (status != AbortStatus.OK && status != AbortStatus.ALREADY_ABORTED) {
 *       System.err.println(status.str);
 *     }
 *   }
 * } finally {
 *   client.close();
 * }
 * }</pre>
 *
 * @see CommitStatus
 * @see CommitError
 * @see com.aerospike.client.AerospikeClient#abort
 */
public enum AbortStatus {
	OK("Abort succeeded"),
	ALREADY_ABORTED("Already aborted"),
	ROLL_BACK_ABANDONED("Transaction client roll back abandoned. Server will eventually abort the transaction."),
	CLOSE_ABANDONED("Transaction has been rolled back, but transaction client close was abandoned. Server will eventually close the transaction.");

	/** Human-readable message for this status. */
	public final String str;

	AbortStatus(String str) {
		this.str = str;
	}
}

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
 * Status code for a transaction abort operation.
 *
 * <p>Indicates success ({@link #OK}, {@link #ALREADY_ABORTED}) or that the client roll-back/close was abandoned
 * and the server will eventually abort. The {@link #str} field holds the full message for logging or display.
 *
 * <p>Abort a transaction and check the returned status.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient(new ClientPolicy(), "localhost", 3000);
 * Txn txn = new Txn();
 * // ... attach txn to multi-record ops, then decide to abort
 * AbortStatus status = client.abort(txn);
 * if (status == AbortStatus.OK || status == AbortStatus.ALREADY_ABORTED) {
 *     // Abort completed or was already aborted
 * } else {
 *     // ROLL_BACK_ABANDONED or CLOSE_ABANDONED; server will eventually abort
 *     System.err.println(status.str);
 * }
 * }</pre>
 *
 * @see com.aerospike.client.CommitStatus
 * @see com.aerospike.client.CommitError
 * @see com.aerospike.client.AerospikeClient#abort
 */
public enum AbortStatus {
	/** Abort completed successfully. */
	OK("Abort succeeded"),

	/** Transaction was already aborted (e.g. duplicate abort). */
	ALREADY_ABORTED("Already aborted"),

	/** Client roll-back was abandoned; server will eventually abort the transaction. */
	ROLL_BACK_ABANDONED("Transaction client roll back abandoned. Server will eventually abort the transaction."),

	/** Transaction was rolled back but client close was abandoned; server will eventually close. */
	CLOSE_ABANDONED("Transaction has been rolled back, but transaction client close was abandoned. Server will eventually close the transaction.");

	/**
	 * Full status message for this value; suitable for logging or user display.
	 */
	public final String str;

	/**
	 * Constructor for enum constant.
	 *
	 * @param str the full status message
	 */
	AbortStatus(String str) {
		this.str = str;
	}
}

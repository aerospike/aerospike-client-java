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
 * Error status for a failed transaction commit, used in {@link AerospikeException.Commit}.
 *
 * <p>Indicates whether verify failed, abort/close was abandoned, or roll-forward was abandoned.
 * The {@link #str} field holds the full message for logging or display.
 *
 * <p>Catch AerospikeException.Commit and switch on the error field.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * try {
 *     CommitStatus status = client.commit(txn);
 * } catch (AerospikeException.Commit e) {
 *     CommitError err = e.error;
 *     switch (err) {
 *         case VERIFY_FAIL: // handle verify failure; break;
 *         case VERIFY_FAIL_CLOSE_ABANDONED: // handle; break;
 *         default: System.err.println(err.str);
 *     }
 * }
 * }</pre>
 *
 * @see AerospikeException.Commit#error
 * @see CommitStatus
 * @see AbortStatus
 */
public enum CommitError {
	/** Transaction verify failed and the transaction was aborted. */
	VERIFY_FAIL("Transaction verify failed. Transaction aborted."),

	/** Transaction verify failed; transaction aborted and client close was abandoned; server will eventually close. */
	VERIFY_FAIL_CLOSE_ABANDONED("Transaction verify failed. Transaction aborted. Transaction client close abandoned. Server will eventually close the transaction."),

	/** Transaction verify failed; client abort was abandoned; server will eventually abort. */
	VERIFY_FAIL_ABORT_ABANDONED("Transaction verify failed. Transaction client abort abandoned. Server will eventually abort the transaction."),

	/** Client mark roll-forward was abandoned; server will eventually abort the transaction. */
	MARK_ROLL_FORWARD_ABANDONED("Transaction client mark roll forward abandoned. Server will eventually abort the transaction.");

	/**
	 * Full error message for this status; suitable for logging or user display.
	 */
	public final String str;

	/**
	 * Constructor for enum constant.
	 *
	 * @param str the full error message for this status
	 */
	CommitError(String str) {
		this.str = str;
	}
}

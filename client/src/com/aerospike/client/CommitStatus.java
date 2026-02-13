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
 * Status code for a transaction commit operation.
 *
 * <p>Indicates success ({@link #OK}, {@link #ALREADY_COMMITTED}) or that the client commit/close was abandoned
 * and the server will eventually complete. The {@link #str} field holds the full message for logging or display.
 *
 * <p>Call commit on a transaction and switch on the returned status.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * CommitStatus status = client.commit(txn);
 * switch (status) {
 *     case OK:
 *     case ALREADY_COMMITTED:
 *         // transaction committed successfully
 *         break;
 *     case ROLL_FORWARD_ABANDONED:
 *     case CLOSE_ABANDONED:
 *         // server will eventually complete; log status.str if needed
 *         break;
 * }
 * }</pre>
 *
 * @see AbortStatus
 * @see CommitError
 */
public enum CommitStatus {
	/** Commit completed successfully. */
	OK("Commit succeeded"),

	/** Transaction was already committed (e.g. duplicate commit). */
	ALREADY_COMMITTED("Already committed"),

	/** Client roll-forward was abandoned; server will eventually commit the transaction. */
	ROLL_FORWARD_ABANDONED("Transaction client roll forward abandoned. Server will eventually commit the transaction."),

	/** Transaction was rolled forward but client close was abandoned; server will eventually close. */
	CLOSE_ABANDONED("Transaction has been rolled forward, but transaction client close was abandoned. Server will eventually close the transaction.");

	/**
	 * Full status message for this value; suitable for logging or user display.
	 */
	public final String str;

	/**
	 * Constructor for enum constant.
	 *
	 * @param str the full status message
	 */
	CommitStatus(String str) {
		this.str = str;
	}
}

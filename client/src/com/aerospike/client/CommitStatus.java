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
 * Transaction commit result returned by {@link com.aerospike.client.AerospikeClient#commit}. OK and ALREADY_COMMITTED indicate success; abandoned statuses mean the server will complete asynchronously.
 *
 * <p><b>Example:</b>
 * <p>Commit a transaction and handle the returned status (OK, abandoned, etc.).</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * CommitStatus status = client.commit(txn);
 * switch (status) {
 *   case OK:
 *   case ALREADY_COMMITTED:
 *     break;
 *   case ROLL_FORWARD_ABANDONED:
 *   case CLOSE_ABANDONED:
 *     System.err.println(status.str);
 *     break;
 * }
 * }</pre>
 *
 * @see AbortStatus
 * @see CommitError
 * @see com.aerospike.client.AerospikeClient#commit
 */
public enum CommitStatus {
	OK("Commit succeeded"),
	ALREADY_COMMITTED("Already committed"),
	ROLL_FORWARD_ABANDONED("Transaction client roll forward abandoned. Server will eventually commit the transaction."),
	CLOSE_ABANDONED("Transaction has been rolled forward, but transaction client close was abandoned. Server will eventually close the transaction.");

	/** Human-readable message for this status. */
	public final String str;

	CommitStatus(String str) {
		this.str = str;
	}
}

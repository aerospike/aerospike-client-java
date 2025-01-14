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
 * Transaction error status.
 */
public enum CommitError {
	VERIFY_FAIL("Transaction verify failed. Transaction aborted."),
	VERIFY_FAIL_CLOSE_ABANDONED("Transaction verify failed. Transaction aborted. Transaction client close abandoned. Server will eventually close the transaction."),
	VERIFY_FAIL_ABORT_ABANDONED("Transaction verify failed. Transaction client abort abandoned. Server will eventually abort the transaction."),
	MARK_ROLL_FORWARD_ABANDONED("Transaction client mark roll forward abandoned. Server will eventually abort the transaction.");

	public final String str;

	CommitError(String str) {
		this.str = str;
	}
}

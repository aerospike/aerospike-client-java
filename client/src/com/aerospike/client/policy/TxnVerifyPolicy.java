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
package com.aerospike.client.policy;

/**
 * Transaction policy fields used to batch verify record versions on commit.
 * Used a placeholder for now as there are no additional fields beyond BatchPolicy.
 */
public class TxnVerifyPolicy extends BatchPolicy {
	/**
	 * Copy policy from another policy.
	 */
	public TxnVerifyPolicy(TxnVerifyPolicy other) {
		super(other);
	}

	/**
	 * Default constructor.
	 */
	public TxnVerifyPolicy() {
		readModeSC = PolicyDefaultValues.READ_MODE_SC_TXN;
		replica = PolicyDefaultValues.REPLICA_TXN;
		maxRetries = PolicyDefaultValues.MAX_RETRIES_TXN;
		socketTimeout = PolicyDefaultValues.SOCKET_TIMEOUT_TXN;
		totalTimeout = PolicyDefaultValues.TOTAL_TIMEOUT_TXN;
		sleepBetweenRetries = PolicyDefaultValues.SLEEP_BETWEEN_RETRIES;
	}
}

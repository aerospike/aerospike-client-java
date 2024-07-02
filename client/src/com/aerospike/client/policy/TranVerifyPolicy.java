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
package com.aerospike.client.policy;

import com.aerospike.client.Tran;

/**
 * Multi-record transaction (MRT) policy fields used to batch verify record versions in
 * {@link com.aerospike.client.AerospikeClient#tranCommit(Tran)}.
 * Used a placeholder for now as there are no additional fields beyond BatchPolicy.
 */
public class TranVerifyPolicy extends BatchPolicy {
	/**
	 * Copy policy from another policy.
	 */
	public TranVerifyPolicy(TranVerifyPolicy other) {
		super(other);
	}

	/**
	 * Default constructor.
	 */
	public TranVerifyPolicy() {
		readModeSC = ReadModeSC.LINEARIZE;
		replica = Replica.MASTER;
		maxRetries = 5;
		totalTimeout = 10000;
		sleepBetweenRetries = 1000;
	}
}
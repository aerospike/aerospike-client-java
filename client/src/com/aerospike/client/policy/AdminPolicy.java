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

/**
 * Policy for admin commands (create/drop user/role, grant/revoke, etc.): socket timeout.
 * <p>
 * Pass to {@link com.aerospike.client.AerospikeClient#createUser}, {@link com.aerospike.client.AerospikeClient#createRole}, and other admin methods. Default timeout 0 (no timeout).
 *
 * <p><b>Example:</b>
 * <p>Create a user with a 5-second timeout for the admin command.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * AdminPolicy ap = new AdminPolicy();
 * ap.timeout = 5000;
 * client.createUser(ap, "jdoe", "secret", java.util.Collections.emptyList());
 * }</pre>
 *
 * @see com.aerospike.client.AerospikeClient#createUser
 * @see com.aerospike.client.AerospikeClient#createRole
 */
public final class AdminPolicy {
	/**
	 * User administration command socket timeout in milliseconds.
	 * <p>
	 * Default: 0 (no timeout)
	 */
	public int timeout;

	/**
	 * Copy admin policy from another admin policy.
	 */
	public AdminPolicy(AdminPolicy other) {
		this.timeout = other.timeout;
	}

	/**
	 * Default constructor.
	 */
	public AdminPolicy() {
	}

	// Include setter to facilitate Spring's ConfigurationProperties.

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}
}

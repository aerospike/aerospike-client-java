/*
 * Copyright 2012-2021 Aerospike, Inc.
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
package com.aerospike.client.admin;

/**
 * Single privilege (code plus optional namespace/set scope) for role-based access; use in {@link com.aerospike.client.AerospikeClient#createRole} and {@link com.aerospike.client.AerospikeClient#grantPrivileges}.
 * <p>
 * Set {@link #namespace} and {@link #setName} to null for global scope when the privilege allows it; use {@link PrivilegeCode#canScope()} to check.
 *
 * <p><b>Example:</b>
 * <p>Create a privilege with namespace scope and use it in createRole.</p>
 * <pre>{@code
 * Privilege p = new Privilege();
 * p.code = PrivilegeCode.READ_WRITE;
 * p.namespace = "test";
 * p.setName = null;
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * try {
 *   client.createRole(null, "myrole", Collections.singletonList(p));
 * } finally {
 *   client.close();
 * }
 * }</pre>
 *
 * @see PrivilegeCode
 * @see com.aerospike.client.AerospikeClient#createRole
 * @see com.aerospike.client.AerospikeClient#grantPrivileges
 * @see com.aerospike.client.AerospikeClient#revokePrivileges
 */
public final class Privilege {
	/** Privilege type; must not be null. */
	public PrivilegeCode code;

	/** Namespace scope; null means all namespaces (when privilege allows). */
	public String namespace;

	/** Set name scope within namespace; null means all sets in the namespace. */
	public String setName;
}

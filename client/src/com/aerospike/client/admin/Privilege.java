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
 * User privilege defining a permission and optional namespace/set scope.
 * Used with role administration APIs such as {@link com.aerospike.client.AerospikeClient#createRole createRole}
 * and {@link com.aerospike.client.AerospikeClient#grantPrivileges grantPrivileges}.
 *
 * <p>Create a privilege for read-write on namespace "test" and set "users", then create a role with that privilege.</p>
 * <pre>{@code
 * Privilege p = new Privilege();
 * p.code = PrivilegeCode.READ_WRITE;
 * p.namespace = "test";
 * p.setName = "users";
 * List<Privilege> privileges = Collections.singletonList(p);
 * client.createRole(null, "myrole", privileges);
 * }</pre>
 *
 * @see PrivilegeCode
 * @see com.aerospike.client.AerospikeClient#createRole
 * @see com.aerospike.client.AerospikeClient#grantPrivileges
 */
public final class Privilege {
	/**
	 * Privilege code; must not be null.
	 */
	public PrivilegeCode code;

	/**
	 * Namespace scope. Apply permission to this namespace only.
	 * If null, the privilege applies to all namespaces.
	 */
	public String namespace;

	/**
	 * Set name scope. Apply permission to this set within namespace only.
	 * If null, the privilege applies to all sets within namespace.
	 */
	public String setName;
}

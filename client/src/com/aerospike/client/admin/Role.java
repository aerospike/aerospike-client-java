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
package com.aerospike.client.admin;

import java.util.List;

/**
 * Role definition: name, privileges, optional whitelist and read/write quotas. Returned by {@link com.aerospike.client.AerospikeClient#queryRole}; privilege list is used in createRole and grantPrivileges.
 * <p>
 * Use the static constants (e.g. {@link #Read}, {@link #ReadWrite}) as role names when creating users; use {@link Privilege} for fine-grained role definitions.
 *
 * <p><b>Example:</b>
 * <p>Query a role and read its name and privileges.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * try {
 *   Role role = client.queryRole(null, "myrole");
 *   if (role != null) {
 *     String name = role.name;
 *     List privs = role.privileges;
 *   }
 * } finally {
 *   client.close();
 * }
 * }</pre>
 *
 * @see Privilege
 * @see com.aerospike.client.AerospikeClient#queryRole
 * @see com.aerospike.client.AerospikeClient#queryRoles
 * @see com.aerospike.client.AerospikeClient#createRole
 */
public final class Role {
	/** Role name for user administration. */
	public static final String UserAdmin = "user-admin";

	/** Role name for server configuration. */
	public static final String SysAdmin = "sys-admin";

	/** Role name for UDF and index administration. */
	public static final String DataAdmin = "data-admin";

	/** Role name for UDF administration. */
	public static final String UDFAdmin = "udf-admin";

	/** Role name for index administration. */
	public static final String SIndexAdmin = "sindex-admin";

	/** Role name for read-only access. */
	public static final String Read = "read";

	/** Role name for read and write access. */
	public static final String ReadWrite = "read-write";

	/** Role name for read/write via UDF. */
	public static final String ReadWriteUdf = "read-write-udf";

	/** Role name for write-only access. */
	public static final String Write = "write";

	/** Role name for truncate. */
	public static final String Truncate = "truncate";

	/** Role name for data masking administration. */
	public static final String MaskingAdmin = "masking-admin";

	/** Role name for read masked data. */
	public static final String ReadMasked = "read-masked";

	/** Role name for write masked data. */
	public static final String WriteMasked = "write-masked";

	/** Role name. */
	public String name;

	/** Assigned privileges (namespace/set-scoped when applicable). */
	public List<Privilege> privileges;

	/** Allowed IP addresses (whitelist); null or empty for no restriction. */
	public List<String> whitelist;

	/** Maximum reads per second (0 for no limit). */
	public int readQuota;

	/** Maximum writes per second (0 for no limit). */
	public int writeQuota;

	public String toString() {
		return "Role [name=" + name + ", privileges=" + privileges + ", whitelist=" + whitelist + ", readQuota="
				+ readQuota + ", writeQuota=" + writeQuota + "]";
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Role other = (Role) obj;
		if (name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!name.equals(other.name)) {
			return false;
		}
		return true;
	}
}

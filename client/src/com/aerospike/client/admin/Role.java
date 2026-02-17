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
 * Role definition.
 */
public final class Role {
	/**
	 * Manage users their roles.
	 */
	public static final String UserAdmin = "user-admin";

	/**
	 * Manage server configuration.
	 */
	public static final String SysAdmin = "sys-admin";

	/**
	 * Manage user defined functions and indicies.
	 */
	public static final String DataAdmin = "data-admin";

	/**
	 * Manage user defined functions.
	 */
	public static final String UDFAdmin = "udf-admin";

	/**
	 * Manage indicies.
	 */
	public static final String SIndexAdmin = "sindex-admin";

	/**
	 * Allow read commands.
	 */
	public static final String Read = "read";

	/**
	 * Allow read and write commands.
	 */
	public static final String ReadWrite = "read-write";

	/**
	 * Allow read and write commands within user defined functions.
	 */
	public static final String ReadWriteUdf = "read-write-udf";

	/**
	 * Allow write commands.
	 */
	public static final String Write = "write";

	/**
	 * Allow truncate.
	 */
	public static final String Truncate = "truncate";

	/**
	 * Manage data masking.
	 */
	public static final String MaskingAdmin = "masking-admin";

	/**
	 * Allow read masked data.
	 */
	public static final String ReadMasked = "read-masked";

	/**
	 * Allow write masked data.
	 */
	public static final String WriteMasked = "write-masked";

	/**
	 * Role name.
	 */
	public String name;

	/**
	 * List of assigned privileges.
	 */
	public List<Privilege> privileges;

	/**
	 * List of allowable IP addresses.
	 */
	public List<String> whitelist;

	/**
	 * Maximum reads per second limit.
	 */
	public int readQuota;

	/**
	 * Maximum writes per second limit.
	 */
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

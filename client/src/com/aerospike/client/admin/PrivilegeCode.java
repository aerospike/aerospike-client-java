/*
 * Copyright 2012-2020 Aerospike, Inc.
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

import com.aerospike.client.AerospikeException;

/**
 * Permission codes define the type of permission granted for a user's role.
 */
public enum PrivilegeCode {
	/**
	 * User can edit/remove other users.  Global scope only.
	 */
	USER_ADMIN(0, Role.UserAdmin),

	/**
	 * User can perform systems administration functions on a database that do not involve user
	 * administration.  Examples include server configuration.
	 * Global scope only.
	 */
	SYS_ADMIN(1, Role.SysAdmin),

	/**
	 * User can perform data administration functions on a database that do not involve user
	 * administration.  Examples include index and user defined function management.
	 * Global scope only.
	 */
	DATA_ADMIN(2, Role.DataAdmin),

	/**
	 * User can read data.
	 */
	READ(10, Role.Read),

	/**
	 * User can read and write data.
	 */
	READ_WRITE(11, Role.ReadWrite),

	/**
	 * User can read and write data through user defined functions.
	 */
	READ_WRITE_UDF(12, Role.ReadWriteUdf),

	/**
	 * User can write data.
	 */
	WRITE(13, Role.Write);

	/**
	 * Privilege code ID used in wire protocol.
	 */
	public final int id;
	private final String value;

	private PrivilegeCode(int id, String value) {
		this.id = id;
		this.value = value;
	}

	/**
	 * Can privilege be scoped with namespace and set.
	 */
	public boolean canScope() {
		return id >= 10;
	}

	/**
	 * Convert ID to privilege code enum.
	 */
	public static PrivilegeCode fromId(int id) {
		switch (id) {
		case 0:
			return USER_ADMIN;

		case 1:
			return SYS_ADMIN;

		case 2:
			return DATA_ADMIN;

		case 10:
			return READ;

		case 11:
			return READ_WRITE;

		case 12:
			return READ_WRITE_UDF;

		case 13:
			return WRITE;

		default:
			throw new AerospikeException("Invalid privilege code: " + id);
		}
	}

	/**
	 * Convert code to string.
	 */
	@Override
	public String toString() {
		return value;
	}
}

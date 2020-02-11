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
	 * Manage indicies and user defined functions.
	 */
	public static final String DataAdmin = "data-admin";

	/**
	 * Allow read transactions.
	 */
	public static final String Read = "read";

	/**
	 * Allow read and write transactions.
	 */
	public static final String ReadWrite = "read-write";

	/**
	 * Allow read and write transactions within user defined functions.
	 */
	public static final String ReadWriteUdf = "read-write-udf";

	/**
	 * Allow write transactions.
	 */
	public static final String Write = "write";

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
}

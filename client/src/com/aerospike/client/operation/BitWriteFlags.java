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
package com.aerospike.client.operation;

/**
 * Bitwise operation policy write flags.
 */
public final class BitWriteFlags {
	/**
	 * Default.  Allow create or update.
	 */
	public static final int DEFAULT = 0;

	/**
	 * If the bin already exists, the operation will be denied.
	 * If the bin does not exist, a new bin will be created.
	 */
	public static final int CREATE_ONLY = 1;

	/**
	 * If the bin already exists, the bin will be overwritten.
	 * If the bin does not exist, the operation will be denied.
	 */
	public static final int UPDATE_ONLY = 2;

	/**
	 * Do not raise error if operation is denied.
	 */
	public static final int NO_FAIL = 4;

	/**
	 * Allow other valid operations to be committed if this operations is
	 * denied due to flag constraints.
	 */
	public static final int PARTIAL = 8;
}

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
package com.aerospike.client.cdt;

/**
 * Map write bit flags.
 * Requires server versions >= 4.3.
 */
public final class MapWriteFlags {
	/**
	 * Default.  Allow create or update.
	 */
	public static final int DEFAULT = 0;

	/**
	 * If the key already exists, the item will be denied.
	 * If the key does not exist, a new item will be created.
	 */
	public static final int CREATE_ONLY = 1;

	/**
	 * If the key already exists, the item will be overwritten.
	 * If the key does not exist, the item will be denied.
	 */
	public static final int UPDATE_ONLY = 2;

	/**
	 * Do not raise error if a map item is denied due to write flag constraints.
	 */
	public static final int NO_FAIL = 4;

	/**
	 * Allow other valid map items to be committed if a map item is denied due to
	 * write flag constraints.
	 */
	public static final int PARTIAL = 8;
}

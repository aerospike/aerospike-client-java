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
 * List write bit flags.
 */
public final class ListWriteFlags {
	/**
	 * Default.  Allow duplicate values and insertions at any index.
	 */
	public static final int DEFAULT = 0;

	/**
	 * Only add unique values.
	 */
	public static final int ADD_UNIQUE = 1;

	/**
	 * Enforce list boundaries when inserting.  Do not allow values to be inserted
	 * at index outside current list boundaries.
	 */
	public static final int INSERT_BOUNDED = 2;

	/**
	 * Do not raise error if a list item fails due to write flag constraints.
	 */
	public static final int NO_FAIL = 4;

	/**
	 * Allow other valid list items to be committed if a list item fails due to
	 * write flag constraints.
	 */
	public static final int PARTIAL = 8;
}

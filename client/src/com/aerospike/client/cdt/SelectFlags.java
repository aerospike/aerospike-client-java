/*
 * Copyright 2012-2025 Aerospike, Inc.
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
 * CDT select flags used in {@link CdtOperation#selectByPath} and
 * {@link com.aerospike.client.exp.CdtExp#selectByPath}.
 */
public final class SelectFlags {
	/**
	 * Return a tree from the root (bin) level to the bottom of the tree,
	 * with only non-filtered out nodes.
	 */
	public static final int MATCHING_TREE = 0;

	/**
	 * Return the list of the values of the nodes finally selected by the context.
	 * For maps, this returns the value of each (key, value) pair.
	 */
	public static final int VALUE = 1;

	/**
	 * Return the list of the values of the nodes finally selected by the context.
	 * This is a alias for {@link #VALUE} to make it clear in your
	 * source code that you're expecting a list.
	 */
	public static final int LIST_VALUE = 1;

	/**
	 * Return the list of map values of the nodes finally selected by the context.
	 * This is an alias for {@link #VALUE} to make it clear in your
	 * source code that you're expecting a map. See also {@link #MAP_KEY_VALUE}. 
	 */
	public static final int MAP_VALUE = 1;

	/**
	 * Return the list of map keys of the nodes finally selected by the context.
	 */
	public static final int MAP_KEY = 2;

	/**
	 * Returns the list of map (key, value) pairs of the nodes finally selected
	 * by the context. This is an alias for setting both
	 * {@link #MAP_KEY} and {@link #MAP_VALUE} bits together.
	 */
	public static final int MAP_KEY_VALUE = MAP_KEY | MAP_VALUE;

	/**
	 * If the expression in the context hits an invalid type (e.g., selects
	 * as an integer when the value is a string), do not fail the operation;
	 * just ignore those elements. Interpret an expression that returns UNKNOWN
	 * as false instead.
	 */
	public static final int NO_FAIL = 0x10;

	private SelectFlags() {}
}

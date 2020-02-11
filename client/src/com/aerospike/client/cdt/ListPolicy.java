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
 * List policy directives when creating a list and writing list items.
 */
public final class ListPolicy {
	/**
	 * Default unordered list with normal write semantics.
	 */
	public static final ListPolicy Default = new ListPolicy();

	protected final int attributes;
	protected final int flags;

	/**
	 * Default constructor.
	 * Create unordered list when list does not exist.
	 * Use normal update mode when writing list items.
	 */
	public ListPolicy() {
		this(ListOrder.UNORDERED, ListWriteFlags.DEFAULT);
	}

	/**
	 * Create list with specified order when list does not exist.
	 * Use specified write flags when writing list items.
	 */
	public ListPolicy(ListOrder order, int flags) {
		this.attributes = order.attributes;
		this.flags = flags;
	}
}

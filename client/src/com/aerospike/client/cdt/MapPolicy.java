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
 * Map policy directives when creating a map and writing map items.
 */
public final class MapPolicy {
	/**
	 * Default unordered unique key map with normal put semantics.
	 */
	public static final MapPolicy Default = new MapPolicy();

	protected final int attributes;
	protected final int flags;
	protected final int itemCommand;
	protected final int itemsCommand;

	/**
	 * Default constructor.
	 * Create unordered unique key map when map does not exist.
	 * Use normal update mode when writing map items.
	 */
	public MapPolicy() {
		this(MapOrder.UNORDERED, MapWriteMode.UPDATE);
	}

	/**
	 * Create unique key map with specified order when map does not exist.
	 * Use specified write mode when writing map items.
	 * <p>
	 * This constructor should only be used for server versions < 4.3.
	 * {@link MapPolicy#MapPolicy(MapOrder,int)} is recommended for server versions >= 4.3.
	 */
	public MapPolicy(MapOrder order, MapWriteMode writeMode) {
		this.attributes = order.attributes;
		this.flags = MapWriteFlags.DEFAULT;
		this.itemCommand = writeMode.itemCommand;
		this.itemsCommand = writeMode.itemsCommand;
	}

	/**
	 * Create unique key map with specified order when map does not exist.
	 * Use specified write flags {@link MapWriteFlags} when writing map items.
	 */
	public MapPolicy(MapOrder order, int flags) {
		this.attributes = order.attributes;
		this.flags = flags;
		this.itemCommand = MapWriteMode.UPDATE.itemCommand;
		this.itemsCommand = MapWriteMode.UPDATE.itemsCommand;
	}
}

/*
 * Copyright 2012-2023 Aerospike, Inc.
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
 * Map policy: order and write flags when creating a map or writing map items.
 * <p>
 * Use with {@link MapOperation#put}, {@link MapOperation#putItems}, {@link MapOperation#replace}, etc. when the map may not exist or when you need key-ordered or update semantics.
 * <p>Use MapPolicy with put and putItems.</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * MapPolicy policy = MapPolicy.Default;
 * client.operate(null, key, MapOperation.put(policy, "bin", Value.get("k"), Value.get(42)));
 *
 * MapPolicy keyOrdered = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT);
 * client.operate(null, key, MapOperation.putItems(keyOrdered, "bin", map));
 * }</pre>
 *
 * @see MapOperation
 * @see MapOrder
 * @see MapWriteFlags
 */
public final class MapPolicy {
	/** Default unordered unique-key map with normal put semantics. */
	public static final MapPolicy Default = new MapPolicy();

	public final int attributes;
	public final int flags;
	public final int itemCommand;
	public final int itemsCommand;

	/**
	 * Default constructor.
	 * Create unordered unique key map when map does not exist.
	 * Use normal update mode when writing map items.
	 */
	public MapPolicy() {
		this(MapOrder.UNORDERED, MapWriteFlags.DEFAULT);
	}

	/**
	 * Create unique key map with specified order when map does not exist.
	 * Use specified write mode when writing map items.
	 * <p>
	 * This constructor should only be used for server versions &lt; 4.3.
	 * {@link MapPolicy#MapPolicy(MapOrder,int)} is recommended for server versions &gt;= 4.3.
	 */
	public MapPolicy(MapOrder order, MapWriteMode writeMode) {
		this.attributes = order.attributes;
		this.flags = MapWriteFlags.DEFAULT;
		this.itemCommand = writeMode.itemCommand;
		this.itemsCommand = writeMode.itemsCommand;
	}

	/**
	 * Create unique key map with specified order when map does not exist.
	 *
	 * @param order			map order
	 * @param flags			map write flags. See {@link MapWriteFlags}.
	 */
	public MapPolicy(MapOrder order, int flags) {
		this.attributes = order.attributes;
		this.flags = flags;
		this.itemCommand = MapWriteMode.UPDATE.itemCommand;
		this.itemsCommand = MapWriteMode.UPDATE.itemsCommand;
	}

	/**
	 * Create unique key map with specified order and persist index flag when map does not exist.
	 *
	 * @param order			map order
	 * @param flags			map write flags. See {@link MapWriteFlags}.
	 * @param persistIndex	if true, persist map index. A map index improves lookup performance,
	 * 						but requires more storage. A map index can be created for a top-level
	 * 						ordered map only. Nested and unordered map indexes are not supported.
	 */
	public MapPolicy(MapOrder order, int flags, boolean persistIndex) {
		int attr = order.attributes;

		if (persistIndex) {
			attr |= 0x10;
		}

		this.attributes = attr;
		this.flags = flags;
		this.itemCommand = MapWriteMode.UPDATE.itemCommand;
		this.itemsCommand = MapWriteMode.UPDATE.itemsCommand;
	}
}

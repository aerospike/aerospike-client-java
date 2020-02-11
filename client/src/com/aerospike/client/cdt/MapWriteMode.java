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
 * Unique key map write type.
 * This enum should only be used for server versions < 4.3.
 * {@link MapWriteFlags} are recommended for server versions >= 4.3.
 */
public enum MapWriteMode {
	/**
	 * If the key already exists, the item will be overwritten.
	 * If the key does not exist, a new item will be created.
	 */
	UPDATE (MapOperation.PUT, MapOperation.PUT_ITEMS),

	/**
	 * If the key already exists, the item will be overwritten.
	 * If the key does not exist, the write will fail.
	 */
	UPDATE_ONLY (MapOperation.REPLACE, MapOperation.REPLACE_ITEMS),

	/**
	 * If the key already exists, the write will fail.
	 * If the key does not exist, a new item will be created.
	 */
	CREATE_ONLY (MapOperation.ADD, MapOperation.ADD_ITEMS);

	protected final int itemCommand;
	protected final int itemsCommand;

	private MapWriteMode(int itemCommand, int itemsCommand) {
		this.itemCommand = itemCommand;
		this.itemsCommand = itemsCommand;
	}
}

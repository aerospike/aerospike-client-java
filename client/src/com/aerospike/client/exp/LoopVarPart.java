/*
 * Copyright 2012-2024 Aerospike, Inc.
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
package com.aerospike.client.exp;

/**
 * Loop variable parts for expression loop variables.
 * Used to specify which part of a loop variable to access during iteration.
 */
public enum LoopVarPart {
	/**
	 * Access the key part of the loop variable.
	 * For maps, this refers to the map key.
	 * For lists, this refers to the list index.
	 */
	MAP_KEY(0),

	/**
	 * Access the value part of the loop variable.
	 * For maps, this refers to the map value.
	 * For lists, this refers to the list item value.
	 */
	VALUE(1),

	/**
	 * Returns a list of indexes.
	 */
	INDEX(2);
	
	
	public final int id;
	
	private LoopVarPart(int id) {
		this.id = id;
	}
}

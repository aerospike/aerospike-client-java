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

import com.aerospike.client.Value;

/**
 * Nested CDT context.  Identifies the location of nested list/map to apply the operation.
 * for the current level.  An array of CTX identifies location of the list/map on multiple
 * levels on nesting.
 */
public final class CTX {
	/**
	 * Lookup list by index offset.
	 * <p>
	 * If the index is negative, the resolved index starts backwards from end of list.
	 * If an index is out of bounds, a parameter error will be returned.  Examples:
	 * <ul>
	 * <li>0: First item.</li>
	 * <li>4: Fifth item.</li>
	 * <li>-1: Last item.</li>
	 * <li>-3: Third to last item.</li>
	 * </ul>
	 */
	public static CTX listIndex(int index) {
		return new CTX(0x10, Value.get(index));
	}

	/**
	 * Create list with given type at index offset.
	 */
	public static CTX listIndexCreate(int index, ListOrder order, boolean pad) {
		return new CTX(0x10 | order.getFlag(pad), Value.get(index));
	}

	/**
	 * Lookup list by rank.
	 * <ul>
	 * <li>0 = smallest value</li>
	 * <li>N = Nth smallest value</li>
	 * <li>-1 = largest value</li>
	 * </ul>
	 */
	public static CTX listRank(int rank) {
		return new CTX(0x11, Value.get(rank));
	}

	/**
	 * Lookup list by value.
	 */
	public static CTX listValue(Value key) {
		return new CTX(0x13, key);
	}

	/**
	 * Lookup map by index offset.
	 * <p>
	 * If the index is negative, the resolved index starts backwards from end of list.
	 * If an index is out of bounds, a parameter error will be returned.  Examples:
	 * <ul>
	 * <li>0: First item.</li>
	 * <li>4: Fifth item.</li>
	 * <li>-1: Last item.</li>
	 * <li>-3: Third to last item.</li>
	 * </ul>
	 */
	public static CTX mapIndex(int index) {
		return new CTX(0x20, Value.get(index));
	}

	/**
	 * Lookup map by rank.
	 * <ul>
	 * <li>0 = smallest value</li>
	 * <li>N = Nth smallest value</li>
	 * <li>-1 = largest value</li>
	 * </ul>
	 */
	public static CTX mapRank(int rank) {
		return new CTX(0x21, Value.get(rank));
	}

	/**
	 * Lookup map by key.
	 */
	public static CTX mapKey(Value key) {
		return new CTX(0x22, key);
	}

	/**
	 * Create map with given type at map key.
	 */
	public static CTX mapKeyCreate(Value key, MapOrder order) {
		return new CTX(0x22 | order.flag, key);
	}

	/**
	 * Lookup map by value.
	 */
	public static CTX mapValue(Value key) {
		return new CTX(0x23, key);
	}

	public final int id;
	public final Value value;

	private CTX(int id, Value value) {
		this.id = id;
		this.value = value;
	}
}

/*
 * Copyright 2012-2022 Aerospike, Inc.
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

import java.util.List;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.util.Crypto;
import com.aerospike.client.util.Pack;
import com.aerospike.client.util.Unpacker;

/**
 * Nested CDT context.  Identifies the location of nested list/map to apply the operation.
 * for the current level.  An array of CTX identifies location of the list/map on multiple
 * levels on nesting.
 */
public final class CTX {
	/**
	 * Apply operation to all children of the current context.
	 * This allows traversing all items in a collection without filtering.
	 */
	public static CTX allChildren() {
		Expression expression = Exp.build(Exp.val(true));
		return new CTX(Exp.CTX_EXP, expression);
	}

	public static CTX allChildrenWithFilter(Exp exp) {
		Expression expression = Exp.build(exp);
		return new CTX(Exp.CTX_EXP, expression);
	}

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
	 * Lookup list by base list's index offset. If the list at index offset is not found,
	 * create it with the given sort order at that index offset. If pad is true and the
	 * index offset is greater than the bounds of the base list, nil entries will be
	 * inserted before the newly created list.
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
	 * Lookup map by base map's key. If the map at key is not found,
	 * create it with the given sort order at that key.
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

	/**
	 * Serialize context array to bytes.
	 */
	public static byte[] toBytes(CTX[] ctx) {
		return Pack.pack(ctx);
	}

	/**
	 * Deserialize bytes to context array.
	 */
	public static CTX[] fromBytes(byte[] bytes) {
		List<?> list = (List<?>)Unpacker.unpackObjectList(bytes, 0, bytes.length);
		int max = list.size();
		CTX[] ctx = new CTX[max / 2];
		int i = 0;
		int count = 0;

		while (i < max) {
			int id = (int)(long)(Long)list.get(i);

			if (++i >= max) {
				throw new AerospikeException.Parse("List count must be even");
			}

			Object obj = list.get(i);
			// Check if this is an expression context based on the id
			if (id == Exp.CTX_EXP) {
				Expression exp = Exp.build(Exp.get(obj));
				ctx[count++] = new CTX(id, exp);
			} else {
				// This is a value context
				Value val = Value.get(obj);
				ctx[count++] = new CTX(id, val);
			}
			i++;
		}
		return ctx;
	}

	/**
	 * Serialize context array to base64 encoded string.
	 */
	public static String toBase64(CTX[] ctx) {
		byte[] bytes = Pack.pack(ctx);
		return Crypto.encodeBase64(bytes);
	}

	/**
	 * Deserialize base64 encoded string to context array.
	 */
	public static CTX[] fromBase64(String base64) {
		byte[] b64 = base64.getBytes();
		byte[] bytes = Crypto.decodeBase64(b64, 0, b64.length);
		return fromBytes(bytes);
	}

	public final int id;
	public final Value value;
	public final Expression exp;

	private CTX(int id, Value value) {
		this.id = id;
		this.value = value;
		this.exp = null;
	}

	private CTX(int id, Expression exp) {
		this.id = id;
		this.value = null;
		this.exp = exp;
	}
}

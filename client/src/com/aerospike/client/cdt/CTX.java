/*
 * Copyright 2012-2026 Aerospike, Inc.
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

import java.util.ArrayList;
import java.util.Arrays;
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

	/**
	 * Apply operation to all children of the current context that match the filter expression.
	 * This allows traversing all items in a collection with filtering.
	 */
	public static CTX allChildrenWithFilter(Exp exp) {
		Expression expression = Exp.build(exp);
		return new CTX(Exp.CTX_EXP, expression);
	}

	/**
	 * Apply operation to all children of the current context that match the filter expression.
	 * This allows traversing all items in a collection with filtering.
	 */
	public static CTX allChildrenWithFilter(Expression exp) {
		return new CTX(Exp.CTX_EXP, exp);
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
	 * Select map entries whose keys are contained in the provided string keys.
	 * <p>
	 * This context selects a subset of a map by matching its keys against
	 * the given keys. Only entries with keys present in {@code keys} are
	 * included. Can be combined with {@link #andFilter(Exp)} to apply
	 * additional filtering on the selected entries.
	 *
	 * <pre>{@code
	 * // Given map: {alpha: 10, beta: 20, gamma: 30, delta: 40}
	 * // Select only the "alpha" and "gamma" entries.
	 * CTX ctx = CTX.mapKeysIn("alpha", "gamma");
	 * Operation op = CdtOperation.selectByPath("myBin", SelectFlags.VALUE, ctx);
	 * Record result = client.operate(null, key, op);
	 * // result: [10, 30]
	 * }</pre>
	 *
	 * @param keys	string map keys to select
	 * @return		a map key-list context
	 * @see #andFilter(Exp)
	 * @see CdtOperation#selectByPath(String, int, CTX...)
	 */
	public static CTX mapKeysIn(String... keys) {
		return new CTX(0x2a, Value.get(Arrays.asList(keys)));
	}

	/**
	 * @hidden
	 * Public for compatibility; omitted from generated Javadoc. Prefer {@link #mapKeysIn(Value...)} for integer keys.
	 */
	public static CTX mapKeysIn(int... keys) {
		// Manual boxing required: Arrays.asList() on a primitive array wraps it as a single element, not per-element.
		List<Integer> list = new ArrayList<>(keys.length);
		for (int k : keys) {
			list.add(k);
		}
		return new CTX(0x2a, Value.get(list));
	}

	/**
	 * @hidden
	 * Public for compatibility; omitted from generated Javadoc. Prefer {@link #mapKeysIn(Value...)}.
	 */
	public static CTX mapKeysIn(long... keys) {
		// Manual boxing required: Arrays.asList() on a primitive array wraps it as a single element, not per-element.
		List<Long> list = new ArrayList<>(keys.length);
		for (long k : keys) {
			list.add(k);
		}
		return new CTX(0x2a, Value.get(list));
	}

	/**
	 * @hidden
	 * Public for compatibility; omitted from generated Javadoc. Encodes small integer keys, not blob keys;
	 * use {@link Value#get(byte[])} inside {@link #mapKeysIn(Value...)} for blob map keys.
	 */
	public static CTX mapKeysIn(byte... keys) {
		// Manual boxing required: Arrays.asList() on a primitive array wraps it as a single element, not per-element.
		List<Byte> list = new ArrayList<>(keys.length);
		for (byte k : keys) {
			list.add(k);
		}
		return new CTX(0x2a, Value.get(list));
	}

	/**
	 * @hidden
	 * Public for compatibility; omitted from generated Javadoc. Prefer {@link #mapKeysIn(Value...)}.
	 */
	public static CTX mapKeysIn(short... keys) {
		// Manual boxing required: Arrays.asList() on a primitive array wraps it as a single element, not per-element.
		List<Short> list = new ArrayList<>(keys.length);
		for (short k : keys) {
			list.add(k);
		}
		return new CTX(0x2a, Value.get(list));
	}

	/**
	 * Select map entries whose keys are contained in the provided key list.
	 * Each element is one CDT map key as a {@link Value} (for example string, integer, or
	 * blob via {@link Value#get(byte[])}). Aerospike map keys are restricted to integer, string,
	 * and blob types; mixed key types are allowed in one invocation when the map stores them.
	 *
	 * @param keys	map keys as {@link Value} instances
	 * @return		a map key-list context
	 * @see #mapKeysIn(String...)
	 * @see CdtOperation#selectByPath(String, int, CTX...)
	 */
	public static CTX mapKeysIn(Value... keys) {
		return new CTX(0x2a, Value.get(keys));
	}

	/**
	 * Apply an additional expression filter at the current context level.
	 * <p>
	 * This creates an AND filter that combines with the preceding context.
	 * Entries must satisfy both the preceding context and this filter expression
	 * to be included in the result. Typically used after {@link #mapKeysIn(String...)}
	 * or other selection contexts to further narrow the results.
	 *
	 * <p>Restrictions:
	 * <ul>
	 *   <li>Only one {@code andFilter} is allowed per context level. Multiple {@code andFilter}
	 *       calls cannot be chained. To combine multiple conditions, use {@link Exp#and(Exp...)}
	 *       within a single {@code andFilter}.</li>
	 *   <li>The preceding context entry must not be an expression type (i.e. {@code andFilter}
	 *       cannot follow {@link #allChildrenWithFilter(Exp)} or {@link #allChildren()}).</li>
	 *   <li>{@code andFilter} cannot be the first entry in the context chain.</li>
	 * </ul>
	 *
	 * <pre>{@code
	 * // Given map: {a: 5, b: 15, c: 25, d: 35}
	 * // Select keys "a", "b", "c" AND keep only entries where value > 10.
	 * CTX keys = CTX.mapKeysIn("a", "b", "c");
	 * CTX filter = CTX.andFilter(
	 *     Exp.gt(Exp.intLoopVar(LoopVarPart.VALUE), Exp.val(10))
	 * );
	 * Operation op = CdtOperation.selectByPath("myBin", SelectFlags.MAP_KEY_VALUE, keys, filter);
	 * Record result = client.operate(null, key, op);
	 * // result: {b: 15, c: 25}
	 * }</pre>
	 *
	 * @param exp	filter expression; entries that evaluate to false are excluded
	 * @return		an AND filter context
	 * @see #mapKeysIn(String...)
	 * @see CdtOperation#selectByPath(String, int, CTX...)
	 */
	public static CTX andFilter(Exp exp) {
		Expression expression = Exp.build(exp);
		return new CTX(Exp.CTX_AND | Exp.CTX_EXP, expression);
	}

	/**
	 * Apply an additional expression filter at the current context level.
	 * <p>
	 * This creates an AND filter that combines with the preceding context.
	 * Entries must satisfy both the preceding context and this filter expression
	 * to be included in the result. Typically used after {@link #mapKeysIn(String...)}
	 * or other selection contexts to further narrow the results.
	 *
	 * @param exp	compiled filter expression; entries that evaluate to false are excluded
	 * @return		an AND filter context
	 * @see #andFilter(Exp)
	 * @see #mapKeysIn(String...)
	 * @see CdtOperation#selectByPath(String, int, CTX...)
	 */
	public static CTX andFilter(Expression exp) {
		return new CTX(Exp.CTX_AND | Exp.CTX_EXP, exp);
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
			// Check if this is an expression context based on the low nibble of the id.
			// Mask with 0x0f so AND|EXP contexts (0x204) are correctly detected.
			if ((id & 0x0f) == Exp.CTX_EXP) {
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

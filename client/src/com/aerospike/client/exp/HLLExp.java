/*
 * Copyright 2012-2021 Aerospike, Inc.
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

import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.client.util.Pack;

/**
 * HyperLogLog (HLL) expression generator. See {@link com.aerospike.client.exp.Exp}.
 * <p>
 * The bin expression argument in these methods can be a reference to a bin or the
 * result of another expression. Expressions that modify bin values are only used
 * for temporary expression evaluation and are not permanently applied to the bin.
 * HLL modify expressions return the HLL bin's value.
 */
public final class HLLExp {
	private static final int MODULE = 2;
	private static final int INIT = 0;
	private static final int ADD = 1;
	private static final int COUNT = 50;
	private static final int UNION = 51;
	private static final int UNION_COUNT = 52;
	private static final int INTERSECT_COUNT = 53;
	private static final int SIMILARITY = 54;
	private static final int DESCRIBE = 55;
	private static final int MAY_CONTAIN = 56;

	/**
	 * Create expression that creates a new HLL or resets an existing HLL.
	 *
	 * @param policy			write policy, use {@link HLLPolicy#Default} for default
	 * @param indexBitCount		number of index bits. Must be between 4 and 16 inclusive.
	 * @param bin				HLL bin or value expression
	 */
	public static Exp init(HLLPolicy policy, Exp indexBitCount, Exp bin) {
		return init(policy, indexBitCount, Exp.val(-1), bin);
	}

	/**
	 * Create expression that creates a new HLL or resets an existing HLL with minhash bits.
	 *
	 * @param policy			write policy, use {@link HLLPolicy#Default} for default
	 * @param indexBitCount		number of index bits. Must be between 4 and 16 inclusive.
	 * @param minHashBitCount	number of min hash bits. Must be between 4 and 51 inclusive.
	 * @param bin				HLL bin or value expression
	 */
	public static Exp init(HLLPolicy policy, Exp indexBitCount, Exp minHashBitCount, Exp bin) {
		byte[] bytes = Pack.pack(INIT, indexBitCount, minHashBitCount, policy.flags);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that adds list values to a HLL set and returns HLL set.
	 * The function assumes HLL bin already exists.
	 *
	 * <pre>{@code
	 * // Add values to HLL bin "a" and check count > 7
	 * Exp.gt(
	 *   HLLExp.getCount(
	 *     HLLExp.add(HLLPolicy.Default, Exp.val(list), Exp.hllBin("a"))),
	 *   Exp.val(7))
	 * }</pre>
	 *
	 * @param policy	write policy, use {@link HLLPolicy#Default} for default
	 * @param list		list bin or value expression of values to be added
	 * @param bin		HLL bin or value expression
	 */
	public static Exp add(HLLPolicy policy, Exp list, Exp bin) {
		return add(policy, list, Exp.val(-1), Exp.val(-1), bin);
	}

	/**
	 * Create expression that adds values to a HLL set and returns HLL set.
	 * If HLL bin does not exist, use indexBitCount to create HLL bin.
	 *
	 * <pre>{@code
	 * // Add values to HLL bin "a" and check count > 7
	 * Exp.gt(
	 *   HLLExp.getCount(
	 *     HLLExp.add(HLLPolicy.Default, Exp.val(list), Exp.val(10), Exp.hllBin("a"))),
	 *   Exp.val(7))
	 * }</pre>
	 *
	 * @param policy			write policy, use {@link HLLPolicy#Default} for default
	 * @param list				list bin or value expression of values to be added
	 * @param indexBitCount		number of index bits expression. Must be between 4 and 16 inclusive.
	 * @param bin				HLL bin or value expression
	 */
	public static Exp add(HLLPolicy policy, Exp list, Exp indexBitCount, Exp bin) {
		return add(policy, list, indexBitCount, Exp.val(-1), bin);
	}

	/**
	 * Create expression that adds values to a HLL set and returns HLL set. If HLL bin does not
	 * exist, use indexBitCount and minHashBitCount to create HLL set.
	 *
	 * <pre>{@code
	 * // Add values to HLL bin "a" and check count > 7
	 * Exp.gt(
	 *   HLLExp.getCount(
	 *     HLLExp.add(HLLPolicy.Default, Exp.val(list), Exp.val(10), Exp.val(20), Exp.hllBin("a"))),
	 *   Exp.val(7))
	 * }</pre>
	 *
	 * @param policy			write policy, use {@link HLLPolicy#Default} for default
	 * @param list				list bin or value expression of values to be added
	 * @param indexBitCount		number of index bits expression. Must be between 4 and 16 inclusive.
	 * @param minHashBitCount   number of min hash bits expression. Must be between 4 and 51 inclusive.
	 * @param bin				HLL bin or value expression
	 */
	public static Exp add(HLLPolicy policy, Exp list, Exp indexBitCount, Exp minHashBitCount, Exp bin) {
		byte[] bytes = Pack.pack(ADD, list, indexBitCount, minHashBitCount, policy.flags);
		return addWrite(bin, bytes);
	}

	/**
	 * Create expression that returns estimated number of elements in the HLL bin.
	 *
	 * <pre>{@code
	 * // HLL bin "a" count > 7
	 * Exp.gt(HLLExp.getCount(Exp.hllBin("a")), Exp.val(7))
	 * }</pre>
	 */
	public static Exp getCount(Exp bin) {
		byte[] bytes = Pack.pack(COUNT);
		return addRead(bin, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns a HLL object that is the union of all specified HLL objects
	 * in the list with the HLL bin.
	 *
	 * <pre>{@code
	 * // Union of HLL bins "a" and "b"
	 * HLLExp.getUnion(Exp.hllBin("a"), Exp.hllBin("b"))
	 *
	 * // Union of local HLL list with bin "b"
	 * HLLExp.getUnion(Exp.val(list), Exp.hllBin("b"))
	 * }</pre>
	 */
	public static Exp getUnion(Exp list, Exp bin) {
		byte[] bytes = Pack.pack(UNION, list);
		return addRead(bin, bytes, Exp.Type.HLL);
	}

	/**
	 * Create expression that returns estimated number of elements that would be contained by
	 * the union of these HLL objects.
	 *
	 * <pre>{@code
	 * // Union count of HLL bins "a" and "b"
	 * HLLExp.getUnionCount(Exp.hllBin("a"), Exp.hllBin("b"))
	 *
	 * // Union count of local HLL list with bin "b"
	 * HLLExp.getUnionCount(Exp.val(list), Exp.hllBin("b"))
	 * }</pre>
	 */
	public static Exp getUnionCount(Exp list, Exp bin) {
		byte[] bytes = Pack.pack(UNION_COUNT, list);
		return addRead(bin, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns estimated number of elements that would be contained by
	 * the intersection of these HLL objects.
	 *
	 * <pre>{@code
	 * // Intersect count of HLL bins "a" and "b"
	 * HLLExp.getIntersectCount(Exp.hllBin("a"), Exp.hllBin("b"))
	 *
	 * // Intersect count of local HLL list with bin "b"
	 * HLLExp.getIntersectCount(Exp.val(list), Exp.hllBin("b"))
	 * }</pre>
	 */
	public static Exp getIntersectCount(Exp list, Exp bin) {
		byte[] bytes = Pack.pack(INTERSECT_COUNT, list);
		return addRead(bin, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns estimated similarity of these HLL objects as a
	 * 64 bit float.
	 *
	 * <pre>{@code
	 * // Similarity of HLL bins "a" and "b" >= 0.75
	 * Exp.ge(HLLExp.getSimilarity(Exp.hllBin("a"), Exp.hllBin("b")), Exp.val(0.75))
	 * }</pre>
	 */
	public static Exp getSimilarity(Exp list, Exp bin) {
		byte[] bytes = Pack.pack(SIMILARITY, list);
		return addRead(bin, bytes, Exp.Type.FLOAT);
	}

	/**
	 * Create expression that returns indexBitCount and minHashBitCount used to create HLL bin
	 * in a list of longs. list[0] is indexBitCount and list[1] is minHashBitCount.
	 *
	 * <pre>{@code
	 * // Bin "a" indexBitCount < 10
	 * Exp.lt(
	 *   ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.val(0),
	 *     HLLExp.describe(Exp.hllBin("a"))),
	 *   Exp.val(10))
	 * }</pre>
	 */
	public static Exp describe(Exp bin) {
		byte[] bytes = Pack.pack(DESCRIBE);
		return addRead(bin, bytes, Exp.Type.LIST);
	}

	/**
	 * Create expression that returns one if HLL bin may contain all items in the list.
	 *
	 * <pre>{@code
	 * // Bin "a" may contain value "x"
	 * ArrayList<Value> list = new ArrayList<Value>();
	 * list.add(Value.get("x"));
	 * Exp.eq(HLLExp.mayContain(Exp.val(list), Exp.hllBin("a")), Exp.val(1));
	 * }</pre>
	 */
	public static Exp mayContain(Exp list, Exp bin) {
		byte[] bytes = Pack.pack(MAY_CONTAIN, list);
		return addRead(bin, bytes, Exp.Type.INT);
	}

	private static Exp addWrite(Exp bin, byte[] bytes) {
		return new Exp.Module(bin, bytes, Exp.Type.HLL.code, MODULE | Exp.MODIFY);
	}

	private static Exp addRead(Exp bin, byte[] bytes, Exp.Type retType) {
		return new Exp.Module(bin, bytes, retType.code, MODULE);
	}
}

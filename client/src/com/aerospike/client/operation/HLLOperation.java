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
package com.aerospike.client.operation;

import java.util.List;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.Value.HLLValue;
import com.aerospike.client.util.Pack;

/**
 * HyperLogLog (HLL) operations.
 * <p>
 * HyperLogLog operations on HLL items nested in lists/maps are not currently
 * supported by the server.
 */
public final class HLLOperation {
	private static final int INIT = 0;
	private static final int ADD = 1;
	private static final int SET_UNION = 2;
	private static final int SET_COUNT = 3;
	private static final int FOLD = 4;
	private static final int COUNT = 50;
	private static final int UNION = 51;
	private static final int UNION_COUNT = 52;
	private static final int INTERSECT_COUNT = 53;
	private static final int SIMILARITY = 54;
	private static final int DESCRIBE = 55;

	/**
	 * Create HLL init operation.
	 * Server creates a new HLL or resets an existing HLL.
	 * Server does not return a value.
	 *
	 * @param policy			write policy, use {@link HLLPolicy#Default} for default
	 * @param binName			name of bin
	 * @param indexBitCount		number of index bits. Must be between 4 and 16 inclusive.
	 */
	public static Operation init(HLLPolicy policy, String binName, int indexBitCount) {
		return init(policy, binName, indexBitCount, -1);
	}

	/**
	 * Create HLL init operation with minhash bits.
	 * Server creates a new HLL or resets an existing HLL.
	 * Server does not return a value.
	 *
	 * @param policy			write policy, use {@link HLLPolicy#Default} for default
	 * @param binName			name of bin
	 * @param indexBitCount		number of index bits. Must be between 4 and 16 inclusive.
	 * @param minHashBitCount   number of min hash bits. Must be between 4 and 51 inclusive.
	 * 							Also, indexBitCount + minHashBitCount must be <= 64.
	 */
	public static Operation init(HLLPolicy policy, String binName, int indexBitCount, int minHashBitCount) {
		byte[] bytes = Pack.pack(HLLOperation.INIT, indexBitCount, minHashBitCount, policy.flags);
		return new Operation(Operation.Type.HLL_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create HLL add operation. This operation assumes HLL bin already exists.
	 * Server adds values to the HLL set.
	 * Server returns number of entries that caused HLL to update a register.
	 *
	 * @param policy			write policy, use {@link HLLPolicy#Default} for default
	 * @param binName			name of bin
	 * @param list				list of values to be added
	 */
	public static Operation add(HLLPolicy policy, String binName, List<Value> list) {
		return add(policy, binName, list, -1, -1);
	}

	/**
	 * Create HLL add operation.
	 * Server adds values to HLL set. If HLL bin does not exist, use indexBitCount to create HLL bin.
	 * Server returns number of entries that caused HLL to update a register.
	 *
	 * @param policy			write policy, use {@link HLLPolicy#Default} for default
	 * @param binName			name of bin
	 * @param list				list of values to be added
	 * @param indexBitCount		number of index bits. Must be between 4 and 16 inclusive.
	 */
	public static Operation add(HLLPolicy policy, String binName, List<Value> list, int indexBitCount) {
		return add(policy, binName, list, indexBitCount, -1);
	}

	/**
	 * Create HLL add operation with minhash bits.
	 * Server adds values to HLL set. If HLL bin does not exist, use indexBitCount and minHashBitCount
	 * to create HLL bin. Server returns number of entries that caused HLL to update a register.
	 *
	 * @param policy			write policy, use {@link HLLPolicy#Default} for default
	 * @param binName			name of bin
	 * @param list				list of values to be added
	 * @param indexBitCount		number of index bits. Must be between 4 and 16 inclusive.
	 * @param minHashBitCount   number of min hash bits. Must be between 4 and 51 inclusive.
	 * 							Also, indexBitCount + minHashBitCount must be <= 64.
	 */
	public static Operation add(HLLPolicy policy, String binName, List<Value> list, int indexBitCount, int minHashBitCount) {
		byte[] bytes = Pack.pack(HLLOperation.ADD, list, indexBitCount, minHashBitCount, policy.flags);
		return new Operation(Operation.Type.HLL_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create HLL set union operation.
	 * Server sets union of specified HLL objects with HLL bin.
	 * Server does not return a value.
	 *
	 * @param policy			write policy, use {@link HLLPolicy#Default} for default
	 * @param binName			name of bin
	 * @param list				list of HLL objects
	 */
	public static Operation setUnion(HLLPolicy policy, String binName, List<HLLValue> list) {
		byte[] bytes = Pack.pack(HLLOperation.SET_UNION, list, policy.flags);
		return new Operation(Operation.Type.HLL_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create HLL refresh operation.
	 * Server updates the cached count (if stale) and returns the count.
	 *
	 * @param binName			name of bin
	 */
	public static Operation refreshCount(String binName) {
		byte[] bytes = Pack.pack(HLLOperation.SET_COUNT);
		return new Operation(Operation.Type.HLL_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create HLL fold operation.
	 * Servers folds indexBitCount to the specified value.
	 * This can only be applied when minHashBitCount on the HLL bin is 0.
	 * Server does not return a value.
	 *
	 * @param binName			name of bin
	 * @param indexBitCount		number of index bits. Must be between 4 and 16 inclusive.
	 */
	public static Operation fold(String binName, int indexBitCount) {
		byte[] bytes = Pack.pack(HLLOperation.FOLD, indexBitCount);
		return new Operation(Operation.Type.HLL_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create HLL getCount operation.
	 * Server returns estimated number of elements in the HLL bin.
	 *
	 * @param binName			name of bin
	 */
	public static Operation getCount(String binName) {
		byte[] bytes = Pack.pack(HLLOperation.COUNT);
		return new Operation(Operation.Type.HLL_READ, binName, Value.get(bytes));
	}

	/**
	 * Create HLL getUnion operation.
	 * Server returns an HLL object that is the union of all specified HLL objects in the list
	 * with the HLL bin.
	 *
	 * @param binName			name of bin
	 * @param list				list of HLL objects
	 */
	public static Operation getUnion(String binName, List<HLLValue> list) {
		byte[] bytes = Pack.pack(HLLOperation.UNION, list);
		return new Operation(Operation.Type.HLL_READ, binName, Value.get(bytes));
	}

	/**
	 * Create HLL getUnionCount operation.
	 * Server returns estimated number of elements that would be contained by the union of these
	 * HLL objects.
	 *
	 * @param binName			name of bin
	 * @param list				list of HLL objects
	 */
	public static Operation getUnionCount(String binName, List<HLLValue> list) {
		byte[] bytes = Pack.pack(HLLOperation.UNION_COUNT, list);
		return new Operation(Operation.Type.HLL_READ, binName, Value.get(bytes));
	}

	/**
	 * Create HLL getIntersectCount operation.
	 * Bin contains an HLL value. 
	 * List of HLL objects may have maximum 2 objects if minhash bits are 0, can have >2 objects otherwise.
	 * Server returns estimated number of elements that would be contained by the intersection of
	 * these HLL objects.
	 *
	 * @param binName			name of bin
	 * @param list				list of HLL objects
	 */
	public static Operation getIntersectCount(String binName, List<HLLValue> list) {
		byte[] bytes = Pack.pack(HLLOperation.INTERSECT_COUNT, list);
		return new Operation(Operation.Type.HLL_READ, binName, Value.get(bytes));
	}

	/**
	 * Create HLL getSimilarity operation.
	 * Bin contains an HLL value. 
	 * List of HLL objects may have maximum 2 objects if minhash bits are 0, can have >2 objects otherwise.
	 * Server returns estimated similarity of these HLL objects. Return type is a double.
	 *
	 * @param binName			name of bin
	 * @param list				list of HLL objects
	 */
	public static Operation getSimilarity(String binName, List<HLLValue> list) {
		byte[] bytes = Pack.pack(HLLOperation.SIMILARITY, list);
		return new Operation(Operation.Type.HLL_READ, binName, Value.get(bytes));
	}

	/**
	 * Create HLL describe operation.
	 * Server returns indexBitCount and minHashBitCount used to create HLL bin in a list of longs.
	 * The list size is 2.
	 *
	 * @param binName			name of bin
	 */
	public static Operation describe(String binName) {
		byte[] bytes = Pack.pack(HLLOperation.DESCRIBE);
		return new Operation(Operation.Type.HLL_READ, binName, Value.get(bytes));
	}
}

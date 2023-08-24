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
package com.aerospike.client.exp;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.util.Pack;
import com.aerospike.client.util.Packer;

/**
 * Map expression generator. See {@link com.aerospike.client.exp.Exp}.
 * <p>
 * The bin expression argument in these methods can be a reference to a bin or the
 * result of another expression. Expressions that modify bin values are only used
 * for temporary expression evaluation and are not permanently applied to the bin.
 * <p>
 * Map modify expressions return the bin's value. This value will be a map except
 * when the map is nested within a list. In that case, a list is returned for the
 * map modify expression.
 * <p>
 * Valid map key types are:
 * <ul>
 * <li>String</li>
 * <li>Integer</li>
 * <li>byte[]</li>
 * <li>List</li>
 * </ul>
 * <p>
 * All maps maintain an index and a rank.  The index is the item offset from the start of the map,
 * for both unordered and ordered maps.  The rank is the sorted index of the value component.
 * Map supports negative indexing for index and rank.
 * <p>
 * Index examples:
 * <ul>
 * <li>Index 0: First item in map.</li>
 * <li>Index 4: Fifth item in map.</li>
 * <li>Index -1: Last item in map.</li>
 * <li>Index -3: Third to last item in map.</li>
 * <li>Index 1 Count 2: Second and third items in map.</li>
 * <li>Index -3 Count 3: Last three items in map.</li>
 * <li>Index -5 Count 4: Range between fifth to last item to second to last item inclusive.</li>
 * </ul>
 * <p>
 * Rank examples:
 * <ul>
 * <li>Rank 0: Item with lowest value rank in map.</li>
 * <li>Rank 4: Fifth lowest ranked item in map.</li>
 * <li>Rank -1: Item with highest ranked value in map.</li>
 * <li>Rank -3: Item with third highest ranked value in map.</li>
 * <li>Rank 1 Count 2: Second and third lowest ranked items in map.</li>
 * <li>Rank -3 Count 3: Top three ranked items in map.</li>
 * </ul>
 * <p>
 * Nested expressions are supported by optional CTX context arguments.  Example:
 * <ul>
 * <li>bin = {key1={key11=9,key12=4}, key2={key21=3,key22=5}}</li>
 * <li>Set map value to 11 for map key "key21" inside of map key "key2".</li>
 * <li>Get size of map key2.</li>
 * <li>MapExp.size(Exp.mapBin("bin"), CTX.mapKey(Value.get("key2"))</li>
 * <li>result = 2</li>
 * </ul>
 */
public final class MapExp {
	private static final int MODULE = 0;
	private static final int PUT = 67;
	private static final int PUT_ITEMS = 68;
	private static final int REPLACE = 69;
	private static final int REPLACE_ITEMS = 70;
	private static final int INCREMENT = 73;
	private static final int CLEAR = 75;
	private static final int REMOVE_BY_KEY = 76;
	private static final int REMOVE_BY_INDEX = 77;
	private static final int REMOVE_BY_RANK = 79;
	private static final int REMOVE_BY_KEY_LIST = 81;
	private static final int REMOVE_BY_VALUE = 82;
	private static final int REMOVE_BY_VALUE_LIST = 83;
	private static final int REMOVE_BY_KEY_INTERVAL = 84;
	private static final int REMOVE_BY_INDEX_RANGE = 85;
	private static final int REMOVE_BY_VALUE_INTERVAL = 86;
	private static final int REMOVE_BY_RANK_RANGE = 87;
	private static final int REMOVE_BY_KEY_REL_INDEX_RANGE = 88;
	private static final int REMOVE_BY_VALUE_REL_RANK_RANGE = 89;
	private static final int SIZE = 96;
	private static final int GET_BY_KEY = 97;
	private static final int GET_BY_INDEX = 98;
	private static final int GET_BY_RANK = 100;
	private static final int GET_BY_VALUE = 102;  // GET_ALL_BY_VALUE on server.
	private static final int GET_BY_KEY_INTERVAL = 103;
	private static final int GET_BY_INDEX_RANGE = 104;
	private static final int GET_BY_VALUE_INTERVAL = 105;
	private static final int GET_BY_RANK_RANGE = 106;
	private static final int GET_BY_KEY_LIST = 107;
	private static final int GET_BY_VALUE_LIST = 108;
	private static final int GET_BY_KEY_REL_INDEX_RANGE = 109;
	private static final int GET_BY_VALUE_REL_RANK_RANGE = 110;

	/**
	 * Create expression that writes key/value item to a map bin. The 'bin' expression should either
	 * reference an existing map bin or be a expression that returns a map.
	 *
	 * <pre>{@code
	 * // Add entry{11,22} to existing map bin.
	 * Expression e = Exp.build(MapExp.put(MapPolicy.Default, Exp.val(11), Exp.val(22), Exp.mapBin(binName)));
	 * client.operate(null, key, ExpOperation.write(binName, e, ExpWriteFlags.DEFAULT));
	 *
	 * // Combine entry{11,22} with source map's first index entry and write resulting map to target map bin.
	 * Expression e = Exp.build(
	 *   MapExp.put(MapPolicy.Default, Exp.val(11), Exp.val(22),
	 *     MapExp.getByIndexRange(MapReturnType.KEY_VALUE, Exp.val(0), Exp.val(1), Exp.mapBin(sourceBinName)))
	 *   );
	 * client.operate(null, key, ExpOperation.write(targetBinName, e, ExpWriteFlags.DEFAULT));
	 * }</pre>
	 */
	public static Exp put(MapPolicy policy, Exp key, Exp value, Exp bin, CTX... ctx) {
		Packer packer = new Packer();

		if (policy.flags != 0) {
			Pack.init(packer, ctx);
			packer.packArrayBegin(5);
			packer.packInt(PUT);
			key.pack(packer);
			value.pack(packer);
			packer.packInt(policy.attributes);
			packer.packInt(policy.flags);
		}
		else {
			if (policy.itemCommand == REPLACE) {
				// Replace doesn't allow map attributes because it does not create on non-existing key.
				Pack.init(packer, ctx);
				packer.packArrayBegin(3);
				packer.packInt(policy.itemCommand);
				key.pack(packer);
				value.pack(packer);
			}
			else {
				Pack.init(packer, ctx);
				packer.packArrayBegin(4);
				packer.packInt(policy.itemCommand);
				key.pack(packer);
				value.pack(packer);
				packer.packInt(policy.attributes);
			}
		}
		byte[] bytes = packer.toByteArray();
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that writes each map item to a map bin.
	 */
	public static Exp putItems(MapPolicy policy, Exp map, Exp bin, CTX... ctx) {
		Packer packer = new Packer();

		if (policy.flags != 0) {
			Pack.init(packer, ctx);
			packer.packArrayBegin(4);
			packer.packInt(PUT_ITEMS);
			map.pack(packer);
			packer.packInt(policy.attributes);
			packer.packInt(policy.flags);
		}
		else {
			if (policy.itemsCommand == REPLACE_ITEMS) {
				// Replace doesn't allow map attributes because it does not create on non-existing key.
				Pack.init(packer, ctx);
				packer.packArrayBegin(2);
				packer.packInt(policy.itemsCommand);
				map.pack(packer);
			}
			else {
				Pack.init(packer, ctx);
				packer.packArrayBegin(3);
				packer.packInt(policy.itemsCommand);
				map.pack(packer);
				packer.packInt(policy.attributes);
			}
		}
		byte[] bytes = packer.toByteArray();
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that increments values by incr for all items identified by key.
	 * Valid only for numbers.
	 */
	public static Exp increment(MapPolicy policy, Exp key, Exp incr, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(INCREMENT, key, incr, policy.attributes, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes all items in map.
	 */
	public static Exp clear(Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(CLEAR, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map item identified by key.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 */
	public static Exp removeByKey(int returnType, Exp key, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_KEY, returnType, key, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items identified by keys.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 */
	public static Exp removeByKeyList(int returnType, Exp keys, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_KEY_LIST, returnType, keys, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items identified by key range (keyBegin inclusive, keyEnd exclusive).
	 * If keyBegin is null, the range is less than keyEnd.
	 * If keyEnd is null, the range is greater than equal to keyBegin.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 */
	public static Exp removeByKeyRange(int returnType, Exp keyBegin, Exp keyEnd, Exp bin, CTX... ctx) {
		byte[] bytes = ListExp.packRangeOperation(REMOVE_BY_KEY_INTERVAL, returnType, keyBegin, keyEnd, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items nearest to key and greater by index.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 * <p>
	 * Examples for map [{0=17},{4=2},{5=15},{9=10}]:
	 * <ul>
	 * <li>(value,index) = [removed items]</li>
	 * <li>(5,0) = [{5=15},{9=10}]</li>
	 * <li>(5,1) = [{9=10}]</li>
	 * <li>(5,-1) = [{4=2},{5=15},{9=10}]</li>
	 * <li>(3,2) = [{9=10}]</li>
	 * <li>(3,-2) = [{0=17},{4=2},{5=15},{9=10}]</li>
	 * </ul>
	 */
	public static Exp removeByKeyRelativeIndexRange(int returnType, Exp key, Exp index, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_KEY_REL_INDEX_RANGE, returnType, key, index, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items nearest to key and greater by index with a count limit.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 * <p>
	 * Examples for map [{0=17},{4=2},{5=15},{9=10}]:
	 * <ul>
	 * <li>(value,index,count) = [removed items]</li>
	 * <li>(5,0,1) = [{5=15}]</li>
	 * <li>(5,1,2) = [{9=10}]</li>
	 * <li>(5,-1,1) = [{4=2}]</li>
	 * <li>(3,2,1) = [{9=10}]</li>
	 * <li>(3,-2,2) = [{0=17}]</li>
	 * </ul>
	 */
	public static Exp removeByKeyRelativeIndexRange(int returnType, Exp key, Exp index, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_KEY_REL_INDEX_RANGE, returnType, key, index, count, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items identified by value.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 */
	public static Exp removeByValue(int returnType, Exp value, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_VALUE, returnType, value, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items identified by values.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 */
	public static Exp removeByValueList(int returnType, Exp values, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_VALUE_LIST, returnType, values, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items identified by value range (valueBegin inclusive, valueEnd exclusive).
	 * If valueBegin is null, the range is less than valueEnd.
	 * If valueEnd is null, the range is greater than equal to valueBegin.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 */
	public static Exp removeByValueRange(int returnType, Exp valueBegin, Exp valueEnd, Exp bin, CTX... ctx) {
		byte[] bytes = ListExp.packRangeOperation(REMOVE_BY_VALUE_INTERVAL, returnType, valueBegin, valueEnd, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items nearest to value and greater by relative rank.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 * <p>
	 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
	 * <ul>
	 * <li>(value,rank) = [removed items]</li>
	 * <li>(11,1) = [{0=17}]</li>
	 * <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
	 * </ul>
	 */
	public static Exp removeByValueRelativeRankRange(int returnType, Exp value, Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_VALUE_REL_RANK_RANGE, returnType, value, rank, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items nearest to value and greater by relative rank with a count limit.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 * <p>
	 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
	 * <ul>
	 * <li>(value,rank,count) = [removed items]</li>
	 * <li>(11,1,1) = [{0=17}]</li>
	 * <li>(11,-1,1) = [{9=10}]</li>
	 * </ul>
	 */
	public static Exp removeByValueRelativeRankRange(int returnType, Exp value, Exp rank, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_VALUE_REL_RANK_RANGE, returnType, value, rank, count, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map item identified by index.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 */
	public static Exp removeByIndex(int returnType, Exp index, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_INDEX, returnType, index, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items starting at specified index to the end of map.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 */
	public static Exp removeByIndexRange(int returnType, Exp index, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_INDEX_RANGE, returnType, index, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes "count" map items starting at specified index.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 */
	public static Exp removeByIndexRange(int returnType, Exp index, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_INDEX_RANGE, returnType, index, count, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map item identified by rank.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 */
	public static Exp removeByRank(int returnType, Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_RANK, returnType, rank, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items starting at specified rank to the last ranked item.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 */
	public static Exp removeByRankRange(int returnType, Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_RANK_RANGE, returnType, rank, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes "count" map items starting at specified rank.
	 * Valid returnType values are {@link com.aerospike.client.cdt.MapReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.MapReturnType#INVERTED}.
	 */
	public static Exp removeByRankRange(int returnType, Exp rank, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_RANK_RANGE, returnType, rank, count, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that returns list size.
	 *
	 * <pre>{@code
	 * // Map bin "a" size > 7
	 * Exp.gt(MapExp.size(Exp.mapBin("a")), Exp.val(7))
	 * }</pre>
	 */
	public static Exp size(Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(SIZE, ctx);
		return addRead(bin, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that selects map item identified by key and returns selected data
	 * specified by returnType.
	 *
	 * <pre>{@code
	 * // Map bin "a" contains key "B"
	 * Exp.gt(
	 *   MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.val("B"), Exp.mapBin("a")),
	 *   Exp.val(0));
	 * }</pre>
	 *
	 * @param returnType	metadata attributes to return. See {@link MapReturnType}
	 * @param valueType		expected type of return value
	 * @param key			map key expression
	 * @param bin			bin or map value expression
	 * @param ctx			optional context path for nested CDT
	 */
	public static Exp getByKey(int returnType, Exp.Type valueType, Exp key, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_KEY, returnType, key, ctx);
		return addRead(bin, bytes, valueType);
	}

	/**
	 * Create expression that selects map items identified by key range (keyBegin inclusive, keyEnd exclusive).
	 * If keyBegin is null, the range is less than keyEnd.
	 * If keyEnd is null, the range is greater than equal to keyBegin.
	 * <p>
	 * Expression returns selected data specified by returnType (See {@link MapReturnType}).
	 */
	public static Exp getByKeyRange(int returnType, Exp keyBegin, Exp keyEnd, Exp bin, CTX... ctx) {
		byte[] bytes = ListExp.packRangeOperation(GET_BY_KEY_INTERVAL, returnType, keyBegin, keyEnd, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects map items identified by keys and returns selected data specified by
	 * returnType (See {@link MapReturnType}).
	 */
	public static Exp getByKeyList(int returnType, Exp keys, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_KEY_LIST, returnType, keys, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects map items nearest to key and greater by index.
	 * Expression returns selected data specified by returnType (See {@link MapReturnType}).
	 * <p>
	 * Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
	 * <ul>
	 * <li>(value,index) = [selected items]</li>
	 * <li>(5,0) = [{5=15},{9=10}]</li>
	 * <li>(5,1) = [{9=10}]</li>
	 * <li>(5,-1) = [{4=2},{5=15},{9=10}]</li>
	 * <li>(3,2) = [{9=10}]</li>
	 * <li>(3,-2) = [{0=17},{4=2},{5=15},{9=10}]</li>
	 * </ul>
	 */
	public static Exp getByKeyRelativeIndexRange(int returnType, Exp key, Exp index, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_KEY_REL_INDEX_RANGE, returnType, key, index, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects map items nearest to key and greater by index with a count limit.
	 * Expression returns selected data specified by returnType (See {@link MapReturnType}).
	 * <p>
	 * Examples for ordered map [{0=17},{4=2},{5=15},{9=10}]:
	 * <ul>
	 * <li>(value,index,count) = [selected items]</li>
	 * <li>(5,0,1) = [{5=15}]</li>
	 * <li>(5,1,2) = [{9=10}]</li>
	 * <li>(5,-1,1) = [{4=2}]</li>
	 * <li>(3,2,1) = [{9=10}]</li>
	 * <li>(3,-2,2) = [{0=17}]</li>
	 * </ul>
	 */
	public static Exp getByKeyRelativeIndexRange(int returnType, Exp key, Exp index, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_KEY_REL_INDEX_RANGE, returnType, key, index, count, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects map items identified by value and returns selected data
	 * specified by returnType.
	 *
	 * <pre>{@code
	 * // Map bin "a" contains value "BBB"
	 * Exp.gt(
	 *   MapExp.getByValue(MapReturnType.COUNT, Exp.val("BBB"), Exp.mapBin("a")),
	 *   Exp.val(0))
	 * }</pre>
	 *
	 * @param returnType	metadata attributes to return. See {@link MapReturnType}
	 * @param value			value expression
	 * @param bin			bin or map value expression
	 * @param ctx			optional context path for nested CDT
	 */
	public static Exp getByValue(int returnType, Exp value, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_VALUE, returnType, value, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects map items identified by value range (valueBegin inclusive, valueEnd exclusive)
	 * If valueBegin is null, the range is less than valueEnd.
	 * If valueEnd is null, the range is greater than equal to valueBegin.
	 * <p>
	 * Expression returns selected data specified by returnType (See {@link MapReturnType}).
	 */
	public static Exp getByValueRange(int returnType, Exp valueBegin, Exp valueEnd, Exp bin, CTX... ctx) {
		byte[] bytes = ListExp.packRangeOperation(GET_BY_VALUE_INTERVAL, returnType, valueBegin, valueEnd, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects map items identified by values and returns selected data specified by
	 * returnType (See {@link MapReturnType}).
	 */
	public static Exp getByValueList(int returnType, Exp values, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_VALUE_LIST, returnType, values, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects map items nearest to value and greater by relative rank.
	 * Expression returns selected data specified by returnType (See {@link MapReturnType}).
	 * <p>
	 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
	 * <ul>
	 * <li>(value,rank) = [selected items]</li>
	 * <li>(11,1) = [{0=17}]</li>
	 * <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
	 * </ul>
	 */
	public static Exp getByValueRelativeRankRange(int returnType, Exp value, Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_VALUE_REL_RANK_RANGE, returnType, value, rank, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects map items nearest to value and greater by relative rank with a count limit.
	 * Expression returns selected data specified by returnType (See {@link MapReturnType}).
	 * <p>
	 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
	 * <ul>
	 * <li>(value,rank,count) = [selected items]</li>
	 * <li>(11,1,1) = [{0=17}]</li>
	 * <li>(11,-1,1) = [{9=10}]</li>
	 * </ul>
	 */
	public static Exp getByValueRelativeRankRange(int returnType, Exp value, Exp rank, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_VALUE_REL_RANK_RANGE, returnType, value, rank, count, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects map item identified by index and returns selected data specified by
	 * returnType (See {@link MapReturnType}).
	 */
	public static Exp getByIndex(int returnType, Exp.Type valueType, Exp index, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_INDEX, returnType, index, ctx);
		return addRead(bin, bytes, valueType);
	}

	/**
	 * Create expression that selects map items starting at specified index to the end of map and returns selected
	 * data specified by returnType (See {@link MapReturnType}).
	 */
	public static Exp getByIndexRange(int returnType, Exp index, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_INDEX_RANGE, returnType, index, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects "count" map items starting at specified index and returns selected data
	 * specified by returnType (See {@link MapReturnType}).
	 */
	public static Exp getByIndexRange(int returnType, Exp index, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_INDEX_RANGE, returnType, index, count, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects map item identified by rank and returns selected data specified by
	 * returnType (See {@link MapReturnType}).
	 */
	public static Exp getByRank(int returnType, Exp.Type valueType, Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_RANK, returnType, rank, ctx);
		return addRead(bin, bytes, valueType);
	}

	/**
	 * Create expression that selects map items starting at specified rank to the last ranked item and
	 * returns selected data specified by returnType (See {@link MapReturnType}).
	 */
	public static Exp getByRankRange(int returnType, Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_RANK_RANGE, returnType, rank, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects "count" map items starting at specified rank and returns selected
	 * data specified by returnType (See {@link MapReturnType}).
	 */
	public static Exp getByRankRange(int returnType, Exp rank, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_RANK_RANGE, returnType, rank, count, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	private static Exp addWrite(Exp bin, byte[] bytes, CTX[] ctx) {
		int retType;

		if (ctx == null || ctx.length == 0) {
			retType = Exp.Type.MAP.code;
		}
		else {
			retType = ((ctx[0].id & 0x10) == 0)? Exp.Type.MAP.code : Exp.Type.LIST.code;
		}
		return new Exp.Module(bin, bytes, retType, MODULE | Exp.MODIFY);
	}

	private static Exp addRead(Exp bin, byte[] bytes, Exp.Type retType) {
		return new Exp.Module(bin, bytes, retType.code, MODULE);
	}

	private static Exp.Type getValueType(int returnType) {
		int t = returnType & ~MapReturnType.INVERTED;

		switch (t) {
		case MapReturnType.INDEX:
		case MapReturnType.REVERSE_INDEX:
		case MapReturnType.RANK:
		case MapReturnType.REVERSE_RANK:
			// This method only called from expressions that can return multiple integers (ie list).
			return Exp.Type.LIST;

		case MapReturnType.COUNT:
			return Exp.Type.INT;

		case MapReturnType.KEY:
		case MapReturnType.VALUE:
			// This method only called from expressions that can return multiple objects (ie list).
			return Exp.Type.LIST;

		case MapReturnType.KEY_VALUE:
		case MapReturnType.ORDERED_MAP:
		case MapReturnType.UNORDERED_MAP:
			return Exp.Type.MAP;

		case MapReturnType.EXISTS:
			return Exp.Type.BOOL;

		default:
		case MapReturnType.NONE:
			throw new AerospikeException("Invalid MapReturnType: " + returnType);
		}
	}
}

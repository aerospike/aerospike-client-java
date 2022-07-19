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
	 * Create expression that writes key/value item to map bin.
	 * 
	 * Helpful note, the 'bin' field should be instantiated using a map. The most obvious way to do this 
	 * is using Exp.mapBin which sources an existing map from a bin
	 * 
	 * e.g. MapExp.put(new MapPolicy(), Exp.val(MAP_KEY), <some expression which is the required map value>, Exp.mapBin(BIN_NAME));
	 * 
	 * alternatively Exp.mapBin might be replaced by a literal Exp.val(Map<?,?> map)
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
	 * Create expression that writes each map item to map bin.
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
	 */
	public static Exp removeByKey(Exp key, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_KEY, MapReturnType.NONE, key, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items identified by keys.
	 */
	public static Exp removeByKeyList(Exp keys, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_KEY_LIST, MapReturnType.NONE, keys, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items identified by key range (keyBegin inclusive, keyEnd exclusive).
	 * If keyBegin is null, the range is less than keyEnd.
	 * If keyEnd is null, the range is greater than equal to keyBegin.
	 */
	public static Exp removeByKeyRange(Exp keyBegin, Exp keyEnd, Exp bin, CTX... ctx) {
		byte[] bytes = ListExp.packRangeOperation(REMOVE_BY_KEY_INTERVAL, MapReturnType.NONE, keyBegin, keyEnd, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items nearest to key and greater by index.
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
	public static Exp removeByKeyRelativeIndexRange(Exp key, Exp index, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_KEY_REL_INDEX_RANGE, MapReturnType.NONE, key, index, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items nearest to key and greater by index with a count limit.
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
	public static Exp removeByKeyRelativeIndexRange(Exp key, Exp index, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_KEY_REL_INDEX_RANGE, MapReturnType.NONE, key, index, count, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items identified by value.
	 */
	public static Exp removeByValue(Exp value, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_VALUE, MapReturnType.NONE, value, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items identified by values.
	 */
	public static Exp removeByValueList(Exp values, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_VALUE_LIST, MapReturnType.NONE, values, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items identified by value range (valueBegin inclusive, valueEnd exclusive).
	 * If valueBegin is null, the range is less than valueEnd.
	 * If valueEnd is null, the range is greater than equal to valueBegin.
	 */
	public static Exp removeByValueRange(Exp valueBegin, Exp valueEnd, Exp bin, CTX... ctx) {
		byte[] bytes = ListExp.packRangeOperation(REMOVE_BY_VALUE_INTERVAL, MapReturnType.NONE, valueBegin, valueEnd, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items nearest to value and greater by relative rank.
	 * <p>
	 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
	 * <ul>
	 * <li>(value,rank) = [removed items]</li>
	 * <li>(11,1) = [{0=17}]</li>
	 * <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
	 * </ul>
	 */
	public static Exp removeByValueRelativeRankRange(Exp value, Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_VALUE_REL_RANK_RANGE, MapReturnType.NONE, value, rank, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items nearest to value and greater by relative rank with a count limit.
	 * <p>
	 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
	 * <ul>
	 * <li>(value,rank,count) = [removed items]</li>
	 * <li>(11,1,1) = [{0=17}]</li>
	 * <li>(11,-1,1) = [{9=10}]</li>
	 * </ul>
	 */
	public static Exp removeByValueRelativeRankRange(Exp value, Exp rank, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_VALUE_REL_RANK_RANGE, MapReturnType.NONE, value, rank, count, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map item identified by index.
	 */
	public static Exp removeByIndex(Exp index, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_INDEX, MapReturnType.NONE, index, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items starting at specified index to the end of map.
	 */
	public static Exp removeByIndexRange(Exp index, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_INDEX_RANGE, MapReturnType.NONE, index, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes "count" map items starting at specified index.
	 */
	public static Exp removeByIndexRange(Exp index, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_INDEX_RANGE, MapReturnType.NONE, index, count, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map item identified by rank.
	 */
	public static Exp removeByRank(Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_RANK, MapReturnType.NONE, rank, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes map items starting at specified rank to the last ranked item.
	 */
	public static Exp removeByRankRange(Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_RANK_RANGE, MapReturnType.NONE, rank, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes "count" map items starting at specified rank.
	 */
	public static Exp removeByRankRange(Exp rank, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_RANK_RANGE, MapReturnType.NONE, rank, count, ctx);
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

		if (t <= MapReturnType.COUNT) {
			return Exp.Type.INT;
		}

		if (t == MapReturnType.KEY_VALUE) {
			return Exp.Type.MAP;
		}
		return Exp.Type.LIST;
	}
}

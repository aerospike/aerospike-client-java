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

import java.util.List;
import java.util.Map;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.util.Packer;

/**
 * Unique key map bin operations. Create map operations used by the client operate command.
 * The default unique key map is unordered.
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
 * Nested CDT operations are supported by optional CTX context arguments.  Examples:
 * <ul>
 * <li>bin = {key1={key11=9,key12=4}, key2={key21=3,key22=5}}</li>
 * <li>Set map value to 11 for map key "key21" inside of map key "key2".</li>
 * <li>MapOperation.put(MapPolicy.Default, "bin", Value.get("key21"), Value.get(11), CTX.mapKey(Value.get("key2")))</li>
 * <li>bin result = {key1={key11=9,key12=4},key2={key21=11,key22=5}}</li>
 * <li></li>
 * <li>bin = {key1={key11={key111=1},key12={key121=5}}, key2={key21={"key211",7}}}</li>
 * <li>Set map value to 11 in map key "key121" for highest ranked map ("key12") inside of map key "key1".</li>
 * <li>MapOperation.put(MapPolicy.Default, "bin", Value.get("key121"), Value.get(11), CTX.mapKey(Value.get("key1")), CTX.mapRank(-1))</li>
 * <li>bin result = {key1={key11={key111=1},key12={key121=11}}, key2={key21={"key211",7}}}</li>
 * </ul>
 */
public class MapOperation {
	private static final int SET_TYPE = 64;
	protected static final int ADD = 65;
	protected static final int ADD_ITEMS = 66;
	protected static final int PUT = 67;
	protected static final int PUT_ITEMS = 68;
	protected static final int REPLACE = 69;
	protected static final int REPLACE_ITEMS = 70;
	private static final int INCREMENT = 73;
	private static final int DECREMENT = 74;
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
	 * Create map create operation.
	 * Server creates map at given context level.
	 */
	public static Operation create(String binName, MapOrder order, CTX... ctx) {
		// If context not defined, the set order for top-level bin map.
		if (ctx == null || ctx.length == 0) {
			return setMapPolicy(new MapPolicy(order, 0), binName);
		}

		Packer packer = new Packer();
		CDT.init(packer, ctx, SET_TYPE, 1, order.flag);
		packer.packInt(order.attributes);
		return new Operation(Operation.Type.MAP_MODIFY, binName, Value.get(packer.toByteArray()));
	}

	/**
	 * Create set map policy operation.
	 * Server sets map policy attributes.  Server returns null.
	 * <p>
	 * The required map policy attributes can be changed after the map is created.
	 */
	public static Operation setMapPolicy(MapPolicy policy, String binName, CTX... ctx) {
		return CDT.createOperation(SET_TYPE, Operation.Type.MAP_MODIFY, binName, ctx, policy.attributes);
	}

	/**
	 * Create map put operation.
	 * Server writes key/value item to map bin and returns map size.
	 * <p>
	 * The required map policy dictates the type of map to create when it does not exist.
	 * The map policy also specifies the flags used when writing items to the map.
	 * See policy {@link com.aerospike.client.cdt.MapPolicy}.
	 */
	public static Operation put(MapPolicy policy, String binName, Value key, Value value, CTX... ctx) {
		Packer packer = new Packer();

		if (policy.flags != 0) {
			CDT.init(packer, ctx, PUT, 4);
			key.pack(packer);
			value.pack(packer);
			packer.packInt(policy.attributes);
			packer.packInt(policy.flags);
		}
		else {
			if (policy.itemCommand == REPLACE) {
				// Replace doesn't allow map attributes because it does not create on non-existing key.
				CDT.init(packer, ctx, policy.itemCommand, 2);
				key.pack(packer);
				value.pack(packer);
			}
			else {
				CDT.init(packer, ctx, policy.itemCommand, 3);
				key.pack(packer);
				value.pack(packer);
				packer.packInt(policy.attributes);
			}
		}
		return new Operation(Operation.Type.MAP_MODIFY, binName, Value.get(packer.toByteArray()));
	}

	/**
	 * Create map put items operation
	 * Server writes each map item to map bin and returns map size.
	 * <p>
	 * The required map policy dictates the type of map to create when it does not exist.
	 * The map policy also specifies the flags used when writing items to the map.
	 * See policy {@link com.aerospike.client.cdt.MapPolicy}.
	 */
	public static Operation putItems(MapPolicy policy, String binName, Map<Value,Value> map, CTX... ctx) {
		Packer packer = new Packer();

		if (policy.flags != 0) {
			CDT.init(packer, ctx, PUT_ITEMS, 3);
			packer.packValueMap(map);
			packer.packInt(policy.attributes);
			packer.packInt(policy.flags);
		}
		else {
			if (policy.itemsCommand == REPLACE_ITEMS) {
				// Replace doesn't allow map attributes because it does not create on non-existing key.
				CDT.init(packer, ctx, policy.itemsCommand, 1);
				packer.packValueMap(map);
			}
			else {
				CDT.init(packer, ctx, policy.itemsCommand, 2);
				packer.packValueMap(map);
				packer.packInt(policy.attributes);
			}
		}
		return new Operation(Operation.Type.MAP_MODIFY, binName, Value.get(packer.toByteArray()));
	}

	/**
	 * Create map increment operation.
	 * Server increments values by incr for all items identified by key and returns final result.
	 * Valid only for numbers.
	 * <p>
	 * The required map policy dictates the type of map to create when it does not exist.
	 * The map policy also specifies the mode used when writing items to the map.
	 * See policy {@link com.aerospike.client.cdt.MapPolicy} and write mode
	 * {@link com.aerospike.client.cdt.MapWriteMode}.
	 */
	public static Operation increment(MapPolicy policy, String binName, Value key, Value incr, CTX... ctx) {
		return CDT.createOperation(INCREMENT, Operation.Type.MAP_MODIFY, binName, ctx, key, incr, policy.attributes);
	}

	/**
	 * Create map decrement operation.
	 * Server decrements values by decr for all items identified by key and returns final result.
	 * Valid only for numbers.
	 * <p>
	 * The required map policy dictates the type of map to create when it does not exist.
	 * The map policy also specifies the mode used when writing items to the map.
	 * See policy {@link com.aerospike.client.cdt.MapPolicy} and write mode
	 * {@link com.aerospike.client.cdt.MapWriteMode}.
	 */
	public static Operation decrement(MapPolicy policy, String binName, Value key, Value decr, CTX... ctx) {
		return CDT.createOperation(DECREMENT, Operation.Type.MAP_MODIFY, binName, ctx, key, decr, policy.attributes);
	}

	/**
	 * Create map clear operation.
	 * Server removes all items in map.  Server returns null.
	 */
	public static Operation clear(String binName, CTX... ctx) {
		return CDT.createOperation(CLEAR, Operation.Type.MAP_MODIFY, binName, ctx);
	}

	/**
	 * Create map remove operation.
	 * Server removes map item identified by key and returns removed data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation removeByKey(String binName, Value key, int returnType, CTX... ctx) {
		return CDT.createOperation(REMOVE_BY_KEY, Operation.Type.MAP_MODIFY, binName, ctx, returnType, key);
	}

	/**
	 * Create map remove operation.
	 * Server removes map items identified by keys and returns removed data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation removeByKeyList(String binName, List<Value> keys, int returnType, CTX... ctx) {
		return CDT.createOperation(REMOVE_BY_KEY_LIST, Operation.Type.MAP_MODIFY, binName, ctx, returnType, keys);
	}

	/**
	 * Create map remove operation.
	 * Server removes map items identified by key range (keyBegin inclusive, keyEnd exclusive).
	 * If keyBegin is null, the range is less than keyEnd.
	 * If keyEnd is null, the range is greater than equal to keyBegin.
	 * <p>
	 * Server returns removed data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation removeByKeyRange(String binName, Value keyBegin, Value keyEnd, int returnType, CTX... ctx) {
		return CDT.createRangeOperation(REMOVE_BY_KEY_INTERVAL, Operation.Type.MAP_MODIFY, binName, ctx, keyBegin, keyEnd, returnType);
	}

	/**
	 * Create map remove by key relative to index range operation.
	 * Server removes map items nearest to key and greater by index.
	 * Server returns removed data specified by returnType (See {@link MapReturnType}).
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
	public static Operation removeByKeyRelativeIndexRange(String binName, Value key, int index, int returnType, CTX... ctx) {
		return CDT.createOperation(REMOVE_BY_KEY_REL_INDEX_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, returnType, key, index);
	}

	/**
	 * Create map remove by key relative to index range operation.
	 * Server removes map items nearest to key and greater by index with a count limit.
	 * Server returns removed data specified by returnType (See {@link MapReturnType}).
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
	public static Operation removeByKeyRelativeIndexRange(String binName, Value key, int index, int count, int returnType, CTX... ctx) {
		return CDT.createOperation(REMOVE_BY_KEY_REL_INDEX_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, returnType, key, index, count);
	}

	/**
	 * Create map remove operation.
	 * Server removes map items identified by value and returns removed data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation removeByValue(String binName, Value value, int returnType, CTX... ctx) {
		return CDT.createOperation(REMOVE_BY_VALUE, Operation.Type.MAP_MODIFY, binName, ctx, returnType, value);
	}

	/**
	 * Create map remove operation.
	 * Server removes map items identified by values and returns removed data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation removeByValueList(String binName, List<Value> values, int returnType, CTX... ctx) {
		return CDT.createOperation(REMOVE_BY_VALUE_LIST, Operation.Type.MAP_MODIFY, binName, ctx, returnType, values);
	}

	/**
	 * Create map remove operation.
	 * Server removes map items identified by value range (valueBegin inclusive, valueEnd exclusive).
	 * If valueBegin is null, the range is less than valueEnd.
	 * If valueEnd is null, the range is greater than equal to valueBegin.
	 * <p>
	 * Server returns removed data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation removeByValueRange(String binName, Value valueBegin, Value valueEnd, int returnType, CTX... ctx) {
		return CDT.createRangeOperation(REMOVE_BY_VALUE_INTERVAL, Operation.Type.MAP_MODIFY, binName, ctx, valueBegin, valueEnd, returnType);
	}

	/**
	 * Create map remove by value relative to rank range operation.
	 * Server removes map items nearest to value and greater by relative rank.
	 * Server returns removed data specified by returnType (See {@link MapReturnType}).
	 * <p>
	 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
	 * <ul>
	 * <li>(value,rank) = [removed items]</li>
	 * <li>(11,1) = [{0=17}]</li>
	 * <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
	 * </ul>
	 */
	public static Operation removeByValueRelativeRankRange(String binName, Value value, int rank, int returnType, CTX... ctx) {
		return CDT.createOperation(REMOVE_BY_VALUE_REL_RANK_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, returnType, value, rank);
	}

	/**
	 * Create map remove by value relative to rank range operation.
	 * Server removes map items nearest to value and greater by relative rank with a count limit.
	 * Server returns removed data specified by returnType (See {@link MapReturnType}).
	 * <p>
	 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
	 * <ul>
	 * <li>(value,rank,count) = [removed items]</li>
	 * <li>(11,1,1) = [{0=17}]</li>
	 * <li>(11,-1,1) = [{9=10}]</li>
	 * </ul>
	 */
	public static Operation removeByValueRelativeRankRange(String binName, Value value, int rank, int count, int returnType, CTX... ctx) {
		return CDT.createOperation(REMOVE_BY_VALUE_REL_RANK_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, returnType, value, rank, count);
	}

	/**
	 * Create map remove operation.
	 * Server removes map item identified by index and returns removed data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation removeByIndex(String binName, int index, int returnType, CTX... ctx) {
		return CDT.createOperation(REMOVE_BY_INDEX, Operation.Type.MAP_MODIFY, binName, ctx, returnType, index);
	}

	/**
	 * Create map remove operation.
	 * Server removes map items starting at specified index to the end of map and returns removed
	 * data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation removeByIndexRange(String binName, int index, int returnType, CTX... ctx) {
		return CDT.createOperation(REMOVE_BY_INDEX_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, returnType, index);
	}

	/**
	 * Create map remove operation.
	 * Server removes "count" map items starting at specified index and returns removed data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation removeByIndexRange(String binName, int index, int count, int returnType, CTX... ctx) {
		return CDT.createOperation(REMOVE_BY_INDEX_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, returnType, index, count);
	}

	/**
	 * Create map remove operation.
	 * Server removes map item identified by rank and returns removed data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation removeByRank(String binName, int rank, int returnType, CTX... ctx) {
		return CDT.createOperation(REMOVE_BY_RANK, Operation.Type.MAP_MODIFY, binName, ctx, returnType, rank);
	}

	/**
	 * Create map remove operation.
	 * Server removes map items starting at specified rank to the last ranked item and returns removed
	 * data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation removeByRankRange(String binName, int rank, int returnType, CTX... ctx) {
		return CDT.createOperation(REMOVE_BY_RANK_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, returnType, rank);
	}

	/**
	 * Create map remove operation.
	 * Server removes "count" map items starting at specified rank and returns removed data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation removeByRankRange(String binName, int rank, int count, int returnType, CTX... ctx) {
		return CDT.createOperation(REMOVE_BY_RANK_RANGE, Operation.Type.MAP_MODIFY, binName, ctx, returnType, rank, count);
	}

	/**
	 * Create map size operation.
	 * Server returns size of map.
	 */
	public static Operation size(String binName, CTX... ctx) {
		return CDT.createOperation(SIZE, Operation.Type.MAP_READ, binName, ctx);
	}

	/**
	 * Create map get by key operation.
	 * Server selects map item identified by key and returns selected data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation getByKey(String binName, Value key, int returnType, CTX... ctx) {
		return CDT.createOperation(GET_BY_KEY, Operation.Type.MAP_READ, binName, ctx, returnType, key);
	}

	/**
	 * Create map get by key range operation.
	 * Server selects map items identified by key range (keyBegin inclusive, keyEnd exclusive).
	 * If keyBegin is null, the range is less than keyEnd.
	 * If keyEnd is null, the range is greater than equal to keyBegin.
	 * <p>
	 * Server returns selected data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation getByKeyRange(String binName, Value keyBegin, Value keyEnd, int returnType, CTX... ctx) {
		return CDT.createRangeOperation(GET_BY_KEY_INTERVAL, Operation.Type.MAP_READ, binName, ctx, keyBegin, keyEnd, returnType);
	}

	/**
	 * Create map get by key list operation.
	 * Server selects map items identified by keys and returns selected data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation getByKeyList(String binName, List<Value> keys, int returnType, CTX... ctx) {
		return CDT.createOperation(GET_BY_KEY_LIST, Operation.Type.MAP_READ, binName, ctx, returnType, keys);
	}

	/**
	 * Create map get by key relative to index range operation.
	 * Server selects map items nearest to key and greater by index.
	 * Server returns selected data specified by returnType (See {@link MapReturnType}).
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
	public static Operation getByKeyRelativeIndexRange(String binName, Value key, int index, int returnType, CTX... ctx) {
		return CDT.createOperation(GET_BY_KEY_REL_INDEX_RANGE, Operation.Type.MAP_READ, binName, ctx, returnType, key, index);
	}

	/**
	 * Create map get by key relative to index range operation.
	 * Server selects map items nearest to key and greater by index with a count limit.
	 * Server returns selected data specified by returnType (See {@link MapReturnType}).
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
	public static Operation getByKeyRelativeIndexRange(String binName, Value key, int index, int count, int returnType, CTX... ctx) {
		return CDT.createOperation(GET_BY_KEY_REL_INDEX_RANGE, Operation.Type.MAP_READ, binName, ctx, returnType, key, index, count);
	}

	/**
	 * Create map get by value operation.
	 * Server selects map items identified by value and returns selected data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation getByValue(String binName, Value value, int returnType, CTX... ctx) {
		return CDT.createOperation(GET_BY_VALUE, Operation.Type.MAP_READ, binName, ctx, returnType, value);
	}

	/**
	 * Create map get by value range operation.
	 * Server selects map items identified by value range (valueBegin inclusive, valueEnd exclusive)
	 * If valueBegin is null, the range is less than valueEnd.
	 * If valueEnd is null, the range is greater than equal to valueBegin.
	 * <p>
	 * Server returns selected data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation getByValueRange(String binName, Value valueBegin, Value valueEnd, int returnType, CTX... ctx) {
		return CDT.createRangeOperation(GET_BY_VALUE_INTERVAL, Operation.Type.MAP_READ, binName, ctx, valueBegin, valueEnd, returnType);
	}

	/**
	 * Create map get by value list operation.
	 * Server selects map items identified by values and returns selected data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation getByValueList(String binName, List<Value> values, int returnType, CTX... ctx) {
		return CDT.createOperation(GET_BY_VALUE_LIST, Operation.Type.MAP_READ, binName, ctx, returnType, values);
	}

	/**
	 * Create map get by value relative to rank range operation.
	 * Server selects map items nearest to value and greater by relative rank.
	 * Server returns selected data specified by returnType (See {@link MapReturnType}).
	 * <p>
	 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
	 * <ul>
	 * <li>(value,rank) = [selected items]</li>
	 * <li>(11,1) = [{0=17}]</li>
	 * <li>(11,-1) = [{9=10},{5=15},{0=17}]</li>
	 * </ul>
	 */
	public static Operation getByValueRelativeRankRange(String binName, Value value, int rank, int returnType, CTX... ctx) {
		return CDT.createOperation(GET_BY_VALUE_REL_RANK_RANGE, Operation.Type.MAP_READ, binName, ctx, returnType, value, rank);
	}

	/**
	 * Create map get by value relative to rank range operation.
	 * Server selects map items nearest to value and greater by relative rank with a count limit.
	 * Server returns selected data specified by returnType (See {@link MapReturnType}).
	 * <p>
	 * Examples for map [{4=2},{9=10},{5=15},{0=17}]:
	 * <ul>
	 * <li>(value,rank,count) = [selected items]</li>
	 * <li>(11,1,1) = [{0=17}]</li>
	 * <li>(11,-1,1) = [{9=10}]</li>
	 * </ul>
	 */
	public static Operation getByValueRelativeRankRange(String binName, Value value, int rank, int count, int returnType, CTX... ctx) {
		return CDT.createOperation(GET_BY_VALUE_REL_RANK_RANGE, Operation.Type.MAP_READ, binName, ctx, returnType, value, rank, count);
	}

	/**
	 * Create map get by index operation.
	 * Server selects map item identified by index and returns selected data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation getByIndex(String binName, int index, int returnType, CTX... ctx) {
		return CDT.createOperation(GET_BY_INDEX, Operation.Type.MAP_READ, binName, ctx, returnType, index);
	}

	/**
	 * Create map get by index range operation.
	 * Server selects map items starting at specified index to the end of map and returns selected
	 * data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation getByIndexRange(String binName, int index, int returnType, CTX... ctx) {
		return CDT.createOperation(GET_BY_INDEX_RANGE, Operation.Type.MAP_READ, binName, ctx, returnType, index);
	}

	/**
	 * Create map get by index range operation.
	 * Server selects "count" map items starting at specified index and returns selected data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation getByIndexRange(String binName, int index, int count, int returnType, CTX... ctx) {
		return CDT.createOperation(GET_BY_INDEX_RANGE, Operation.Type.MAP_READ, binName, ctx, returnType, index, count);
	}

	/**
	 * Create map get by rank operation.
	 * Server selects map item identified by rank and returns selected data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation getByRank(String binName, int rank, int returnType, CTX... ctx) {
		return CDT.createOperation(GET_BY_RANK, Operation.Type.MAP_READ, binName, ctx, returnType, rank);
	}

	/**
	 * Create map get by rank range operation.
	 * Server selects map items starting at specified rank to the last ranked item and returns selected
	 * data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation getByRankRange(String binName, int rank, int returnType, CTX... ctx) {
		return CDT.createOperation(GET_BY_RANK_RANGE, Operation.Type.MAP_READ, binName, ctx, returnType, rank);
	}

	/**
	 * Create map get by rank range operation.
	 * Server selects "count" map items starting at specified rank and returns selected data specified by returnType (See {@link MapReturnType}).
	 */
	public static Operation getByRankRange(String binName, int rank, int count, int returnType, CTX... ctx) {
		return CDT.createOperation(GET_BY_RANK_RANGE, Operation.Type.MAP_READ, binName, ctx, returnType, rank, count);
	}
}

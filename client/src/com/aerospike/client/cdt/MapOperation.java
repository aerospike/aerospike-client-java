/*
 * Copyright 2012-2017 Aerospike, Inc.
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
 */
public class MapOperation {	
	/**
	 * Create set map policy operation.
	 * Server sets map policy attributes.  Server returns null.
	 * <p>
	 * The required map policy attributes can be changed after the map is created.
	 */
	public static Operation setMapPolicy(MapPolicy policy, String binName) {
		return MapBase.setMapPolicy(binName, policy.attributes);
	}
	
	/**
	 * Create map put operation.
	 * Server writes key/value item to map bin and returns map size.
	 * <p>
	 * The required map policy dictates the type of map to create when it does not exist.
	 * The map policy also specifies the mode used when writing items to the map.
	 * See policy {@link com.aerospike.client.cdt.MapPolicy} and write mode 
	 * {@link com.aerospike.client.cdt.MapWriteMode}.
	 */
	public static Operation put(MapPolicy policy, String binName, Value key, Value value) {
		return MapBase.createPut(policy.itemCommand, policy.attributes, binName, key, value);
	}

	/**
	 * Create map put items operation
	 * Server writes each map item to map bin and returns map size.
	 * <p>
	 * The required map policy dictates the type of map to create when it does not exist.
	 * The map policy also specifies the mode used when writing items to the map.
	 * See policy {@link com.aerospike.client.cdt.MapPolicy} and write mode 
	 * {@link com.aerospike.client.cdt.MapWriteMode}.
	 */
	public static Operation putItems(MapPolicy policy, String binName, Map<Value,Value> map) {
		Packer packer = new Packer();
		packer.packRawShort(policy.itemsCommand);
		
		if (policy.itemsCommand == MapBase.REPLACE_ITEMS) {
			// Replace doesn't allow map attributes because it does not create on non-existing key.
			packer.packArrayBegin(1);
			packer.packValueMap(map);
		}
		else {
			packer.packArrayBegin(2);
			packer.packValueMap(map);
			packer.packInt(policy.attributes);			
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
	public static Operation increment(MapPolicy policy, String binName, Value key, Value incr) {
		return MapBase.createOperation(MapBase.INCREMENT, policy.attributes, binName, key, incr);
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
	public static Operation decrement(MapPolicy policy, String binName, Value key, Value decr) {
		return MapBase.createOperation(MapBase.DECREMENT, policy.attributes, binName, key, decr);
	}

	/**
	 * Create map clear operation.
	 * Server removes all items in map.  Server returns null.
	 */
	public static Operation clear(String binName) {
		return MapBase.createOperation(MapBase.CLEAR, Operation.Type.MAP_MODIFY, binName);
	}

	/**
	 * Create map remove operation.
	 * Server removes map item identified by key and returns removed data specified by returnType.
	 */
	public static Operation removeByKey(String binName, Value key, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.REMOVE_BY_KEY, Operation.Type.MAP_MODIFY, binName, key, returnType);
	}

	/**
	 * Create map remove operation.
	 * Server removes map items identified by keys and returns removed data specified by returnType.
	 */
	public static Operation removeByKeyList(String binName, List<Value> keys, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.REMOVE_BY_KEY_LIST, Operation.Type.MAP_MODIFY, binName, keys, returnType);
	}

	/**
	 * Create map remove operation.
	 * Server removes map items identified by key range (keyBegin inclusive, keyEnd exclusive).
	 * If keyBegin is null, the range is less than keyEnd.
	 * If keyEnd is null, the range is greater than equal to keyBegin. 
	 * <p>
	 * Server returns removed data specified by returnType. 
	 */
	public static Operation removeByKeyRange(String binName, Value keyBegin, Value keyEnd, MapReturnType returnType) {
		return MapBase.createRangeOperation(MapBase.REMOVE_BY_KEY_INTERVAL, Operation.Type.MAP_MODIFY, binName, keyBegin, keyEnd, returnType);
	}

	/**
	 * Create map remove operation.
	 * Server removes map items identified by value and returns removed data specified by returnType.
	 */
	public static Operation removeByValue(String binName, Value value, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.REMOVE_BY_VALUE, Operation.Type.MAP_MODIFY, binName, value, returnType);
	}
	
	/**
	 * Create map remove operation.
	 * Server removes map items identified by values and returns removed data specified by returnType.
	 */
	public static Operation removeByValueList(String binName, List<Value> values, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.REMOVE_BY_VALUE_LIST, Operation.Type.MAP_MODIFY, binName, values, returnType);
	}

	/**
	 * Create map remove operation.
	 * Server removes map items identified by value range (valueBegin inclusive, valueEnd exclusive).
	 * If valueBegin is null, the range is less than valueEnd.
	 * If valueEnd is null, the range is greater than equal to valueBegin. 
	 * <p>
	 * Server returns removed data specified by returnType. 
	 */
	public static Operation removeByValueRange(String binName, Value valueBegin, Value valueEnd, MapReturnType returnType) {
		return MapBase.createRangeOperation(MapBase.REMOVE_BY_VALUE_INTERVAL, Operation.Type.MAP_MODIFY, binName, valueBegin, valueEnd, returnType);
	}

	/**
	 * Create map remove operation.
	 * Server removes map item identified by index and returns removed data specified by returnType. 
	 */
	public static Operation removeByIndex(String binName, int index, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.REMOVE_BY_INDEX, Operation.Type.MAP_MODIFY, binName, index, returnType);
	}
	
	/**
	 * Create map remove operation.
	 * Server removes map items starting at specified index to the end of map and returns removed
	 * data specified by returnType.
	 */
	public static Operation removeByIndexRange(String binName, int index, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.REMOVE_BY_INDEX_RANGE, Operation.Type.MAP_MODIFY, binName, index, returnType);
	}

	/**
	 * Create map remove operation.
	 * Server removes "count" map items starting at specified index and returns removed data specified by returnType.
	 */
	public static Operation removeByIndexRange(String binName, int index, int count, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.REMOVE_BY_INDEX_RANGE, Operation.Type.MAP_MODIFY, binName, index, count, returnType);
	}	

	/**
	 * Create map remove operation.
	 * Server removes map item identified by rank and returns removed data specified by returnType.
	 */
	public static Operation removeByRank(String binName, int rank, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.REMOVE_BY_RANK, Operation.Type.MAP_MODIFY, binName, rank, returnType);
	}

	/**
	 * Create map remove operation.
	 * Server removes map items starting at specified rank to the last ranked item and returns removed
	 * data specified by returnType.
	 */
	public static Operation removeByRankRange(String binName, int rank, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.REMOVE_BY_RANK_RANGE, Operation.Type.MAP_MODIFY, binName, rank, returnType);
	}

	/**
	 * Create map remove operation.
	 * Server removes "count" map items starting at specified rank and returns removed data specified by returnType.
	 */
	public static Operation removeByRankRange(String binName, int rank, int count, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.REMOVE_BY_RANK_RANGE, Operation.Type.MAP_MODIFY, binName, rank, count, returnType);
	}	

	/**
	 * Create map size operation.
	 * Server returns size of map.
	 */
	public static Operation size(String binName) {
		return MapBase.createOperation(MapBase.SIZE, Operation.Type.MAP_READ, binName);
	}
	
	/**
	 * Create map get by key operation.
	 * Server selects map item identified by key and returns selected data specified by returnType.
	 */
	public static Operation getByKey(String binName, Value key, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.GET_BY_KEY, Operation.Type.MAP_READ, binName, key, returnType);
	}

	/**
	 * Create map get by key range operation.
	 * Server selects map items identified by key range (keyBegin inclusive, keyEnd exclusive). 
	 * If keyBegin is null, the range is less than keyEnd.
	 * If keyEnd is null, the range is greater than equal to keyBegin. 
	 * <p>
	 * Server returns selected data specified by returnType. 
	 */
	public static Operation getByKeyRange(String binName, Value keyBegin, Value keyEnd, MapReturnType returnType) {
		return MapBase.createRangeOperation(MapBase.GET_BY_KEY_INTERVAL, Operation.Type.MAP_READ, binName, keyBegin, keyEnd, returnType);
	}

	/**
	 * Create map get by value operation.
	 * Server selects map items identified by value and returns selected data specified by returnType.
	 */
	public static Operation getByValue(String binName, Value value, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.GET_BY_VALUE, Operation.Type.MAP_READ, binName, value, returnType);
	}

	/**
	 * Create map get by value range operation.
	 * Server selects map items identified by value range (valueBegin inclusive, valueEnd exclusive)
	 * If valueBegin is null, the range is less than valueEnd.
	 * If valueEnd is null, the range is greater than equal to valueBegin. 
	 * <p>
	 * Server returns selected data specified by returnType. 
	 */
	public static Operation getByValueRange(String binName, Value valueBegin, Value valueEnd, MapReturnType returnType) {
		return MapBase.createRangeOperation(MapBase.GET_BY_VALUE_INTERVAL, Operation.Type.MAP_READ, binName, valueBegin, valueEnd, returnType);
	}

	/**
	 * Create map get by index operation.
	 * Server selects map item identified by index and returns selected data specified by returnType. 
	 */
	public static Operation getByIndex(String binName, int index, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.GET_BY_INDEX, Operation.Type.MAP_READ, binName, index, returnType);
	}
	
	/**
	 * Create map get by index range operation.
	 * Server selects map items starting at specified index to the end of map and returns selected
	 * data specified by returnType.
	 */
	public static Operation getByIndexRange(String binName, int index, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.GET_BY_INDEX_RANGE, Operation.Type.MAP_READ, binName, index, returnType);
	}

	/**
	 * Create map get by index range operation.
	 * Server selects "count" map items starting at specified index and returns selected data specified by returnType.
	 */
	public static Operation getByIndexRange(String binName, int index, int count, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.GET_BY_INDEX_RANGE, Operation.Type.MAP_READ, binName, index, count, returnType);
	}	

	/**
	 * Create map get by rank operation.
	 * Server selects map item identified by rank and returns selected data specified by returnType.
	 */
	public static Operation getByRank(String binName, int rank, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.GET_BY_RANK, Operation.Type.MAP_READ, binName, rank, returnType);
	}

	/**
	 * Create map get by rank range operation.
	 * Server selects map items starting at specified rank to the last ranked item and returns selected
	 * data specified by returnType.
	 */
	public static Operation getByRankRange(String binName, int rank, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.GET_BY_RANK_RANGE, Operation.Type.MAP_READ, binName, rank, returnType);
	}

	/**
	 * Create map get by rank range operation.
	 * Server selects "count" map items starting at specified rank and returns selected data specified by returnType.
	 */
	public static Operation getByRankRange(String binName, int rank, int count, MapReturnType returnType) {
		return MapBase.createOperation(MapBase.GET_BY_RANK_RANGE, Operation.Type.MAP_READ, binName, rank, count, returnType);
	}	
}

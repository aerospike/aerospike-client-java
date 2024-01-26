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
package com.aerospike.client.cdt;

import java.util.List;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.util.Pack;
import com.aerospike.client.util.Packer;

/**
 * List bin operations. Create list operations used by client operate command.
 * <p>
 * List operations support negative indexing.  If the index is negative, the
 * resolved index starts backwards from end of list. If an index is out of bounds,
 * a parameter error will be returned. If a range is partially out of bounds, the
 * valid part of the range will be returned. Index/Range examples:
 * <ul>
 * <li>Index 0: First item in list.</li>
 * <li>Index 4: Fifth item in list.</li>
 * <li>Index -1: Last item in list.</li>
 * <li>Index -3: Third to last item in list.</li>
 * <li>Index 1 Count 2: Second and third items in list.</li>
 * <li>Index -3 Count 3: Last three items in list.</li>
 * <li>Index -5 Count 4: Range between fifth to last item to second to last item inclusive.</li>
 * </ul>
 * <p>
 * Nested CDT operations are supported by optional CTX context arguments.  Examples:
 * <ul>
 * <li>bin = [[7,9,5],[1,2,3],[6,5,4,1]]</li>
 * <li>Append 11 to last list.</li>
 * <li>ListOperation.append("bin", Value.get(11), CTX.listIndex(-1))</li>
 * <li>bin result = [[7,9,5],[1,2,3],[6,5,4,1,11]]</li>
 * <li></li>
 * <li>bin = {key1=[[7,9,5],[13]], key2=[[9],[2,4],[6,1,9]], key3=[[6,5]]}</li>
 * <li>Append 11 to lowest ranked list in map identified by "key2".</li>
 * <li>ListOperation.append("bin", Value.get(11), CTX.mapKey(Value.get("key2")), CTX.listRank(0))</li>
 * <li>bin result = {key1=[[7,9,5],[13]], key2=[[9],[2,4,11],[6,1,9]], key3=[[6,5]]}</li>
 * </ul>
 */
public class ListOperation {
	private static final int SET_TYPE = 0;
	private static final int APPEND = 1;
	private static final int APPEND_ITEMS = 2;
	private static final int INSERT = 3;
	private static final int INSERT_ITEMS = 4;
	private static final int POP = 5;
	private static final int POP_RANGE = 6;
	private static final int REMOVE = 7;
	private static final int REMOVE_RANGE = 8;
	private static final int SET = 9;
	private static final int TRIM = 10;
	private static final int CLEAR = 11;
	private static final int INCREMENT = 12;
	private static final int SORT = 13;
	private static final int SIZE = 16;
	private static final int GET = 17;
	private static final int GET_RANGE = 18;
	private static final int GET_BY_INDEX = 19;
	private static final int GET_BY_RANK = 21;
	private static final int GET_BY_VALUE = 22;  // GET_ALL_BY_VALUE on server.
	private static final int GET_BY_VALUE_LIST = 23;
	private static final int GET_BY_INDEX_RANGE = 24;
	private static final int GET_BY_VALUE_INTERVAL = 25;
	private static final int GET_BY_RANK_RANGE = 26;
	private static final int GET_BY_VALUE_REL_RANK_RANGE = 27;
	private static final int REMOVE_BY_INDEX = 32;
	private static final int REMOVE_BY_RANK = 34;
	private static final int REMOVE_BY_VALUE = 35;
	private static final int REMOVE_BY_VALUE_LIST = 36;
	private static final int REMOVE_BY_INDEX_RANGE = 37;
	private static final int REMOVE_BY_VALUE_INTERVAL = 38;
	private static final int REMOVE_BY_RANK_RANGE = 39;
	private static final int REMOVE_BY_VALUE_REL_RANK_RANGE = 40;

	/**
	 * Create list create operation.
	 * Server creates list at given context level.
	 *
	 * @param binName		bin name
	 * @param order			list order
	 * @param pad			if true, context is allowed to be beyond list boundaries and insert nil list entries to
	 *						satisfy the ctx position
	 * @param ctx			optional path to nested list. If not defined, the top-level list is used.
	 */
	public static Operation create(String binName, ListOrder order, boolean pad, CTX... ctx) {
		// If context not defined, the set order for top-level bin list.
		if (ctx == null || ctx.length == 0) {
			return setOrder(binName, order);
		}

		byte[] bytes = packCreate(order, pad, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list create operation.
	 * Server creates list at given context level.
	 *
	 * @param binName		bin name
	 * @param order			list order
	 * @param pad			if true, context is allowed to be beyond list boundaries and insert nil list entries to
	 *						satisfy the ctx position
	 * @param persistIndex	if true, persist list index. A list index improves lookup performance,
	 * 						but requires more storage. A list index can be created for a top-level
	 * 						ordered list only. Nested and unordered list indexes are not supported.
	 * @param ctx			optional path to nested list. If not defined, the top-level list is used.
	 */
	public static Operation create(String binName, ListOrder order, boolean pad, boolean persistIndex, CTX... ctx) {
		// If context not defined, the set order for top-level bin list.
		if (ctx == null || ctx.length == 0) {
			return setOrder(binName, order, persistIndex);
		}

		// Create nested list. persistIndex does not apply here, so ignore it.
		byte[] bytes = packCreate(order, pad, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	private static byte[] packCreate(ListOrder order, boolean pad, CTX[] ctx) {
		Packer packer = new Packer();
		CDT.init(packer, ctx, SET_TYPE, 1, order.getFlag(pad));
		packer.packInt(order.attributes);
		return packer.toByteArray();
	}

	/**
	 * Create set list order operation.
	 * Server sets list order.  Server returns null.
	 */
	public static Operation setOrder(String binName, ListOrder order, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.SET_TYPE, order.attributes, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create set list order operation.
	 * Server sets list order.  Server returns null.
	 *
	 * @param binName		bin name
	 * @param order			list order
	 * @param persistIndex	if true, persist list index. A list index improves lookup performance,
	 * 						but requires more storage. A list index can be created for a top-level
	 * 						ordered list only. Nested and unordered list indexes are not supported.
	 * @param ctx			optional path to nested list. If not defined, the top-level list is used.
	 */
	public static Operation setOrder(String binName, ListOrder order, boolean persistIndex, CTX... ctx) {
		int attr = order.attributes;

		if (persistIndex) {
			attr |= 0x10;
		}
		byte[] bytes = Pack.pack(ListOperation.SET_TYPE, attr, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create default list append operation.
	 * Server appends value to end of list bin.
	 * Server returns list size.
	 */
	public static Operation append(String binName, Value value, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.APPEND, value, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list append operation with policy.
	 * Server appends value to list bin.
	 * Server returns list size.
	 */
	public static Operation append(ListPolicy policy, String binName, Value value, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.APPEND, value, policy.attributes, policy.flags, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create default list append items operation.
	 * Server appends each input list item to end of list bin.
	 * Server returns list size.
	 */
	public static Operation appendItems(String binName, List<Value> list, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.APPEND_ITEMS, list, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list append items operation with policy.
	 * Server appends each input list item to list bin.
	 * Server returns list size.
	 */
	public static Operation appendItems(ListPolicy policy, String binName, List<Value> list, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.APPEND_ITEMS, list, policy.attributes, policy.flags, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create default list insert operation.
	 * Server inserts value to specified index of list bin.
	 * Server returns list size.
	 */
	public static Operation insert(String binName, int index, Value value, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.INSERT, index, value, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list insert operation with policy.
	 * Server inserts value to specified index of list bin.
	 * Server returns list size.
	 */
	public static Operation insert(ListPolicy policy, String binName, int index, Value value, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.INSERT, index, value, policy.flags, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create default list insert items operation.
	 * Server inserts each input list item starting at specified index of list bin.
	 * Server returns list size.
	 */
	public static Operation insertItems(String binName, int index, List<Value> list, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.INSERT_ITEMS, index, list, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list insert items operation with policy.
	 * Server inserts each input list item starting at specified index of list bin.
	 * Server returns list size.
	 */
	public static Operation insertItems(ListPolicy policy, String binName, int index, List<Value> list, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.INSERT_ITEMS, index, list, policy.flags, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create default list increment operation.
	 * Server increments list[index] by 1.
	 * Server returns list[index] after incrementing.
	 */
	public static Operation increment(String binName, int index, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.INCREMENT, index, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list increment operation with policy.
	 * Server increments list[index] by 1.
	 * Server returns list[index] after incrementing.
	 */
	public static Operation increment(ListPolicy policy, String binName, int index, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.INCREMENT, index, Value.get(1), policy.attributes, policy.flags, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create default list increment operation.
	 * Server increments list[index] by value.
	 * Value should be integer(IntegerValue, LongValue) or double(DoubleValue, FloatValue).
	 * Server returns list[index] after incrementing.
	 */
	public static Operation increment(String binName, int index, Value value, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.INCREMENT, index, value, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list increment operation.
	 * Server increments list[index] by value.
	 * Value should be integer(IntegerValue, LongValue) or double(DoubleValue, FloatValue).
	 * Server returns list[index] after incrementing.
	 */
	public static Operation increment(ListPolicy policy, String binName, int index, Value value, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.INCREMENT, index, value, policy.attributes, policy.flags, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list pop operation.
	 * Server returns item at specified index and removes item from list bin.
	 */
	public static Operation pop(String binName, int index, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.POP, index, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list pop range operation.
	 * Server returns "count" items starting at specified index and removes items from list bin.
	 */
	public static Operation popRange(String binName, int index, int count, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.POP_RANGE, index, count, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list pop range operation.
	 * Server returns items starting at specified index to the end of list and removes those items
	 * from list bin.
	 */
	public static Operation popRange(String binName, int index, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.POP_RANGE, index, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove operation.
	 * Server removes item at specified index from list bin.
	 * Server returns number of items removed.
	 */
	public static Operation remove(String binName, int index, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.REMOVE, index, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove range operation.
	 * Server removes "count" items starting at specified index from list bin.
	 * Server returns number of items removed.
	 */
	public static Operation removeRange(String binName, int index, int count, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.REMOVE_RANGE, index, count, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove range operation.
	 * Server removes items starting at specified index to the end of list.
	 * Server returns number of items removed.
	 */
	public static Operation removeRange(String binName, int index, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.REMOVE_RANGE, index, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list set operation.
	 * Server sets item value at specified index in list bin.
	 * Server does not return a result by default.
	 */
	public static Operation set(String binName, int index, Value value, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.SET, index, value, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list set operation with policy.
	 * Server sets item value at specified index in list bin.
	 * Server does not return a result by default.
	 */
	public static Operation set(ListPolicy policy, String binName, int index, Value value, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.SET, index, value, policy.flags, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list trim operation.
	 * Server removes items in list bin that do not fall into range specified by index
	 * and count range.  If the range is out of bounds, then all items will be removed.
	 * Server returns list size after trim.
	 */
	public static Operation trim(String binName, int index, int count, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.TRIM, index, count, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list clear operation.
	 * Server removes all items in list bin.
	 * Server does not return a result by default.
	 */
	public static Operation clear(String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.CLEAR, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list sort operation.
	 * Server sorts list according to sortFlags.
	 * Server does not return a result by default.
	 *
	 * @param binName	server bin name
	 * @param sortFlags sort flags. See {@link ListSortFlags}.
	 * @param ctx		optional context path for nested CDT
	 */
	public static Operation sort(String binName, int sortFlags, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.SORT, sortFlags, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove operation.
	 * Server removes list items identified by value and returns removed data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByValue(String binName, Value value, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.REMOVE_BY_VALUE, returnType, value, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove operation.
	 * Server removes list items identified by values and returns removed data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByValueList(String binName, List<Value> values, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.REMOVE_BY_VALUE_LIST, returnType, values, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove operation.
	 * Server removes list items identified by value range (valueBegin inclusive, valueEnd exclusive).
	 * If valueBegin is null, the range is less than valueEnd.
	 * If valueEnd is null, the range is greater than equal to valueBegin.
	 * <p>
	 * Server returns removed data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByValueRange(String binName, Value valueBegin, Value valueEnd, int returnType, CTX... ctx) {
		byte[] bytes = CDT.packRangeOperation(ListOperation.REMOVE_BY_VALUE_INTERVAL, returnType, valueBegin, valueEnd, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove by value relative to rank range operation.
	 * Server removes list items nearest to value and greater by relative rank.
	 * Server returns removed data specified by returnType (See {@link ListReturnType}).
	 * <p>
	 * Examples for ordered list [0,4,5,9,11,15]:
	 * <ul>
	 * <li>(value,rank) = [removed items]</li>
	 * <li>(5,0) = [5,9,11,15]</li>
	 * <li>(5,1) = [9,11,15]</li>
	 * <li>(5,-1) = [4,5,9,11,15]</li>
	 * <li>(3,0) = [4,5,9,11,15]</li>
	 * <li>(3,3) = [11,15]</li>
	 * <li>(3,-3) = [0,4,5,9,11,15]</li>
	 * </ul>
	 */
	public static Operation removeByValueRelativeRankRange(String binName, Value value, int rank, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.REMOVE_BY_VALUE_REL_RANK_RANGE, returnType, value, rank, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove by value relative to rank range operation.
	 * Server removes list items nearest to value and greater by relative rank with a count limit.
	 * Server returns removed data specified by returnType (See {@link ListReturnType}).
	 * <p>
	 * Examples for ordered list [0,4,5,9,11,15]:
	 * <ul>
	 * <li>(value,rank,count) = [removed items]</li>
	 * <li>(5,0,2) = [5,9]</li>
	 * <li>(5,1,1) = [9]</li>
	 * <li>(5,-1,2) = [4,5]</li>
	 * <li>(3,0,1) = [4]</li>
	 * <li>(3,3,7) = [11,15]</li>
	 * <li>(3,-3,2) = []</li>
	 * </ul>
	 */
	public static Operation removeByValueRelativeRankRange(String binName, Value value, int rank, int count, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.REMOVE_BY_VALUE_REL_RANK_RANGE, returnType, value, rank, count, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove operation.
	 * Server removes list item identified by index and returns removed data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByIndex(String binName, int index, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.REMOVE_BY_INDEX, returnType, index, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove operation.
	 * Server removes list items starting at specified index to the end of list and returns removed
	 * data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByIndexRange(String binName, int index, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.REMOVE_BY_INDEX_RANGE, returnType, index, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove operation.
	 * Server removes "count" list items starting at specified index and returns removed data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByIndexRange(String binName, int index, int count, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.REMOVE_BY_INDEX_RANGE, returnType, index, count, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove operation.
	 * Server removes list item identified by rank and returns removed data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByRank(String binName, int rank, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.REMOVE_BY_RANK, returnType, rank, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove operation.
	 * Server removes list items starting at specified rank to the last ranked item and returns removed
	 * data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByRankRange(String binName, int rank, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.REMOVE_BY_RANK_RANGE, returnType, rank, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove operation.
	 * Server removes "count" list items starting at specified rank and returns removed data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByRankRange(String binName, int rank, int count, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.REMOVE_BY_RANK_RANGE, returnType, rank, count, ctx);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list size operation.
	 * Server returns size of list.
	 */
	public static Operation size(String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.SIZE, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get operation.
	 * Server returns item at specified index in list bin.
	 */
	public static Operation get(String binName, int index, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.GET, index, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get range operation.
	 * Server returns "count" items starting at specified index in list bin.
	 */
	public static Operation getRange(String binName, int index, int count, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.GET_RANGE, index, count, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get range operation.
	 * Server returns items starting at index to the end of list.
	 */
	public static Operation getRange(String binName, int index, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.GET_RANGE, index, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get by value operation.
	 * Server selects list items identified by value and returns selected data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByValue(String binName, Value value, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.GET_BY_VALUE, returnType, value, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get by value range operation.
	 * Server selects list items identified by value range (valueBegin inclusive, valueEnd exclusive)
	 * If valueBegin is null, the range is less than valueEnd.
	 * If valueEnd is null, the range is greater than equal to valueBegin.
	 * <p>
	 * Server returns selected data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByValueRange(String binName, Value valueBegin, Value valueEnd, int returnType, CTX... ctx) {
		byte[] bytes = CDT.packRangeOperation(ListOperation.GET_BY_VALUE_INTERVAL, returnType, valueBegin, valueEnd, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get by value list operation.
	 * Server selects list items identified by values and returns selected data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByValueList(String binName, List<Value> values, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.GET_BY_VALUE_LIST, returnType, values, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get by value relative to rank range operation.
	 * Server selects list items nearest to value and greater by relative rank.
	 * Server returns selected data specified by returnType (See {@link ListReturnType}).
	 * <p>
	 * Examples for ordered list [0,4,5,9,11,15]:
	 * <ul>
	 * <li>(value,rank) = [selected items]</li>
	 * <li>(5,0) = [5,9,11,15]</li>
	 * <li>(5,1) = [9,11,15]</li>
	 * <li>(5,-1) = [4,5,9,11,15]</li>
	 * <li>(3,0) = [4,5,9,11,15]</li>
	 * <li>(3,3) = [11,15]</li>
	 * <li>(3,-3) = [0,4,5,9,11,15]</li>
	 * </ul>
	 */
	public static Operation getByValueRelativeRankRange(String binName, Value value, int rank, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.GET_BY_VALUE_REL_RANK_RANGE, returnType, value, rank, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get by value relative to rank range operation.
	 * Server selects list items nearest to value and greater by relative rank with a count limit.
	 * Server returns selected data specified by returnType (See {@link ListReturnType}).
	 * <p>
	 * Examples for ordered list [0,4,5,9,11,15]:
	 * <ul>
	 * <li>(value,rank,count) = [selected items]</li>
	 * <li>(5,0,2) = [5,9]</li>
	 * <li>(5,1,1) = [9]</li>
	 * <li>(5,-1,2) = [4,5]</li>
	 * <li>(3,0,1) = [4]</li>
	 * <li>(3,3,7) = [11,15]</li>
	 * <li>(3,-3,2) = []</li>
	 * </ul>
	 */
	public static Operation getByValueRelativeRankRange(String binName, Value value, int rank, int count, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.GET_BY_VALUE_REL_RANK_RANGE, returnType, value, rank, count, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get by index operation.
	 * Server selects list item identified by index and returns selected data specified by returnType
	 * (See {@link ListReturnType}).
	 */
	public static Operation getByIndex(String binName, int index, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.GET_BY_INDEX, returnType, index, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get by index range operation.
	 * Server selects list items starting at specified index to the end of list and returns selected
	 * data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByIndexRange(String binName, int index, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.GET_BY_INDEX_RANGE, returnType, index, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get by index range operation.
	 * Server selects "count" list items starting at specified index and returns selected data specified
	 * by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByIndexRange(String binName, int index, int count, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.GET_BY_INDEX_RANGE, returnType, index, count, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get by rank operation.
	 * Server selects list item identified by rank and returns selected data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByRank(String binName, int rank, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.GET_BY_RANK, returnType, rank, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get by rank range operation.
	 * Server selects list items starting at specified rank to the last ranked item and returns selected
	 * data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByRankRange(String binName, int rank, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.GET_BY_RANK_RANGE, returnType, rank, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get by rank range operation.
	 * Server selects "count" list items starting at specified rank and returns selected data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByRankRange(String binName, int rank, int count, int returnType, CTX... ctx) {
		byte[] bytes = Pack.pack(ListOperation.GET_BY_RANK_RANGE, returnType, rank, count, ctx);
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}
}

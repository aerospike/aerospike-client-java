/*
 * Copyright 2012-2018 Aerospike, Inc.
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
import com.aerospike.client.util.Packer;

/**
 * List bin operations. Create list operations used by client operate command.
 * List operations support negative indexing.  If the index is negative, the
 * resolved index starts backwards from end of list.
 * <p>
 * Index/Range examples:
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
 * If an index is out of bounds, a parameter error will be returned. If a range is partially 
 * out of bounds, the valid part of the range will be returned.
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
	private static final int REMOVE_BY_INDEX = 32;
	private static final int REMOVE_BY_RANK = 34;
	private static final int REMOVE_BY_VALUE = 35;
	private static final int REMOVE_BY_VALUE_LIST = 36;
	private static final int REMOVE_BY_INDEX_RANGE = 37;
	private static final int REMOVE_BY_VALUE_INTERVAL = 38;
	private static final int REMOVE_BY_RANK_RANGE = 39;

	/**
	 * Create set list order operation.
	 * Server sets list order.  Server returns null.
	 */
	public static Operation setOrder(String binName, ListOrder order) {
		return CDT.createOperation(SET_TYPE, Operation.Type.CDT_MODIFY, binName, order.attributes);
	}

	/**
	 * Create default list append operation.
	 * Server appends value to end of list bin.
	 * Server returns list size.
	 */
	public static Operation append(String binName, Value value) {
		Packer packer = new Packer();
		packer.packRawShort(APPEND);
		packer.packArrayBegin(1);
		value.pack(packer);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(packer.toByteArray()));
	}
	
	/**
	 * Create list append operation with policy.
	 * Server appends value to list bin.
	 * Server returns list size.
	 */
	public static Operation append(ListPolicy policy, String binName, Value value) {
		Packer packer = new Packer();
		packer.packRawShort(APPEND);
		packer.packArrayBegin(3);
		value.pack(packer);
		packer.packInt(policy.attributes);
		packer.packInt(policy.flags);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(packer.toByteArray()));
	}

	/**
	 * Create default list append items operation.
	 * Server appends each input list item to end of list bin.
	 * Server returns list size.
	 */
	public static Operation appendItems(String binName, List<Value> list) {
		Packer packer = new Packer();
		packer.packRawShort(APPEND_ITEMS);
		packer.packArrayBegin(1);
		packer.packValueList(list);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(packer.toByteArray()));
	}

	/**
	 * Create list append items operation with policy.
	 * Server appends each input list item to list bin.
	 * Server returns list size.
	 */
	public static Operation appendItems(ListPolicy policy, String binName, List<Value> list) {
		Packer packer = new Packer();
		packer.packRawShort(APPEND_ITEMS);
		packer.packArrayBegin(3);
		packer.packValueList(list);
		packer.packInt(policy.attributes);
		packer.packInt(policy.flags);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(packer.toByteArray()));
	}

	/**
	 * Create default list insert operation.
	 * Server inserts value to specified index of list bin.
	 * Server returns list size.
	 */
	public static Operation insert(String binName, int index, Value value) {
		return CDT.createOperation(INSERT, Operation.Type.CDT_MODIFY, binName, index, value);
	}

	/**
	 * Create list insert operation with policy.
	 * Server inserts value to specified index of list bin.
	 * Server returns list size.
	 */
	public static Operation insert(ListPolicy policy, String binName, int index, Value value) {
		return CDT.createOperation(INSERT, Operation.Type.CDT_MODIFY, binName, index, value, policy.flags);
	}

	/**
	 * Create default list insert items operation.
	 * Server inserts each input list item starting at specified index of list bin. 
	 * Server returns list size.
	 */
	public static Operation insertItems(String binName, int index, List<Value> list) {
		Packer packer = new Packer();
		packer.packRawShort(INSERT_ITEMS);
		packer.packArrayBegin(2);
		packer.packInt(index);
		packer.packValueList(list);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(packer.toByteArray()));
	}

	/**
	 * Create list insert items operation with policy.
	 * Server inserts each input list item starting at specified index of list bin. 
	 * Server returns list size.
	 */
	public static Operation insertItems(ListPolicy policy, String binName, int index, List<Value> list) {
		Packer packer = new Packer();
		packer.packRawShort(INSERT_ITEMS);
		packer.packArrayBegin(3);
		packer.packInt(index);
		packer.packValueList(list);
		packer.packInt(policy.flags);
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(packer.toByteArray()));
	}

	/**
	 * Create default list increment operation.
	 * Server increments list[index] by 1.
	 * Server returns list[index] after incrementing.
	 */
	public static Operation increment(String binName, int index) {
		return CDT.createOperation(INCREMENT, Operation.Type.CDT_MODIFY, binName, index);
	}

	/**
	 * Create list increment operation with policy.
	 * Server increments list[index] by 1.
	 * Server returns list[index] after incrementing.
	 */
	public static Operation increment(ListPolicy policy, String binName, int index) {
		return CDT.createOperation(INCREMENT, Operation.Type.CDT_MODIFY, binName, index, Value.get(1), policy.attributes, policy.flags);
	}

	/**
	 * Create default list increment operation.
	 * Server increments list[index] by value.
	 * Value should be integer(IntegerValue, LongValue) or double(DoubleValue, FloatValue).
	 * Server returns list[index] after incrementing.
	 */
	public static Operation increment(String binName, int index, Value value) {
		return CDT.createOperation(INCREMENT, Operation.Type.CDT_MODIFY, binName, index, value);
	}

	/**
	 * Create list increment operation.
	 * Server increments list[index] by value.
	 * Value should be integer(IntegerValue, LongValue) or double(DoubleValue, FloatValue).
	 * Server returns list[index] after incrementing.
	 */
	public static Operation increment(ListPolicy policy, String binName, int index, Value value) {
		return CDT.createOperation(INCREMENT, Operation.Type.CDT_MODIFY, binName, index, value, policy.attributes, policy.flags);
	}

	/**
	 * Create list pop operation.
	 * Server returns item at specified index and removes item from list bin.
	 */
	public static Operation pop(String binName, int index) {
		return CDT.createOperation(POP, Operation.Type.CDT_MODIFY, binName, index);
	}
	
	/**
	 * Create list pop range operation.
	 * Server returns "count" items starting at specified index and removes items from list bin.
	 */
	public static Operation popRange(String binName, int index, int count) {
		return CDT.createOperation(POP_RANGE, Operation.Type.CDT_MODIFY, binName, index, count);
	}

	/**
	 * Create list pop range operation.
	 * Server returns items starting at specified index to the end of list and removes those items
	 * from list bin.
	 */
	public static Operation popRange(String binName, int index) {
		return CDT.createOperation(POP_RANGE, Operation.Type.CDT_MODIFY, binName, index);
	}

	/**
	 * Create list remove operation.
	 * Server removes item at specified index from list bin.
	 * Server returns number of items removed.
	 */
	public static Operation remove(String binName, int index) {
		return CDT.createOperation(REMOVE, Operation.Type.CDT_MODIFY, binName, index);
	}

	/**
	 * Create list remove range operation.
	 * Server removes "count" items starting at specified index from list bin.
	 * Server returns number of items removed.
	 */
	public static Operation removeRange(String binName, int index, int count) {
		return CDT.createOperation(REMOVE_RANGE, Operation.Type.CDT_MODIFY, binName, index, count);
	}
	
	/**
	 * Create list remove range operation.
	 * Server removes items starting at specified index to the end of list.
	 * Server returns number of items removed.
	 */
	public static Operation removeRange(String binName, int index) {
		return CDT.createOperation(REMOVE_RANGE, Operation.Type.CDT_MODIFY, binName, index);
	}

	/**
	 * Create list set operation.
	 * Server sets item value at specified index in list bin.
	 * Server does not return a result by default.
	 */
	public static Operation set(String binName, int index, Value value) {
		return CDT.createOperation(SET, Operation.Type.CDT_MODIFY, binName, index, value);
	}

	/**
	 * Create list set operation with policy.
	 * Server sets item value at specified index in list bin.
	 * Server does not return a result by default.
	 */
	public static Operation set(ListPolicy policy, String binName, int index, Value value) {
		return CDT.createOperation(SET, Operation.Type.CDT_MODIFY, binName, index, value, policy.flags);
	}

	/**
	 * Create list trim operation.
	 * Server removes items in list bin that do not fall into range specified by index 
	 * and count range.  If the range is out of bounds, then all items will be removed.
	 * Server returns list size after trim.
	 */
	public static Operation trim(String binName, int index, int count) {
		return CDT.createOperation(TRIM, Operation.Type.CDT_MODIFY, binName, index, count);
	}

	/**
	 * Create list clear operation.
	 * Server removes all items in list bin.
	 * Server does not return a result by default.
	 */
	public static Operation clear(String binName) {
		return CDT.createOperation(CLEAR, Operation.Type.CDT_MODIFY, binName);
	}

	/**
	 * Create list sort operation.
	 * Server sorts list according to sortFlags.
	 * Server does not return a result by default.
	 * 
	 * @param binName	server bin name
	 * @param sortFlags sort flags. See {@link ListSortFlags}.
	 */
	public static Operation sort(String binName, int sortFlags) {
		return CDT.createOperation(SORT, Operation.Type.CDT_MODIFY, binName, sortFlags);
	}
	
	/**
	 * Create list remove operation.
	 * Server removes list items identified by value and returns removed data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByValue(String binName, Value value, int returnType) {
		return CDT.createOperation(REMOVE_BY_VALUE, Operation.Type.CDT_MODIFY, binName, returnType, value);
	}
	
	/**
	 * Create list remove operation.
	 * Server removes list items identified by values and returns removed data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByValueList(String binName, List<Value> values, int returnType) {
		return CDT.createOperation(REMOVE_BY_VALUE_LIST, Operation.Type.CDT_MODIFY, binName, returnType, values);
	}

	/**
	 * Create list remove operation.
	 * Server removes list items identified by value range (valueBegin inclusive, valueEnd exclusive).
	 * If valueBegin is null, the range is less than valueEnd.
	 * If valueEnd is null, the range is greater than equal to valueBegin. 
	 * <p>
	 * Server returns removed data specified by returnType (See {@link ListReturnType}). 
	 */
	public static Operation removeByValueRange(String binName, Value valueBegin, Value valueEnd, int returnType) {
		return CDT.createRangeOperation(REMOVE_BY_VALUE_INTERVAL, Operation.Type.CDT_MODIFY, binName, valueBegin, valueEnd, returnType);
	}

	/**
	 * Create list remove operation.
	 * Server removes list item identified by index and returns removed data specified by returnType (See {@link ListReturnType}). 
	 */
	public static Operation removeByIndex(String binName, int index, int returnType) {
		return CDT.createOperation(REMOVE_BY_INDEX, Operation.Type.CDT_MODIFY, binName, returnType, index);
	}
	
	/**
	 * Create list remove operation.
	 * Server removes list items starting at specified index to the end of list and returns removed
	 * data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByIndexRange(String binName, int index, int returnType) {
		return CDT.createOperation(REMOVE_BY_INDEX_RANGE, Operation.Type.CDT_MODIFY, binName, returnType, index);
	}

	/**
	 * Create list remove operation.
	 * Server removes "count" list items starting at specified index and returns removed data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByIndexRange(String binName, int index, int count, int returnType) {
		return CDT.createOperation(REMOVE_BY_INDEX_RANGE, Operation.Type.CDT_MODIFY, binName, returnType, index, count);
	}	

	/**
	 * Create list remove operation.
	 * Server removes list item identified by rank and returns removed data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByRank(String binName, int rank, int returnType) {
		return CDT.createOperation(REMOVE_BY_RANK, Operation.Type.CDT_MODIFY, binName, returnType, rank);
	}

	/**
	 * Create list remove operation.
	 * Server removes list items starting at specified rank to the last ranked item and returns removed
	 * data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByRankRange(String binName, int rank, int returnType) {
		return CDT.createOperation(REMOVE_BY_RANK_RANGE, Operation.Type.CDT_MODIFY, binName, returnType, rank);
	}

	/**
	 * Create list remove operation.
	 * Server removes "count" list items starting at specified rank and returns removed data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation removeByRankRange(String binName, int rank, int count, int returnType) {
		return CDT.createOperation(REMOVE_BY_RANK_RANGE, Operation.Type.CDT_MODIFY, binName, returnType, rank, count);
	}	

	/**
	 * Create list size operation.
	 * Server returns size of list.
	 */
	public static Operation size(String binName) {
		return CDT.createOperation(SIZE, Operation.Type.CDT_READ, binName);
	}

	/**
	 * Create list get operation.
	 * Server returns item at specified index in list bin.
	 */
	public static Operation get(String binName, int index) {
		return CDT.createOperation(GET, Operation.Type.CDT_READ, binName, index);
	}

	/**
	 * Create list get range operation.
	 * Server returns "count" items starting at specified index in list bin.
	 */
	public static Operation getRange(String binName, int index, int count) {
		return CDT.createOperation(GET_RANGE, Operation.Type.CDT_READ, binName, index, count);
	}
	
	/**
	 * Create list get range operation.
	 * Server returns items starting at index to the end of list.
	 */
	public static Operation getRange(String binName, int index) {
		return CDT.createOperation(GET_RANGE, Operation.Type.CDT_READ, binName, index);
	}

	/**
	 * Create list get by value operation.
	 * Server selects list items identified by value and returns selected data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByValue(String binName, Value value, int returnType) {
		return CDT.createOperation(GET_BY_VALUE, Operation.Type.CDT_READ, binName, returnType, value);
	}

	/**
	 * Create list get by value range operation.
	 * Server selects list items identified by value range (valueBegin inclusive, valueEnd exclusive)
	 * If valueBegin is null, the range is less than valueEnd.
	 * If valueEnd is null, the range is greater than equal to valueBegin. 
	 * <p>
	 * Server returns selected data specified by returnType (See {@link ListReturnType}). 
	 */
	public static Operation getByValueRange(String binName, Value valueBegin, Value valueEnd, int returnType) {
		return CDT.createRangeOperation(GET_BY_VALUE_INTERVAL, Operation.Type.CDT_READ, binName, valueBegin, valueEnd, returnType);
	}

	/**
	 * Create list get by value list operation.
	 * Server selects list items identified by values and returns selected data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByValueList(String binName, List<Value> values, int returnType) {
		return CDT.createOperation(GET_BY_VALUE_LIST, Operation.Type.CDT_READ, binName, returnType, values);
	}

	/**
	 * Create list get by index operation.
	 * Server selects list item identified by index and returns selected data specified by returnType
	 * (See {@link ListReturnType}). 
	 */
	public static Operation getByIndex(String binName, int index, int returnType) {
		return CDT.createOperation(GET_BY_INDEX, Operation.Type.CDT_READ, binName, returnType, index);
	}

	/**
	 * Create list get by index range operation.
	 * Server selects list items starting at specified index to the end of list and returns selected
	 * data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByIndexRange(String binName, int index, int returnType) {
		return CDT.createOperation(GET_BY_INDEX_RANGE, Operation.Type.CDT_READ, binName, returnType, index);
	}

	/**
	 * Create list get by index range operation.
	 * Server selects "count" list items starting at specified index and returns selected data specified
	 * by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByIndexRange(String binName, int index, int count, int returnType) {
		return CDT.createOperation(GET_BY_INDEX_RANGE, Operation.Type.CDT_READ, binName, returnType, index, count);
	}

	/**
	 * Create list get by rank operation.
	 * Server selects list item identified by rank and returns selected data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByRank(String binName, int rank, int returnType) {
		return CDT.createOperation(GET_BY_RANK, Operation.Type.CDT_READ, binName, returnType, rank);
	}

	/**
	 * Create list get by rank range operation.
	 * Server selects list items starting at specified rank to the last ranked item and returns selected
	 * data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByRankRange(String binName, int rank, int returnType) {
		return CDT.createOperation(GET_BY_RANK_RANGE, Operation.Type.CDT_READ, binName, returnType, rank);
	}

	/**
	 * Create list get by rank range operation.
	 * Server selects "count" list items starting at specified rank and returns selected data specified by returnType (See {@link ListReturnType}).
	 */
	public static Operation getByRankRange(String binName, int rank, int count, int returnType) {
		return CDT.createOperation(GET_BY_RANK_RANGE, Operation.Type.CDT_READ, binName, returnType, rank, count);
	}	
}

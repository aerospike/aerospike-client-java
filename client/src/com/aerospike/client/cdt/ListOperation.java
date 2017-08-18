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
	private static final int SIZE = 16;
	private static final int GET = 17;
	private static final int GET_RANGE = 18;
	
	/**
	 * Create list append operation.
	 * Server appends value to end of list bin.
	 * Server returns list size.
	 */
	public static Operation append(String binName, Value value) {
		Packer packer = new Packer();
		packer.packRawShort(APPEND);
		packer.packArrayBegin(1);
		value.pack(packer);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}
	
	/**
	 * Create list append items operation.
	 * Server appends each input list item to end of list bin.
	 * Server returns list size.
	 */
	public static Operation appendItems(String binName, List<Value> list) {
		Packer packer = new Packer();
		packer.packRawShort(APPEND_ITEMS);
		packer.packArrayBegin(1);
		packer.packValueList(list);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list insert operation.
	 * Server inserts value to specified index of list bin.
	 * Server returns list size.
	 */
	public static Operation insert(String binName, int index, Value value) {
		Packer packer = new Packer();
		packer.packRawShort(INSERT);
		packer.packArrayBegin(2);
		packer.packInt(index);
		value.pack(packer);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list insert items operation.
	 * Server inserts each input list item starting at specified index of list bin. 
	 * Server returns list size.
	 */
	public static Operation insertItems(String binName, int index, List<Value> list) {
		Packer packer = new Packer();
		packer.packRawShort(INSERT_ITEMS);
		packer.packArrayBegin(2);
		packer.packInt(index);
		packer.packValueList(list);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list pop operation.
	 * Server returns item at specified index and removes item from list bin.
	 */
	public static Operation pop(String binName, int index) {
		Packer packer = new Packer();
		packer.packRawShort(POP);
		packer.packArrayBegin(1);
		packer.packInt(index);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}
	
	/**
	 * Create list pop range operation.
	 * Server returns "count" items starting at specified index and removes items from list bin.
	 */
	public static Operation popRange(String binName, int index, int count) {
		Packer packer = new Packer();
		packer.packRawShort(POP_RANGE);
		packer.packArrayBegin(2);
		packer.packInt(index);
		packer.packInt(count);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list pop range operation.
	 * Server returns items starting at specified index to the end of list and removes those items
	 * from list bin.
	 */
	public static Operation popRange(String binName, int index) {
		Packer packer = new Packer();
		packer.packRawShort(POP_RANGE);
		packer.packArrayBegin(1);
		packer.packInt(index);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove operation.
	 * Server removes item at specified index from list bin.
	 * Server returns number of items removed.
	 */
	public static Operation remove(String binName, int index) {
		Packer packer = new Packer();
		packer.packRawShort(REMOVE);
		packer.packArrayBegin(1);
		packer.packInt(index);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list remove range operation.
	 * Server removes "count" items starting at specified index from list bin.
	 * Server returns number of items removed.
	 */
	public static Operation removeRange(String binName, int index, int count) {
		Packer packer = new Packer();
		packer.packRawShort(REMOVE_RANGE);
		packer.packArrayBegin(2);
		packer.packInt(index);
		packer.packInt(count);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}
	
	/**
	 * Create list remove range operation.
	 * Server removes items starting at specified index to the end of list.
	 * Server returns number of items removed.
	 */
	public static Operation removeRange(String binName, int index) {
		Packer packer = new Packer();
		packer.packRawShort(REMOVE_RANGE);
		packer.packArrayBegin(1);
		packer.packInt(index);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list set operation.
	 * Server sets item value at specified index in list bin.
	 * Server does not return a result by default.
	 */
	public static Operation set(String binName, int index, Value value) {
		Packer packer = new Packer();
		packer.packRawShort(SET);
		packer.packArrayBegin(2);
		packer.packInt(index);
		value.pack(packer);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list trim operation.
	 * Server removes items in list bin that do not fall into range specified by index 
	 * and count range.  If the range is out of bounds, then all items will be removed.
	 * Server returns list size after trim.
	 */
	public static Operation trim(String binName, int index, int count) {
		Packer packer = new Packer();
		packer.packRawShort(TRIM);
		packer.packArrayBegin(2);
		packer.packInt(index);
		packer.packInt(count);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list clear operation.
	 * Server removes all items in list bin.
	 * Server does not return a result by default.
	 */
	public static Operation clear(String binName) {
		Packer packer = new Packer();
		packer.packRawShort(CLEAR);
		//packer.packArrayBegin(0);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list increment operation.
	 * Server increments list[index] by 1.
	 * Server returns list[index] after incrementing.
	 */
	public static Operation increment(String binName, int index) {
		Packer packer = new Packer();
		packer.packRawShort(INCREMENT);
		packer.packArrayBegin(1);
		packer.packInt(index);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list increment operation.
	 * Server increments list[index] by value.
	 * Value should be integer(IntegerValue, LongValue) or double(DoubleValue, FloatValue).
	 * Server returns list[index] after incrementing.
	 */
	public static Operation increment(String binName, int index, Value value) {
		Packer packer = new Packer();
		packer.packRawShort(INCREMENT);
		packer.packArrayBegin(2);
		packer.packInt(index);
		value.pack(packer);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_MODIFY, binName, Value.get(bytes));
	}

	/**
	 * Create list size operation.
	 * Server returns size of list.
	 */
	public static Operation size(String binName) {
		Packer packer = new Packer();
		packer.packRawShort(SIZE);
		//packer.packArrayBegin(0);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get operation.
	 * Server returns item at specified index in list bin.
	 */
	public static Operation get(String binName, int index) {
		Packer packer = new Packer();
		packer.packRawShort(GET);
		packer.packArrayBegin(1);
		packer.packInt(index);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}

	/**
	 * Create list get range operation.
	 * Server returns "count" items starting at specified index in list bin.
	 */
	public static Operation getRange(String binName, int index, int count) {
		Packer packer = new Packer();
		packer.packRawShort(GET_RANGE);
		packer.packArrayBegin(2);
		packer.packInt(index);
		packer.packInt(count);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}
	
	/**
	 * Create list get range operation.
	 * Server returns items starting at index to the end of list.
	 */
	public static Operation getRange(String binName, int index) {
		Packer packer = new Packer();
		packer.packRawShort(GET_RANGE);
		packer.packArrayBegin(1);
		packer.packInt(index);
		byte[] bytes = packer.toByteArray();
		return new Operation(Operation.Type.CDT_READ, binName, Value.get(bytes));
	}
}

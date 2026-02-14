/*
 * Copyright 2012-2024 Aerospike, Inc.
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
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.ListSortFlags;
import com.aerospike.client.util.Pack;
import com.aerospike.client.util.Packer;

/**
 * List expression generator for filters and operate; see {@link Exp}.
 * <p>
 * The bin argument can be a bin reference or an expression that returns a list. List modify
 * expressions return the bin's value (a list, or a map when the list is nested in a map). Negative
 * indexing is supported; index out of bounds returns a parameter error; a range partially out of
 * bounds returns the valid part. Use optional {@link com.aerospike.client.cdt.CTX} for nested
 * lists/maps.
 * <p>
 * Index (item offset; negative = from end):
 * <ul>
 * <li>Index 0: first item; 4: fifth item; -1: last item; -3: third to last.</li>
 * <li>Index 1 Count 2: second and third items; Index -3 Count 3: last three items.</li>
 * <li>Index -5 Count 4: range from fifth-to-last to second-to-last inclusive.</li>
 * </ul>
 * <p>Filter by list size, read by index/range, and nested CTX (e.g. size of last list).</p>
 * <pre>{@code
 * IAerospikeClient client = new AerospikeClient("localhost", 3000);
 * Key key = new Key("ns", "set", "mykey");
 *
 * // Filter by list size (e.g. list at CTX listIndex 4 has size 6)
 * Policy policy = new Policy();
 * policy.filterExp = Exp.build(Exp.eq(ListExp.size(Exp.listBin("A"), CTX.listIndex(4)), Exp.val(6)));
 * Record rec = client.get(policy, key);
 *
 * // Index: first (0), last (-1), range second+third (1, count 2), last three (-3, count 3), -5 count 4
 * Exp byIndex0 = ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.val(0), Exp.listBin("bin"));
 * Exp byIndexLast = ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.val(-1), Exp.listBin("bin"));
 * Exp byIndexRange = ListExp.getByIndexRange(ListReturnType.VALUE, Exp.val(1), Exp.val(2), Exp.listBin("bin"));
 * Exp lastThree = ListExp.getByIndexRange(ListReturnType.VALUE, Exp.val(-3), Exp.val(3), Exp.listBin("bin"));
 * Exp rangeFromFifthToLast = ListExp.getByIndexRange(ListReturnType.VALUE, Exp.val(-5), Exp.val(4), Exp.listBin("bin"));
 *
 * // List modify (returns bin value)
 * Expression exp = Exp.build(ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.val(0), Exp.listBin("bin")));
 * Record r = client.operate(null, key, ExpOperation.read("first", exp, ExpReadFlags.DEFAULT));
 *
 * // Nested: size of last list (bin = [[7,9,5],[1,2,3],[6,5,4,1]] -> result 4)
 * Exp sizeOfLastList = ListExp.size(Exp.listBin("bin"), CTX.listIndex(-1));
 * }</pre>
 *
 * @see Exp
 * @see MapExp
 * @see com.aerospike.client.cdt.CTX
 * @see com.aerospike.client.cdt.ListReturnType
 */
public final class ListExp {
	private static final int MODULE = 0;
	private static final int APPEND = 1;
	private static final int APPEND_ITEMS = 2;
	private static final int INSERT = 3;
	private static final int INSERT_ITEMS = 4;
	private static final int SET = 9;
	private static final int CLEAR = 11;
	private static final int INCREMENT = 12;
	private static final int SORT = 13;
	private static final int SIZE = 16;
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
	 * Create expression that appends value to end of list.
	 */
	public static Exp append(ListPolicy policy, Exp value, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(APPEND, value, policy.attributes, policy.flags, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that appends list items to end of list.
	 */
	public static Exp appendItems(ListPolicy policy, Exp list, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(APPEND_ITEMS, list, policy.attributes, policy.flags, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that inserts value to specified index of list.
	 */
	public static Exp insert(ListPolicy policy, Exp index, Exp value, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(INSERT, index, value, policy.flags, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that inserts each input list item starting at specified index of list.
	 */
	public static Exp insertItems(ListPolicy policy, Exp index, Exp list, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(INSERT_ITEMS, index, list, policy.flags, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that increments list[index] by value.
	 * Value expression should resolve to a number.
	 */
	public static Exp increment(ListPolicy policy, Exp index, Exp value, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(INCREMENT, index, value, policy.attributes, policy.flags, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that sets item value at specified index in list.
	 */
	public static Exp set(ListPolicy policy, Exp index, Exp value, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(SET, index, value, policy.flags, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes all items in list.
	 */
	public static Exp clear(Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(CLEAR, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that sorts list according to sortFlags.
	 *
	 * @param sortFlags 	sort flags. See {@link ListSortFlags}.
	 * @param bin			bin or list value expression
	 * @param ctx			optional context path for nested CDT
	 */
	public static Exp sort(int sortFlags, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(SORT, sortFlags, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes list items identified by value.
	 * Valid returnType values are {@link com.aerospike.client.cdt.ListReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.ListReturnType#INVERTED}.
	 */
	public static Exp removeByValue(int returnType, Exp value, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_VALUE, returnType, value, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes list items identified by values.
	 * Valid returnType values are {@link com.aerospike.client.cdt.ListReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.ListReturnType#INVERTED}.
	 */
	public static Exp removeByValueList(int returnType, Exp values, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_VALUE_LIST, returnType, values, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes list items identified by value range (valueBegin inclusive, valueEnd exclusive).
	 * If valueBegin is null, the range is less than valueEnd. If valueEnd is null, the range is
	 * greater than equal to valueBegin.
	 * Valid returnType values are {@link com.aerospike.client.cdt.ListReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.ListReturnType#INVERTED}.
	 */
	public static Exp removeByValueRange(int returnType, Exp valueBegin, Exp valueEnd, Exp bin, CTX... ctx) {
		byte[] bytes = ListExp.packRangeOperation(REMOVE_BY_VALUE_INTERVAL, returnType, valueBegin, valueEnd, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes list items nearest to value and greater by relative rank.
	 * Valid returnType values are {@link com.aerospike.client.cdt.ListReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.ListReturnType#INVERTED}.
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
	public static Exp removeByValueRelativeRankRange(int returnType, Exp value, Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_VALUE_REL_RANK_RANGE, returnType, value, rank, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes list items nearest to value and greater by relative rank with a count limit.
	 * Valid returnType values are {@link com.aerospike.client.cdt.ListReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.ListReturnType#INVERTED}.
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
	public static Exp removeByValueRelativeRankRange(int returnType, Exp value, Exp rank, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_VALUE_REL_RANK_RANGE, returnType, value, rank, count, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes list item identified by index.
	 */
	public static Exp removeByIndex(Exp index, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_INDEX, ListReturnType.NONE, index, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes list items starting at specified index to the end of list.
	 * Valid returnType values are {@link com.aerospike.client.cdt.ListReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.ListReturnType#INVERTED}.
	 */
	public static Exp removeByIndexRange(int returnType, Exp index, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_INDEX_RANGE, returnType, index, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes "count" list items starting at specified index.
	 * Valid returnType values are {@link com.aerospike.client.cdt.ListReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.ListReturnType#INVERTED}.
	 */
	public static Exp removeByIndexRange(int returnType, Exp index, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_INDEX_RANGE, returnType, index, count, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes list item identified by rank.
	 */
	public static Exp removeByRank(Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_RANK, ListReturnType.NONE, rank, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes list items starting at specified rank to the last ranked item.
	 * Valid returnType values are {@link com.aerospike.client.cdt.ListReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.ListReturnType#INVERTED}.
	 */
	public static Exp removeByRankRange(int returnType, Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_RANK_RANGE, returnType, rank, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that removes "count" list items starting at specified rank.
	 * Valid returnType values are {@link com.aerospike.client.cdt.ListReturnType#NONE} or
	 * {@link com.aerospike.client.cdt.ListReturnType#INVERTED}.
	 */
	public static Exp removeByRankRange(int returnType, Exp rank, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(REMOVE_BY_RANK_RANGE, returnType, rank, count, ctx);
		return addWrite(bin, bytes, ctx);
	}

	/**
	 * Create expression that returns list size.
	 * <p>Example for this expression.</p>
	 * <pre>{@code
	 * // List bin "a" size > 7
	 * Exp.gt(ListExp.size(Exp.listBin("a")), Exp.val(7))
	 * }</pre>
	 */
	public static Exp size(Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(SIZE, ctx);
		return addRead(bin, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that selects list items identified by value and returns selected
	 * data specified by returnType.
	 * <p>Example for this expression.</p>
	 * <pre>{@code
	 * // List bin "a" contains at least one item == "abc"
	 * ListExp.getByValue(ListReturnType.EXISTS, Exp.val("abc"), Exp.listBin("a"))
	 * }</pre>
	 *
	 * @param returnType	metadata attributes to return. See {@link ListReturnType}
	 * @param value			search expression
	 * @param bin			list bin or list value expression
	 * @param ctx			optional context path for nested CDT
	 */
	public static Exp getByValue(int returnType, Exp value, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_VALUE, returnType, value, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects list items identified by value range and returns selected data
	 * specified by returnType.
	 * <p>Example for this expression.</p>
	 * <pre>{@code
	 * // List bin "a" items >= 10 && items < 20
	 * ListExp.getByValueRange(ListReturnType.VALUE, Exp.val(10), Exp.val(20), Exp.listBin("a"))
	 * }</pre>
	 *
	 * @param returnType	metadata attributes to return. See {@link ListReturnType}
	 * @param valueBegin	begin expression inclusive. If null, range is less than valueEnd.
	 * @param valueEnd		end expression exclusive. If null, range is greater than equal to valueBegin.
	 * @param bin			bin or list value expression
	 * @param ctx			optional context path for nested CDT
	 */
	public static Exp getByValueRange(int returnType, Exp valueBegin, Exp valueEnd, Exp bin, CTX... ctx) {
		byte[] bytes = ListExp.packRangeOperation(GET_BY_VALUE_INTERVAL, returnType, valueBegin, valueEnd, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects list items identified by values and returns selected data
	 * specified by returnType.
	 */
	public static Exp getByValueList(int returnType, Exp values, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_VALUE_LIST, returnType, values, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects list items nearest to value and greater by relative rank
	 * and returns selected data specified by returnType (See {@link ListReturnType}).
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
	public static Exp getByValueRelativeRankRange(int returnType, Exp value, Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_VALUE_REL_RANK_RANGE, returnType, value, rank, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects list items nearest to value and greater by relative rank with a count limit
	 * and returns selected data specified by returnType (See {@link ListReturnType}).
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
	public static Exp getByValueRelativeRankRange(int returnType, Exp value, Exp rank, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_VALUE_REL_RANK_RANGE, returnType, value, rank, count, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects list item identified by index and returns
	 * selected data specified by returnType.
	 * <p>Example for this expression.</p>
	 * <pre>{@code
	 * // a[3] == 5
	 * Exp.eq(
	 *   ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.val(3), Exp.listBin("a")),
	 *   Exp.val(5));
	 * }</pre>
	 *
	 * @param returnType	metadata attributes to return. See {@link ListReturnType}
	 * @param valueType		expected type of return value
	 * @param index			list index expression
	 * @param bin			list bin or list value expression
	 * @param ctx			optional context path for nested CDT
	 */
	public static Exp getByIndex(int returnType, Exp.Type valueType, Exp index, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_INDEX, returnType, index, ctx);
		return addRead(bin, bytes, valueType);
	}

	/**
	 * Create expression that selects list items starting at specified index to the end of list
	 * and returns selected data specified by returnType (See {@link ListReturnType}).
	 */
	public static Exp getByIndexRange(int returnType, Exp index, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_INDEX_RANGE, returnType, index, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects "count" list items starting at specified index
	 * and returns selected data specified by returnType (See {@link ListReturnType}).
	 */
	public static Exp getByIndexRange(int returnType, Exp index, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_INDEX_RANGE, returnType, index, count, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects list item identified by rank and returns selected
	 * data specified by returnType.
	 * <p>Example for this expression.</p>
	 * <pre>{@code
	 * // Player with lowest score.
	 * ListExp.getByRank(ListReturnType.VALUE, Type.STRING, Exp.val(0), Exp.listBin("a"))
	 * }</pre>
	 *
	 * @param returnType	metadata attributes to return. See {@link ListReturnType}
	 * @param valueType		expected type of return value
	 * @param rank 			rank expression
	 * @param bin			list bin or list value expression
	 * @param ctx			optional context path for nested CDT
	 */
	public static Exp getByRank(int returnType, Exp.Type valueType, Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_RANK, returnType, rank, ctx);
		return addRead(bin, bytes, valueType);
	}

	/**
	 * Create expression that selects list items starting at specified rank to the last ranked item
	 * and returns selected data specified by returnType (See {@link ListReturnType}).
	 */
	public static Exp getByRankRange(int returnType, Exp rank, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_RANK_RANGE, returnType, rank, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	/**
	 * Create expression that selects "count" list items starting at specified rank and returns
	 * selected data specified by returnType (See {@link ListReturnType}).
	 */
	public static Exp getByRankRange(int returnType, Exp rank, Exp count, Exp bin, CTX... ctx) {
		byte[] bytes = Pack.pack(GET_BY_RANK_RANGE, returnType, rank, count, ctx);
		return addRead(bin, bytes, getValueType(returnType));
	}

	private static Exp addWrite(Exp bin, byte[] bytes, CTX[] ctx) {
		int retType;

		if (ctx == null || ctx.length == 0) {
			retType = Exp.Type.LIST.code;
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
		int t = returnType & ~ListReturnType.INVERTED;

		switch (t) {
		case ListReturnType.INDEX:
		case ListReturnType.REVERSE_INDEX:
		case ListReturnType.RANK:
		case ListReturnType.REVERSE_RANK:
			// This method only called from expressions that can return multiple integers (ie list).
			return Exp.Type.LIST;

		case ListReturnType.COUNT:
			return Exp.Type.INT;

		case ListReturnType.VALUE:
			// This method only called from expressions that can return multiple objects (ie list).
			return Exp.Type.LIST;

		case ListReturnType.EXISTS:
			return Exp.Type.BOOL;

		default:
		case ListReturnType.NONE:
			throw new AerospikeException("Invalid ListReturnType: " + returnType);
		}
	}

	protected static byte[] packRangeOperation(int command, int returnType, Exp begin, Exp end, CTX[] ctx) {
		Packer packer = new Packer();

		// First pass calculates buffer size.
		// Second pass writes to buffer.
		for (int i = 0; i < 2; i++) {
			Pack.init(packer, ctx);
			packer.packArrayBegin((end != null) ? 4 : 3);
			packer.packInt(command);
			packer.packInt(returnType);

			if (begin != null) {
				begin.pack(packer);
			} else {
				packer.packNil();
			}

			if (end != null) {
				end.pack(packer);
			}

			if (i == 0) {
				packer.createBuffer();
			}
		}

		return packer.getBuffer();
	}
}

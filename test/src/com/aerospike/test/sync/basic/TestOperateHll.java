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
package com.aerospike.test.sync.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.Value.HLLValue;
import com.aerospike.client.Value.StringValue;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.client.operation.HLLWriteFlags;
import com.aerospike.test.sync.TestSync;

public class TestOperateHll extends TestSync {
	private static boolean debug = false;

	private static final String binName = "ophbin";
	private static final Key key = new Key(args.namespace, args.set, "ophkey");
	private static final Key[] keys = new Key[] {
			new Key(args.namespace, args.set, "ophkey0"),
			new Key(args.namespace, args.set, "ophkey1"),
			new Key(args.namespace, args.set, "ophkey2")};
	private static final int n_entries = 1 << 18;

	private static final int minIndexBits = 4;
	private static final int maxIndexBits = 16;
	private static final int minMinhashBits = 4;
	private static final int maxMinhashBits = 51;

	private static final ArrayList<Value> entries = new ArrayList<Value>();
	private static final ArrayList<Integer> legalIndexBits = new ArrayList<Integer>();
	private static final ArrayList<ArrayList<Integer>> legalDescriptions = new ArrayList<ArrayList<Integer>>();
	private static final ArrayList<ArrayList<Integer>> illegalDescriptions = new ArrayList<ArrayList<Integer>>();

	@BeforeClass
	public static void createData() {
		for (int i = 0; i < n_entries; i++) {
			entries.add(new StringValue("key " + i));
		}

		for (int index_bits = minIndexBits; index_bits <= maxIndexBits; index_bits += 4) {
			int combined_bits = maxMinhashBits + index_bits;
			int max_allowed_minhash_bits = maxMinhashBits;

			if (combined_bits > 64) {
				max_allowed_minhash_bits -= combined_bits - 64;
			}

			int mid_minhash_bits = (max_allowed_minhash_bits + index_bits) / 2;
			ArrayList<Integer> legal_zero = new ArrayList<Integer>();
			ArrayList<Integer> legal_min = new ArrayList<Integer>();
			ArrayList<Integer> legal_mid = new ArrayList<Integer>();
			ArrayList<Integer> legal_max = new ArrayList<Integer>();

			legalIndexBits.add(index_bits);
			legal_zero.add(index_bits);
			legal_min.add(index_bits);
			legal_mid.add(index_bits);
			legal_max.add(index_bits);

			legal_zero.add(0);
			legal_min.add(minMinhashBits);
			legal_mid.add(mid_minhash_bits);
			legal_max.add(max_allowed_minhash_bits);

			legalDescriptions.add(legal_zero);
			legalDescriptions.add(legal_min);
			legalDescriptions.add(legal_mid);
			legalDescriptions.add(legal_max);
		}

		for (int index_bits = minIndexBits - 1; index_bits <= maxIndexBits + 5; index_bits += 4) {
			if (index_bits < minIndexBits || index_bits > maxIndexBits) {
				ArrayList<Integer> illegal_zero = new ArrayList<Integer>();
				ArrayList<Integer> illegal_min = new ArrayList<Integer>();
				ArrayList<Integer> illegal_max = new ArrayList<Integer>();

				illegal_zero.add(index_bits);
				illegal_min.add(index_bits);
				illegal_max.add(index_bits);

				illegal_zero.add(0);
				illegal_min.add(minMinhashBits - 1);
				illegal_max.add(maxMinhashBits);

				illegalDescriptions.add(illegal_zero);
				illegalDescriptions.add(illegal_min);
				illegalDescriptions.add(illegal_max);
			}
			else {
				ArrayList<Integer> illegal_min = new ArrayList<Integer>();
				ArrayList<Integer> illegal_max = new ArrayList<Integer>();
				ArrayList<Integer> illegal_max1 = new ArrayList<Integer>();

				illegal_min.add(index_bits);
				illegal_max.add(index_bits);

				illegal_min.add(minMinhashBits - 1);
				illegal_max.add(maxMinhashBits + 1);

				illegalDescriptions.add(illegal_min);
				illegalDescriptions.add(illegal_max);

				if (index_bits + maxMinhashBits > 64) {
					illegal_max1.add(index_bits);
					illegal_max1.add(1 + maxMinhashBits - (64 - (index_bits + maxMinhashBits)));
					illegalDescriptions.add(illegal_max1);
				}
			}
		}
	}

	public void printDebug(String msg) {
		if (debug) {
			System.out.println("debug :: " + msg);
			System.out.flush();
		}
	}

	public void assertThrows(String msg, Key key, Class<?> eclass, int eresult, Operation... ops) {
		try {
			client.operate(null, key, ops);
			assertTrue(msg + " succeeded?", false);
		}
		catch (AerospikeException e) {
			if (eclass != e.getClass() || eresult != e.getResultCode()) {
				assertEquals(msg + " " + e.getClass() + " " + e, eclass, e.getClass());
				assertEquals(msg + " " + e.getResultCode() + " " + e, eresult, e.getResultCode());
			}
		}
	}

	public Record assertSuccess(String msg, Key key, Operation... ops) {
		Record record;

		try {
			record = client.operate(null, key, ops);
		}
		catch (Exception e) {
			assertEquals(msg + " " + e, null, e);
			return null;
		}

		assertRecordFound(key, record);

		return record;
	}

	public boolean checkBits(int index_bits, int minhash_bits) {
		return ! (index_bits < minIndexBits || index_bits > maxIndexBits ||
				(minhash_bits != 0 && minhash_bits < minMinhashBits) ||
				minhash_bits > maxMinhashBits || index_bits + minhash_bits > 64);
	}

	public double relativeCountError(int n_index_bits) {
		return 1.04 / Math.sqrt(Math.pow(2, n_index_bits));
	}

	public void assertDescription(String msg, List<?>description, int index_bits, int minhash_bits) {
		assertEquals(msg, index_bits, (long)(Long) description.get(0));
		assertEquals(msg, minhash_bits, (long)(Long) description.get(1));
	}

	public void assertInit(int index_bits, int minhash_bits, boolean should_pass) {
		String msg = "Fail - index_bits " + index_bits + " minhash_bits " + minhash_bits;
		HLLPolicy p = HLLPolicy.Default;
		Operation[] ops = new Operation[] {
				HLLOperation.init(p, binName, index_bits, minhash_bits),
				HLLOperation.getCount(binName),
				HLLOperation.refreshCount(binName),
				HLLOperation.describe(binName)};

		if (! should_pass) {
			assertThrows(msg, key, AerospikeException.class, ResultCode.PARAMETER_ERROR, ops);
			return;
		}

		Record record = assertSuccess(msg, key, ops);
		List<?> result_list = record.getList(binName);
		long count = (Long)result_list.get(1);
		long count1 = (Long)result_list.get(2);
		List<?> description = (List<?>)result_list.get(3);

		assertDescription(msg, description, index_bits, minhash_bits);
		assertEquals(0, count);
		assertEquals(0, count1);
	}

	@Test
	public void operateHLLInit() {
		client.delete(null, key);

		for (ArrayList<Integer> desc : legalDescriptions) {
			assertInit(desc.get(0), desc.get(1), true);
		}

		for (ArrayList<Integer> desc : illegalDescriptions) {
			assertInit(desc.get(0), desc.get(1), false);
		}
	}

	@Test
	public void operateHLLFlags() {
		int index_bits = 4;

		// Keep record around win binName is removed.
		assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.init(HLLPolicy.Default, binName + "other", index_bits));

		// create_only
		HLLPolicy c = new HLLPolicy(HLLWriteFlags.CREATE_ONLY);

		assertSuccess("create_only", key, HLLOperation.init(c, binName, index_bits));
		assertThrows("create_only - error", key, AerospikeException.class, ResultCode.BIN_EXISTS_ERROR,
				HLLOperation.init(c, binName, index_bits));

		// update_only
		HLLPolicy u = new HLLPolicy(HLLWriteFlags.UPDATE_ONLY);

		assertSuccess("update_only", key, HLLOperation.init(u, binName, index_bits));
		assertSuccess("remove bin", key, Operation.put(Bin.asNull(binName)));
		assertThrows("update_only - error", key, AerospikeException.class, ResultCode.BIN_NOT_FOUND,
				HLLOperation.init(u, binName, index_bits));

		// create_only no_fail
		HLLPolicy cn = new HLLPolicy(HLLWriteFlags.CREATE_ONLY | HLLWriteFlags.NO_FAIL);

		assertSuccess("create_only nofail", key, HLLOperation.init(cn, binName, index_bits));
		assertSuccess("create_only nofail - no error", key, HLLOperation.init(cn, binName, index_bits));

		// update_only no_fail
		HLLPolicy un = new HLLPolicy(HLLWriteFlags.UPDATE_ONLY | HLLWriteFlags.NO_FAIL);

		assertSuccess("update_only nofail", key, HLLOperation.init(un, binName, index_bits));
		assertSuccess("remove bin", key, Operation.put(Bin.asNull(binName)));
		assertSuccess("update_only nofail - no error", key, HLLOperation.init(un, binName, index_bits));

		// fold
		assertSuccess("create_only", key, HLLOperation.init(c, binName, index_bits));

		HLLPolicy f = new HLLPolicy(HLLWriteFlags.ALLOW_FOLD);

		assertThrows("fold", key, AerospikeException.class, ResultCode.PARAMETER_ERROR,
				HLLOperation.init(f, binName, index_bits));
	}

	@Test
	public void badReInit() {
		HLLPolicy p = HLLPolicy.Default;

		assertSuccess("create min max", key, Operation.delete(), HLLOperation.init(p, binName, maxIndexBits, 0));
		assertThrows("create_only", key, AerospikeException.class, ResultCode.OP_NOT_APPLICABLE,
				HLLOperation.init(p, binName, -1, maxMinhashBits));
	}

	public boolean isWithinRelativeError(long expected, long estimate, double relative_error) {
		return expected * (1 - relative_error) <= estimate || estimate <= expected * (1 + relative_error);
	}

	public void assertHLLCount(String msg, int index_bits, long hll_count, long expected) {
		double count_err_6sigma = relativeCountError(index_bits) * 6;

		msg = msg + " - err " + count_err_6sigma + " count " + hll_count + " expected " + expected + " index_bits " +
				index_bits;

		printDebug(msg);
		assertTrue(msg, isWithinRelativeError(expected, hll_count, count_err_6sigma));
	}

	public void assertAddInit(int index_bits, int minhash_bits) {
		client.delete(null, key);

		String msg = "Fail - index_bits " + index_bits + " minhash_bits " + minhash_bits;
		HLLPolicy p = HLLPolicy.Default;
		Operation[] ops = new Operation[] {
				HLLOperation.add(p, binName, entries, index_bits, minhash_bits),
				HLLOperation.getCount(binName),
				HLLOperation.refreshCount(binName),
				HLLOperation.describe(binName),
				HLLOperation.add(p, binName, entries),
		};

		if (!checkBits(index_bits, minhash_bits)) {
			assertThrows(msg, key, AerospikeException.class, ResultCode.PARAMETER_ERROR, ops);
			return;
		}

		Record record = assertSuccess(msg, key, ops);
		List<?> result_list = record.getList(binName);
		long count = (Long) result_list.get(1);
		long count1 = (Long) result_list.get(2);
		List<?> description = (List<?>) result_list.get(3);
		long n_added = (Long) result_list.get(4);

		assertDescription(msg, description, index_bits, minhash_bits);
		assertHLLCount(msg, index_bits, count, entries.size());
		assertEquals(count, count1);
		assertEquals(n_added, 0);
	}

	@Test
	public void operateHLLAddInit() {
		for (ArrayList<Integer> desc : legalDescriptions) {
			assertAddInit(desc.get(0), desc.get(1));
		}
	}

	@Test
	public void operateAddFlags() {
		int index_bits = 4;

		// Keep record around win binName is removed.
		assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.init(HLLPolicy.Default, binName + "other", index_bits));

		// create_only
		HLLPolicy c = new HLLPolicy(HLLWriteFlags.CREATE_ONLY);

		assertSuccess("create_only", key, HLLOperation.add(c, binName, entries, index_bits));
		assertThrows("create_only - error", key, AerospikeException.class, ResultCode.BIN_EXISTS_ERROR,
				HLLOperation.add(c, binName, entries, index_bits));

		// update_only
		HLLPolicy u = new HLLPolicy(HLLWriteFlags.UPDATE_ONLY);

		assertThrows("update_only - error", key, AerospikeException.class, ResultCode.PARAMETER_ERROR,
				HLLOperation.add(u, binName, entries, index_bits));

		// create_only no_fail
		HLLPolicy cn = new HLLPolicy(HLLWriteFlags.CREATE_ONLY | HLLWriteFlags.NO_FAIL);

		assertSuccess("create_only nofail", key, HLLOperation.add(cn, binName, entries, index_bits));
		assertSuccess("create_only nofail - no error", key, HLLOperation.add(cn, binName, entries, index_bits));

		// fold
		assertSuccess("init", key, HLLOperation.init(HLLPolicy.Default, binName, index_bits));

		HLLPolicy f = new HLLPolicy(HLLWriteFlags.ALLOW_FOLD);

		assertThrows("fold", key, AerospikeException.class, ResultCode.PARAMETER_ERROR,
				HLLOperation.add(f, binName, entries, index_bits));
	}

	public void assertFold(List<Value> vals0, List<Value> vals1, int index_bits) {
		String msg = "Fail - index_bits " + index_bits;
		HLLPolicy p = HLLPolicy.Default;

		for (int ix = minIndexBits; ix <= index_bits; ix++) {
			if (!checkBits(index_bits, 0) || ! checkBits(ix, 0)) {
				assertTrue("Expected valid inputs: " + msg, false);
			}

			Record recorda = assertSuccess(msg, key,
					Operation.delete(),
					HLLOperation.add(p, binName, vals0, index_bits),
					HLLOperation.getCount(binName),
					HLLOperation.refreshCount(binName),
					HLLOperation.describe(binName));

			List<?> resulta_list = recorda.getList(binName);
			long counta = (Long) resulta_list.get(1);
			long counta1 = (Long) resulta_list.get(2);
			List<?> descriptiona = (List<?>) resulta_list.get(3);

			assertDescription(msg, descriptiona, index_bits, 0);
			assertHLLCount(msg, index_bits, counta, vals0.size());
			assertEquals(counta, counta1);

			Record recordb = assertSuccess(msg, key,
					HLLOperation.fold(binName, ix),
					HLLOperation.getCount(binName),
					HLLOperation.add(p, binName, vals0),
					HLLOperation.add(p, binName, vals1),
					HLLOperation.getCount(binName),
					HLLOperation.describe(binName));

			List<?> resultb_list = recordb.getList(binName);
			long countb = (Long) resultb_list.get(1);
			long n_added0 = (Long) resultb_list.get(2);
			long countb1 = (Long) resultb_list.get(4);
			List<?> descriptionb = (List<?>) resultb_list.get(5);

			assertEquals(0, n_added0);
			assertDescription(msg, descriptionb, ix, 0);
			assertHLLCount(msg, ix, countb, vals0.size());
			assertHLLCount(msg, ix, countb1, vals0.size() + vals1.size());
		}
	}

	@Test
	public void operateFold() {
		List<Value> vals0 = new ArrayList<Value>();
		List<Value> vals1 = new ArrayList<Value>();

		for (int i = 0; i < n_entries / 2; i++) {
			vals0.add(new StringValue("key " + i));
		}

		for (int i = n_entries / 2; i < n_entries; i++) {
			vals1.add(new StringValue("key " + i));
		}

		for (int index_bits = 4; index_bits < maxIndexBits; index_bits++) {
			assertFold(vals0, vals1, index_bits);
		}
	}

	@Test
	public void operateFoldExists() {
		int index_bits = 10;
		int fold_down = 4;
		int fold_up = 16;

		// Keep record around win binName is removed.
		assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.init(HLLPolicy.Default, binName + "other", index_bits),
				HLLOperation.init(HLLPolicy.Default, binName, index_bits));

		// Exists.
		assertSuccess("exists fold down", key, HLLOperation.fold(binName, fold_down));
		assertThrows("exists fold up", key, AerospikeException.class, ResultCode.OP_NOT_APPLICABLE,
				HLLOperation.fold(binName, fold_up));

		// Does not exist.
		assertSuccess("remove bin", key, Operation.put(Bin.asNull(binName)));

		assertThrows("create_only - error", key, AerospikeException.class, ResultCode.BIN_NOT_FOUND,
				HLLOperation.fold(binName, fold_down));
	}

	public void assertSetUnion(List<List<Value>> vals, int index_bits, boolean folding,
			boolean allow_folding) {
		String msg = "Fail - index_bits " + index_bits;
		HLLPolicy p = HLLPolicy.Default;
		HLLPolicy u = HLLPolicy.Default;

		if (allow_folding) {
			u = new HLLPolicy(HLLWriteFlags.ALLOW_FOLD);
		}

		long union_expected = 0;
		boolean folded = false;

		for (int i = 0; i < keys.length; i++) {
			int ix = index_bits;

			if (folding) {
				ix -= i;

				if (ix < minIndexBits) {
					ix = minIndexBits;
				}

				if (ix < index_bits) {
					folded = true;
				}
			}

			List<Value> sub_vals = vals.get(i);

			union_expected += sub_vals.size();

			Record record = assertSuccess(msg, keys[i],
					Operation.delete(),
					HLLOperation.add(p, binName, sub_vals, ix),
					HLLOperation.getCount(binName));
			List<?> result_list = record.getList(binName);
			long count = (Long) result_list.get(1);

			assertHLLCount(msg, ix, count, sub_vals.size());
		}

		ArrayList<HLLValue> hlls = new ArrayList<HLLValue>();

		for (int i = 0; i < keys.length; i++) {
			Record record = assertSuccess(msg, keys[i],
					Operation.get(binName), HLLOperation.getCount(binName));
			List<?> result_list = record.getList(binName);
			HLLValue hll = (HLLValue)result_list.get(0);

			assertNotEquals(null, hll);
			hlls.add(hll);
		}

		Operation[] ops = new Operation[] {
				Operation.delete(),
				HLLOperation.init(p, binName, index_bits),
				HLLOperation.setUnion(u, binName, hlls),
				HLLOperation.getCount(binName),
				Operation.delete(), // And recreate it to test creating empty.
				HLLOperation.setUnion(p, binName, hlls),
				HLLOperation.getCount(binName)
		};

		if (folded && ! allow_folding) {
			assertThrows(msg, key, AerospikeException.class, ResultCode.OP_NOT_APPLICABLE, ops);
			return;
		}

		Record record_union = assertSuccess(msg, key, ops);
		List<?> union_result_list = record_union.getList(binName);
		long union_count = (Long) union_result_list.get(2);
		long union_count2 = (Long) union_result_list.get(4);

		assertHLLCount(msg, index_bits, union_count, union_expected);
		assertEquals(msg, union_count, union_count2);

		for (int i = 0; i < keys.length; i++) {
			List<Value> sub_vals = vals.get(i);
			Record record = assertSuccess(msg, key,
					HLLOperation.add(p, binName, sub_vals, index_bits),
					HLLOperation.getCount(binName));
			List<?> result_list = record.getList(binName);
			long n_added = (Long) result_list.get(0);
			long count = (Long) result_list.get(1);

			assertEquals(msg, 0, n_added);
			assertEquals(msg, union_count, count);
			assertHLLCount(msg, index_bits, count, union_expected);
		}
	}

	@Test
	public void operateSetUnion() {
		ArrayList<List<Value>> vals = new ArrayList<List<Value>>();

		for (int i = 0; i < keys.length; i++) {
			ArrayList<Value> sub_vals = new ArrayList<Value>();

			for (int j = 0; j < n_entries / 3; j++) {
				sub_vals.add(new StringValue("key" + i + " " + j));
			}

			vals.add(sub_vals);
		}

		for (Integer index_bits : legalIndexBits) {
			assertSetUnion(vals, index_bits, false, false);
			assertSetUnion(vals, index_bits, false, true);
			assertSetUnion(vals, index_bits, true, false);
			assertSetUnion(vals, index_bits, true, true);
		}
	}

	@Test
	public void operateSetUnionFlags() {
		int index_bits = 6;
		int low_n_bits = 4;
		int high_n_bits = 8;
		String otherName = binName + "o";

		// Keep record around win binName is removed.
		ArrayList<HLLValue> hlls = new ArrayList<HLLValue>();
		Record record = assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.add(HLLPolicy.Default, otherName, entries, index_bits),
				Operation.get(otherName));
		List<?> result_list = record.getList(otherName);
		HLLValue hll = (HLLValue) result_list.get(1);

		hlls.add(hll);

		// create_only
		HLLPolicy c = new HLLPolicy(HLLWriteFlags.CREATE_ONLY);

		assertSuccess("create_only", key, HLLOperation.setUnion(c, binName, hlls));
		assertThrows("create_only - error", key, AerospikeException.class, ResultCode.BIN_EXISTS_ERROR,
				HLLOperation.setUnion(c, binName, hlls));

		// update_only
		HLLPolicy u = new HLLPolicy(HLLWriteFlags.UPDATE_ONLY);

		assertSuccess("update_only", key, HLLOperation.setUnion(u, binName, hlls));
		assertSuccess("remove bin", key, Operation.put(Bin.asNull(binName)));
		assertThrows("update_only - error", key, AerospikeException.class, ResultCode.BIN_NOT_FOUND,
				HLLOperation.setUnion(u, binName, hlls));

		// create_only no_fail
		HLLPolicy cn = new HLLPolicy(HLLWriteFlags.CREATE_ONLY | HLLWriteFlags.NO_FAIL);

		assertSuccess("create_only nofail", key, HLLOperation.setUnion(cn, binName, hlls));
		assertSuccess("create_only nofail - no error", key, HLLOperation.setUnion(cn, binName, hlls));

		// update_only no_fail
		HLLPolicy un = new HLLPolicy(HLLWriteFlags.UPDATE_ONLY | HLLWriteFlags.NO_FAIL);

		assertSuccess("update_only nofail", key, HLLOperation.setUnion(un, binName, hlls));
		assertSuccess("remove bin", key, Operation.put(Bin.asNull(binName)));
		assertSuccess("update_only nofail - no error", key, HLLOperation.setUnion(un, binName, hlls));

		// fold
		HLLPolicy f = new HLLPolicy(HLLWriteFlags.ALLOW_FOLD);

		// fold down
		assertSuccess("size up", key, HLLOperation.init(HLLPolicy.Default, binName, high_n_bits));
		assertSuccess("fold down to index_bits", key, HLLOperation.setUnion(f, binName, hlls));

		// fold up
		assertSuccess("size down", key, HLLOperation.init(HLLPolicy.Default, binName, low_n_bits));
		assertSuccess("fold down to low_n_bits", key, HLLOperation.setUnion(f, binName, hlls));
	}

	@Test
	public void operateRefreshCount() {
		int index_bits = 6;

		// Keep record around win binName is removed.
		assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.init(HLLPolicy.Default, binName + "other", index_bits),
				HLLOperation.init(HLLPolicy.Default, binName, index_bits));

		// Exists.
		assertSuccess("refresh zero count", key, HLLOperation.refreshCount(binName),
				HLLOperation.refreshCount(binName));
		assertSuccess("add items", key, HLLOperation.add(HLLPolicy.Default, binName, entries));
		assertSuccess("refresh count", key, HLLOperation.refreshCount(binName),
				HLLOperation.refreshCount(binName));

		// Does not exist.
		assertSuccess("remove bin", key, Operation.put(Bin.asNull(binName)));
		assertThrows("refresh nonexistant count", key, AerospikeException.class, ResultCode.BIN_NOT_FOUND,
				HLLOperation.refreshCount(binName));
	}

	@Test
	public void operateGetCount() {
		int index_bits = 6;

		// Keep record around win binName is removed.
		assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.init(HLLPolicy.Default, binName + "other", index_bits),
				HLLOperation.add(HLLPolicy.Default, binName, entries, index_bits));

		// Exists.
		Record record = assertSuccess("exists count", key, HLLOperation.getCount(binName));
		long count = record.getLong(binName);
		assertHLLCount("check count", index_bits, count, entries.size());

		// Does not exist.
		assertSuccess("remove bin", key, Operation.put(Bin.asNull(binName)));
		record = assertSuccess("exists count", key, HLLOperation.getCount(binName));
		assertEquals(null, record.getValue(binName));
	}

	@Test
	public void operateGetUnion() {
		int index_bits = 14;
		long expected_union_count = 0;
		ArrayList<List<Value>> vals = new ArrayList<List<Value>>();
		List<HLLValue> hlls = new ArrayList<HLLValue>();

		for (int i = 0; i < keys.length; i++) {
			ArrayList<Value> sub_vals = new ArrayList<Value>();

			for (int j = 0; j < n_entries / 3; j++) {
				sub_vals.add(new StringValue("key" + i + " " + j));
			}

			Record record = assertSuccess("init other keys", keys[i],
					Operation.delete(),
					HLLOperation.add(HLLPolicy.Default, binName, sub_vals, index_bits),
					Operation.get(binName));

			List<?> result_list = record.getList(binName);
			hlls.add((HLLValue)result_list.get(1));
			expected_union_count += sub_vals.size();
			vals.add(sub_vals);
		}

		// Keep record around win binName is removed.
		assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.init(HLLPolicy.Default, binName + "other", index_bits),
				HLLOperation.add(HLLPolicy.Default, binName, vals.get(0), index_bits));

		Record record = assertSuccess("union and unionCount", key,
				HLLOperation.getUnion(binName, hlls),
				HLLOperation.getUnionCount(binName, hlls));
		List<?> result_list = record.getList(binName);
		long union_count = (Long)result_list.get(1);

		assertHLLCount("verify union count", index_bits, union_count, expected_union_count);

		HLLValue union_hll = (HLLValue)result_list.get(0);

		record = assertSuccess("", key,
				Operation.put(new Bin(binName, union_hll)),
				HLLOperation.getCount(binName));
		result_list = record.getList(binName);
		long union_count_2 = (Long)result_list.get(1);

		assertEquals("unions equal", union_count, union_count_2);
	}

	@Test
	public void getPut() {
		for (ArrayList<Integer> desc : legalDescriptions) {
			int index_bits = desc.get(0);
			int minhash_bits = desc.get(1);

			assertSuccess("init record", key,
					Operation.delete(), HLLOperation.init(HLLPolicy.Default, binName, index_bits, minhash_bits));

			Record record = client.get(null, key);
			HLLValue hll = (HLLValue)record.getHLLValue(binName);

			client.delete(null, key);
			client.put(null, key, new Bin(binName, hll));

			record = assertSuccess("describe", key,
					HLLOperation.getCount(binName),
					HLLOperation.describe(binName));

			List<?> result_list = record.getList(binName);
			long count = (Long)result_list.get(0);
			List<?> description = (List<?>)result_list.get(1);

			assertEquals(0, count);
			assertDescription("Check description", description, index_bits, minhash_bits);
		}
	}

	public double absoluteSimilarityError(int index_bits, int minhash_bits, double expected_similarity) {
		double min_err_index = 1 / Math.sqrt(1 << index_bits);
		double min_err_minhash = 6 * Math.pow(Math.E, minhash_bits * -1) / expected_similarity;

		printDebug("min_err_index " + min_err_index + " min_err_minhash " + min_err_minhash +
				" index_bits " + index_bits + " minhash_bits " + minhash_bits + " expected_similarity "
				+ expected_similarity);

		return Math.max(min_err_index, min_err_minhash);
	}

	public void assertHMHSimilarity(String msg, int index_bits, int minhash_bits, double similarity,
			double expected_similarity, long intersect_count, long expected_intersect_count) {
		double sim_err_6sigma = 0;

		if (minhash_bits != 0) {
			sim_err_6sigma = 6 * absoluteSimilarityError(index_bits, minhash_bits, expected_similarity);
		}

		msg = msg + " - err " + sim_err_6sigma + " index_bits " + index_bits + " minhash_bits " + minhash_bits +
				"\n\t- similarity " + similarity + " expected_similarity " + expected_similarity +
				"\n\t- intersect_count " + intersect_count + " expected_intersect_count " + expected_intersect_count;

		printDebug(msg);

		if (minhash_bits == 0) {
			return;
		}

		assertTrue(msg, sim_err_6sigma > Math.abs(expected_similarity - similarity));
		assertTrue(msg, isWithinRelativeError(expected_intersect_count, intersect_count, sim_err_6sigma));
	}

	public void assertSimilarityOp(double overlap, List<Value> common, List<List<Value>> vals, int index_bits,
			int minhash_bits) {
		List<HLLValue> hlls = new ArrayList<HLLValue>();

		for (int i = 0; i < keys.length; i++) {
			Record record = assertSuccess("init other keys", keys[i],
					Operation.delete(),
					HLLOperation.add(HLLPolicy.Default, binName, vals.get(i), index_bits, minhash_bits),
					HLLOperation.add(HLLPolicy.Default, binName, common, index_bits, minhash_bits),
					Operation.get(binName));

			List<?> result_list = record.getList(binName);
			hlls.add((HLLValue)result_list.get(2));
		}

		// Keep record around win binName is removed.
		Record record = assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.init(HLLPolicy.Default, binName + "other", index_bits, minhash_bits),
				HLLOperation.setUnion(HLLPolicy.Default, binName, hlls),
				HLLOperation.describe(binName));
		List<?> result_list = record.getList(binName);
		List<?> description = (List<?>)result_list.get(1);

		assertDescription("check desc", description, index_bits, minhash_bits);

		record = assertSuccess("similarity and intersect_count", key,
				HLLOperation.getSimilarity(binName, hlls),
				HLLOperation.getIntersectCount(binName, hlls));
		result_list = record.getList(binName);
		double sim = (Double)result_list.get(0);
		long intersect_count = (Long)result_list.get(1);
		double expected_similarity = overlap;
		long expected_intersect_count = common.size();

		assertHMHSimilarity("check sim", index_bits, minhash_bits, sim, expected_similarity, intersect_count,
				expected_intersect_count);
	}

	@Test
	public void operateSimilarity() {
		double[] overlaps = new double[] {0.0001, 0.001, 0.01, 0.1, 0.5};

		for (double overlap : overlaps) {
			long expected_intersect_count = (long)(n_entries * overlap);
			ArrayList<Value> common = new ArrayList<Value>();

			for (int i = 0; i < expected_intersect_count; i++) {
				common.add(new StringValue("common" + i));
			}

			ArrayList<List<Value>> vals = new ArrayList<List<Value>>();
			long unique_entries_per_node = (n_entries - expected_intersect_count) / 3;

			for (int i = 0; i < keys.length; i++) {
				ArrayList<Value> sub_vals = new ArrayList<Value>();

				for (int j = 0; j < unique_entries_per_node; j++) {
					sub_vals.add(new StringValue("key" + i + " " + j));
				}

				vals.add(sub_vals);
			}

			for (ArrayList<Integer> desc : legalDescriptions) {
				int index_bits = desc.get(0);
				int minhash_bits = desc.get(1);

				if (minhash_bits == 0) {
					continue;
				}

				assertSimilarityOp(overlap, common, vals, index_bits, minhash_bits);
			}
		}
	}
}

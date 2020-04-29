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
	private static final int nEntries = 1 << 18;

	private static final int minNIndexBits = 4;
	private static final int maxNIndexBits = 16;
	private static final int minNMinhashBits = 4;
	private static final int maxNMinhashBits = 51;

	private static final ArrayList<Value> entries = new ArrayList<Value>();
	private static final ArrayList<Integer> legalNIndexBits = new ArrayList<Integer>();
	private static final ArrayList<ArrayList<Integer>> legalDescriptions = new ArrayList<ArrayList<Integer>>();
	private static final ArrayList<ArrayList<Integer>> illegalDescriptions = new ArrayList<ArrayList<Integer>>();

	@BeforeClass
	public static void createData() {
		for (int i = 0; i < nEntries; i++) {
			entries.add(new StringValue("key " + i));
		}

		for (int nIndexBits = minNIndexBits; nIndexBits <= maxNIndexBits; nIndexBits += 4) {
			int nCombinedBits = maxNMinhashBits + nIndexBits;
			int maxAllowedNMinhashBits = maxNMinhashBits;

			if (nCombinedBits > 64) {
				maxAllowedNMinhashBits -= nCombinedBits - 64;
			}

			int midNMinhashBits = (maxAllowedNMinhashBits + nIndexBits) / 2;
			ArrayList<Integer> legalZero = new ArrayList<Integer>();
			ArrayList<Integer> legalMin = new ArrayList<Integer>();
			ArrayList<Integer> legalMid = new ArrayList<Integer>();
			ArrayList<Integer> legalMax = new ArrayList<Integer>();

			legalNIndexBits.add(nIndexBits);
			legalZero.add(nIndexBits);
			legalMin.add(nIndexBits);
			legalMid.add(nIndexBits);
			legalMax.add(nIndexBits);

			legalZero.add(0);
			legalMin.add(minNMinhashBits);
			legalMid.add(midNMinhashBits);
			legalMax.add(maxAllowedNMinhashBits);

			legalDescriptions.add(legalZero);
			legalDescriptions.add(legalMin);
			legalDescriptions.add(legalMid);
			legalDescriptions.add(legalMax);
		}

		for (int indexBits = minNIndexBits - 1; indexBits <= maxNIndexBits + 5; indexBits += 4) {
			if (indexBits < minNIndexBits || indexBits > maxNIndexBits) {
				ArrayList<Integer> illegalZero = new ArrayList<Integer>();
				ArrayList<Integer> illegalMin = new ArrayList<Integer>();
				ArrayList<Integer> illegalMax = new ArrayList<Integer>();

				illegalZero.add(indexBits);
				illegalMin.add(indexBits);
				illegalMax.add(indexBits);

				illegalZero.add(0);
				illegalMin.add(minNMinhashBits - 1);
				illegalMax.add(maxNMinhashBits);

				illegalDescriptions.add(illegalZero);
				illegalDescriptions.add(illegalMin);
				illegalDescriptions.add(illegalMax);
			}
			else {
				ArrayList<Integer> illegalMin = new ArrayList<Integer>();
				ArrayList<Integer> illegalMax = new ArrayList<Integer>();
				ArrayList<Integer> illegalMax1 = new ArrayList<Integer>();

				illegalMin.add(indexBits);
				illegalMax.add(indexBits);

				illegalMin.add(minNMinhashBits - 1);
				illegalMax.add(maxNMinhashBits + 1);

				illegalDescriptions.add(illegalMin);
				illegalDescriptions.add(illegalMax);

				if (indexBits + maxNMinhashBits > 64) {
					illegalMax1.add(indexBits);
					illegalMax1.add(1 + maxNMinhashBits - (64 - (indexBits + maxNMinhashBits)));
					illegalDescriptions.add(illegalMax1);
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

	public boolean checkBits(int nIndexBits, int nMinhashBits) {
		return ! (nIndexBits < minNIndexBits || nIndexBits > maxNIndexBits ||
				(nMinhashBits != 0 && nMinhashBits < minNMinhashBits) ||
				nMinhashBits > maxNMinhashBits || nIndexBits + nMinhashBits > 64);
	}

	public double relativeCountError(int nIndexBits) {
		return 1.04 / Math.sqrt(Math.pow(2, nIndexBits));
	}

	public void assertDescription(String msg, List<?>description, int nIndexBits, int nMinhashBits) {
		assertEquals(msg, nIndexBits, (long)(Long) description.get(0));
		assertEquals(msg, nMinhashBits, (long)(Long) description.get(1));
	}

	public void assertInit(int nIndexBits, int nMinhashBits, boolean shouldPass) {
		String msg = "Fail - nIndexBits " + nIndexBits + " nMinhashBits " + nMinhashBits;
		HLLPolicy p = HLLPolicy.Default;
		Operation[] ops = new Operation[] {
				HLLOperation.init(p, binName, nIndexBits, nMinhashBits),
				HLLOperation.getCount(binName),
				HLLOperation.refreshCount(binName),
				HLLOperation.describe(binName)};

		if (! shouldPass) {
			assertThrows(msg, key, AerospikeException.class, ResultCode.PARAMETER_ERROR, ops);
			return;
		}

		Record record = assertSuccess(msg, key, ops);
		List<?> resultList = record.getList(binName);
		long count = (Long)resultList.get(1);
		long count1 = (Long)resultList.get(2);
		List<?> description = (List<?>)resultList.get(3);

		assertDescription(msg, description, nIndexBits, nMinhashBits);
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
		int nIndexBits = 4;

		// Keep record around win binName is removed.
		assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.init(HLLPolicy.Default, binName + "other", nIndexBits));

		// create_only
		HLLPolicy c = new HLLPolicy(HLLWriteFlags.CREATE_ONLY);

		assertSuccess("create_only", key, HLLOperation.init(c, binName, nIndexBits));
		assertThrows("create_only - error", key, AerospikeException.class, ResultCode.BIN_EXISTS_ERROR,
				HLLOperation.init(c, binName, nIndexBits));

		// update_only
		HLLPolicy u = new HLLPolicy(HLLWriteFlags.UPDATE_ONLY);

		assertSuccess("update_only", key, HLLOperation.init(u, binName, nIndexBits));
		assertSuccess("remove bin", key, Operation.put(Bin.asNull(binName)));
		assertThrows("update_only - error", key, AerospikeException.class, ResultCode.BIN_NOT_FOUND,
				HLLOperation.init(u, binName, nIndexBits));

		// create_only no_fail
		HLLPolicy cn = new HLLPolicy(HLLWriteFlags.CREATE_ONLY | HLLWriteFlags.NO_FAIL);

		assertSuccess("create_only nofail", key, HLLOperation.init(cn, binName, nIndexBits));
		assertSuccess("create_only nofail - no error", key, HLLOperation.init(cn, binName, nIndexBits));

		// update_only no_fail
		HLLPolicy un = new HLLPolicy(HLLWriteFlags.UPDATE_ONLY | HLLWriteFlags.NO_FAIL);

		assertSuccess("update_only nofail", key, HLLOperation.init(un, binName, nIndexBits));
		assertSuccess("remove bin", key, Operation.put(Bin.asNull(binName)));
		assertSuccess("update_only nofail - no error", key, HLLOperation.init(un, binName, nIndexBits));

		// fold
		assertSuccess("create_only", key, HLLOperation.init(c, binName, nIndexBits));

		HLLPolicy f = new HLLPolicy(HLLWriteFlags.ALLOW_FOLD);

		assertThrows("fold", key, AerospikeException.class, ResultCode.PARAMETER_ERROR,
				HLLOperation.init(f, binName, nIndexBits));
	}

	@Test
	public void badReInit() {
		HLLPolicy p = HLLPolicy.Default;

		assertSuccess("create min max", key, Operation.delete(), HLLOperation.init(p, binName, maxNIndexBits, 0));
		assertThrows("create_only", key, AerospikeException.class, ResultCode.OP_NOT_APPLICABLE,
				HLLOperation.init(p, binName, -1, maxNMinhashBits));
	}

	public boolean isWithinRelativeError(long expected, long estimate, double relativeError) {
		return expected * (1 - relativeError) <= estimate || estimate <= expected * (1 + relativeError);
	}

	public void assertHLLCount(String msg, int nIndexBits, long hllCount, long expected) {
		double countErr6Sigma = relativeCountError(nIndexBits) * 6;

		msg = msg + " - err " + countErr6Sigma + " count " + hllCount + " expected " + expected + " nIndexBits " +
				nIndexBits;

		printDebug(msg);
		assertTrue(msg, isWithinRelativeError(expected, hllCount, countErr6Sigma));
	}

	public void assertAddInit(int nIndexBits, int nMinhashBits) {
		client.delete(null, key);

		String msg = "Fail - nIdexBits " + nIndexBits + " nMinhashBits " + nMinhashBits;
		HLLPolicy p = HLLPolicy.Default;
		Operation[] ops = new Operation[] {
				HLLOperation.add(p, binName, entries, nIndexBits, nMinhashBits),
				HLLOperation.getCount(binName),
				HLLOperation.refreshCount(binName),
				HLLOperation.describe(binName),
				HLLOperation.add(p, binName, entries),
		};

		if (!checkBits(nIndexBits, nMinhashBits)) {
			assertThrows(msg, key, AerospikeException.class, ResultCode.PARAMETER_ERROR, ops);
			return;
		}

		Record record = assertSuccess(msg, key, ops);
		List<?> resultList = record.getList(binName);
		long count = (Long) resultList.get(1);
		long count1 = (Long) resultList.get(2);
		List<?> description = (List<?>) resultList.get(3);
		long nAdded = (Long) resultList.get(4);

		assertDescription(msg, description, nIndexBits, nMinhashBits);
		assertHLLCount(msg, nIndexBits, count, entries.size());
		assertEquals(count, count1);
		assertEquals(nAdded, 0);
	}

	@Test
	public void operateHLLAddInit() {
		for (ArrayList<Integer> desc : legalDescriptions) {
			assertAddInit(desc.get(0), desc.get(1));
		}
	}

	@Test
	public void operateAddFlags() {
		int nIndexBits = 4;

		// Keep record around win binName is removed.
		assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.init(HLLPolicy.Default, binName + "other", nIndexBits));

		// create_only
		HLLPolicy c = new HLLPolicy(HLLWriteFlags.CREATE_ONLY);

		assertSuccess("create_only", key, HLLOperation.add(c, binName, entries, nIndexBits));
		assertThrows("create_only - error", key, AerospikeException.class, ResultCode.BIN_EXISTS_ERROR,
				HLLOperation.add(c, binName, entries, nIndexBits));

		// update_only
		HLLPolicy u = new HLLPolicy(HLLWriteFlags.UPDATE_ONLY);

		assertThrows("update_only - error", key, AerospikeException.class, ResultCode.PARAMETER_ERROR,
				HLLOperation.add(u, binName, entries, nIndexBits));

		// create_only no_fail
		HLLPolicy cn = new HLLPolicy(HLLWriteFlags.CREATE_ONLY | HLLWriteFlags.NO_FAIL);

		assertSuccess("create_only nofail", key, HLLOperation.add(cn, binName, entries, nIndexBits));
		assertSuccess("create_only nofail - no error", key, HLLOperation.add(cn, binName, entries, nIndexBits));

		// fold
		assertSuccess("init", key, HLLOperation.init(HLLPolicy.Default, binName, nIndexBits));

		HLLPolicy f = new HLLPolicy(HLLWriteFlags.ALLOW_FOLD);

		assertThrows("fold", key, AerospikeException.class, ResultCode.PARAMETER_ERROR,
				HLLOperation.add(f, binName, entries, nIndexBits));
	}

	public void assertFold(List<Value> vals0, List<Value> vals1, int nIndexBits) {
		String msg = "Fail - nIndexBits " + nIndexBits;
		HLLPolicy p = HLLPolicy.Default;

		for (int ix = minNIndexBits; ix <= nIndexBits; ix++) {
			if (!checkBits(nIndexBits, 0) || ! checkBits(ix, 0)) {
				assertTrue("Expected valid inputs: " + msg, false);
			}

			Record recorda = assertSuccess(msg, key,
					Operation.delete(),
					HLLOperation.add(p, binName, vals0, nIndexBits),
					HLLOperation.getCount(binName),
					HLLOperation.refreshCount(binName),
					HLLOperation.describe(binName));

			List<?> resultAList = recorda.getList(binName);
			long counta = (Long) resultAList.get(1);
			long counta1 = (Long) resultAList.get(2);
			List<?> descriptiona = (List<?>) resultAList.get(3);

			assertDescription(msg, descriptiona, nIndexBits, 0);
			assertHLLCount(msg, nIndexBits, counta, vals0.size());
			assertEquals(counta, counta1);

			Record recordb = assertSuccess(msg, key,
					HLLOperation.fold(binName, ix),
					HLLOperation.getCount(binName),
					HLLOperation.add(p, binName, vals0),
					HLLOperation.add(p, binName, vals1),
					HLLOperation.getCount(binName),
					HLLOperation.describe(binName));

			List<?> resultBList = recordb.getList(binName);
			long countb = (Long) resultBList.get(1);
			long nAdded0 = (Long) resultBList.get(2);
			long countb1 = (Long) resultBList.get(4);
			List<?> descriptionb = (List<?>) resultBList.get(5);

			assertEquals(0, nAdded0);
			assertDescription(msg, descriptionb, ix, 0);
			assertHLLCount(msg, ix, countb, vals0.size());
			assertHLLCount(msg, ix, countb1, vals0.size() + vals1.size());
		}
	}

	@Test
	public void operateFold() {
		List<Value> vals0 = new ArrayList<Value>();
		List<Value> vals1 = new ArrayList<Value>();

		for (int i = 0; i < nEntries / 2; i++) {
			vals0.add(new StringValue("key " + i));
		}

		for (int i = nEntries / 2; i < nEntries; i++) {
			vals1.add(new StringValue("key " + i));
		}

		for (int nIndexBits = 4; nIndexBits < maxNIndexBits; nIndexBits++) {
			assertFold(vals0, vals1, nIndexBits);
		}
	}

	@Test
	public void operateFoldExists() {
		int nIndexBits = 10;
		int foldDown = 4;
		int foldUp = 16;

		// Keep record around win binName is removed.
		assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.init(HLLPolicy.Default, binName + "other", nIndexBits),
				HLLOperation.init(HLLPolicy.Default, binName, nIndexBits));

		// Exists.
		assertSuccess("exists fold down", key, HLLOperation.fold(binName, foldDown));
		assertThrows("exists fold up", key, AerospikeException.class, ResultCode.OP_NOT_APPLICABLE,
				HLLOperation.fold(binName, foldUp));

		// Does not exist.
		assertSuccess("remove bin", key, Operation.put(Bin.asNull(binName)));

		assertThrows("create_only - error", key, AerospikeException.class, ResultCode.BIN_NOT_FOUND,
				HLLOperation.fold(binName, foldDown));
	}

	public void assertSetUnion(List<List<Value>> vals, int nIndexBits, boolean folding, boolean allowFolding) {
		String msg = "Fail - nIndexBits " + nIndexBits;
		HLLPolicy p = HLLPolicy.Default;
		HLLPolicy u = HLLPolicy.Default;

		if (allowFolding) {
			u = new HLLPolicy(HLLWriteFlags.ALLOW_FOLD);
		}

		long unionExpected = 0;
		boolean folded = false;

		for (int i = 0; i < keys.length; i++) {
			int ix = nIndexBits;

			if (folding) {
				ix -= i;

				if (ix < minNIndexBits) {
					ix = minNIndexBits;
				}

				if (ix < nIndexBits) {
					folded = true;
				}
			}

			List<Value> subVals = vals.get(i);

			unionExpected += subVals.size();

			Record record = assertSuccess(msg, keys[i],
					Operation.delete(),
					HLLOperation.add(p, binName, subVals, ix),
					HLLOperation.getCount(binName));
			List<?> resultList = record.getList(binName);
			long count = (Long) resultList.get(1);

			assertHLLCount(msg, ix, count, subVals.size());
		}

		ArrayList<HLLValue> hlls = new ArrayList<HLLValue>();

		for (int i = 0; i < keys.length; i++) {
			Record record = assertSuccess(msg, keys[i],
					Operation.get(binName), HLLOperation.getCount(binName));
			List<?> resultList = record.getList(binName);
			HLLValue hll = (HLLValue)resultList.get(0);

			assertNotEquals(null, hll);
			hlls.add(hll);
		}

		Operation[] ops = new Operation[] {
				Operation.delete(),
				HLLOperation.init(p, binName, nIndexBits),
				HLLOperation.setUnion(u, binName, hlls),
				HLLOperation.getCount(binName),
				Operation.delete(), // And recreate it to test creating empty.
				HLLOperation.setUnion(p, binName, hlls),
				HLLOperation.getCount(binName)
		};

		if (folded && ! allowFolding) {
			assertThrows(msg, key, AerospikeException.class, ResultCode.OP_NOT_APPLICABLE, ops);
			return;
		}

		Record recordUnion = assertSuccess(msg, key, ops);
		List<?> unionResultList = recordUnion.getList(binName);
		long unionCount = (Long) unionResultList.get(2);
		long unionCount2 = (Long) unionResultList.get(4);

		assertHLLCount(msg, nIndexBits, unionCount, unionExpected);
		assertEquals(msg, unionCount, unionCount2);

		for (int i = 0; i < keys.length; i++) {
			List<Value> subVals = vals.get(i);
			Record record = assertSuccess(msg, key,
					HLLOperation.add(p, binName, subVals, nIndexBits),
					HLLOperation.getCount(binName));
			List<?> resultList = record.getList(binName);
			long nAdded = (Long) resultList.get(0);
			long count = (Long) resultList.get(1);

			assertEquals(msg, 0, nAdded);
			assertEquals(msg, unionCount, count);
			assertHLLCount(msg, nIndexBits, count, unionExpected);
		}
	}

	@Test
	public void operateSetUnion() {
		ArrayList<List<Value>> vals = new ArrayList<List<Value>>();

		for (int i = 0; i < keys.length; i++) {
			ArrayList<Value> subVals = new ArrayList<Value>();

			for (int j = 0; j < nEntries / 3; j++) {
				subVals.add(new StringValue("key" + i + " " + j));
			}

			vals.add(subVals);
		}

		for (Integer nIndexBits : legalNIndexBits) {
			assertSetUnion(vals, nIndexBits, false, false);
			assertSetUnion(vals, nIndexBits, false, true);
			assertSetUnion(vals, nIndexBits, true, false);
			assertSetUnion(vals, nIndexBits, true, true);
		}
	}

	@Test
	public void operateSetUnionFlags() {
		int nIndexBits = 6;
		int lowNBits = 4;
		int highNBits = 8;
		String otherName = binName + "o";

		// Keep record around win binName is removed.
		ArrayList<HLLValue> hlls = new ArrayList<HLLValue>();
		Record record = assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.add(HLLPolicy.Default, otherName, entries, nIndexBits),
				Operation.get(otherName));
		List<?> resultList = record.getList(otherName);
		HLLValue hll = (HLLValue) resultList.get(1);

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
		assertSuccess("size up", key, HLLOperation.init(HLLPolicy.Default, binName, highNBits));
		assertSuccess("fold down to index_bits", key, HLLOperation.setUnion(f, binName, hlls));

		// fold up
		assertSuccess("size down", key, HLLOperation.init(HLLPolicy.Default, binName, lowNBits));
		assertSuccess("fold down to low_n_bits", key, HLLOperation.setUnion(f, binName, hlls));
	}

	@Test
	public void operateRefreshCount() {
		int nIndexBits = 6;

		// Keep record around win binName is removed.
		assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.init(HLLPolicy.Default, binName + "other", nIndexBits),
				HLLOperation.init(HLLPolicy.Default, binName, nIndexBits));

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
		int nIndexBits = 6;

		// Keep record around win binName is removed.
		assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.init(HLLPolicy.Default, binName + "other", nIndexBits),
				HLLOperation.add(HLLPolicy.Default, binName, entries, nIndexBits));

		// Exists.
		Record record = assertSuccess("exists count", key, HLLOperation.getCount(binName));
		long count = record.getLong(binName);
		assertHLLCount("check count", nIndexBits, count, entries.size());

		// Does not exist.
		assertSuccess("remove bin", key, Operation.put(Bin.asNull(binName)));
		record = assertSuccess("exists count", key, HLLOperation.getCount(binName));
		assertEquals(null, record.getValue(binName));
	}

	@Test
	public void operateGetUnion() {
		int nIndexBits = 14;
		long expectedUnionCount = 0;
		ArrayList<List<Value>> vals = new ArrayList<List<Value>>();
		List<HLLValue> hlls = new ArrayList<HLLValue>();

		for (int i = 0; i < keys.length; i++) {
			ArrayList<Value> subVals = new ArrayList<Value>();

			for (int j = 0; j < nEntries / 3; j++) {
				subVals.add(new StringValue("key" + i + " " + j));
			}

			Record record = assertSuccess("init other keys", keys[i],
					Operation.delete(),
					HLLOperation.add(HLLPolicy.Default, binName, subVals, nIndexBits),
					Operation.get(binName));

			List<?> resultList = record.getList(binName);
			hlls.add((HLLValue)resultList.get(1));
			expectedUnionCount += subVals.size();
			vals.add(subVals);
		}

		// Keep record around win binName is removed.
		assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.init(HLLPolicy.Default, binName + "other", nIndexBits),
				HLLOperation.add(HLLPolicy.Default, binName, vals.get(0), nIndexBits));

		Record record = assertSuccess("union and unionCount", key,
				HLLOperation.getUnion(binName, hlls),
				HLLOperation.getUnionCount(binName, hlls));
		List<?> resultList = record.getList(binName);
		long unionCount = (Long)resultList.get(1);

		assertHLLCount("verify union count", nIndexBits, unionCount, expectedUnionCount);

		HLLValue unionHll = (HLLValue)resultList.get(0);

		record = assertSuccess("", key,
				Operation.put(new Bin(binName, unionHll)),
				HLLOperation.getCount(binName));
		resultList = record.getList(binName);
		long unionCount2 = (Long)resultList.get(1);

		assertEquals("unions equal", unionCount, unionCount2);
	}

	@Test
	public void getPut() {
		for (ArrayList<Integer> desc : legalDescriptions) {
			int nIndexBits = desc.get(0);
			int nMinhashBits = desc.get(1);

			assertSuccess("init record", key,
					Operation.delete(), HLLOperation.init(HLLPolicy.Default, binName, nIndexBits, nMinhashBits));

			Record record = client.get(null, key);
			HLLValue hll = (HLLValue)record.getHLLValue(binName);

			client.delete(null, key);
			client.put(null, key, new Bin(binName, hll));

			record = assertSuccess("describe", key,
					HLLOperation.getCount(binName),
					HLLOperation.describe(binName));

			List<?> resultList = record.getList(binName);
			long count = (Long)resultList.get(0);
			List<?> description = (List<?>)resultList.get(1);

			assertEquals(0, count);
			assertDescription("Check description", description, nIndexBits, nMinhashBits);
		}
	}

	public double absoluteSimilarityError(int nIndexBits, int nMinhashBits, double expectedSimilarity) {
		double minErrIndex = 1 / Math.sqrt(1 << nIndexBits);
		double minErrMinhash = 6 * Math.pow(Math.E, nMinhashBits * -1) / expectedSimilarity;

		printDebug("minErrIndex " + minErrIndex + " minErrMinhash " + minErrMinhash + " nIndexBits " + nIndexBits +
				" nMinhashBits " + nMinhashBits + " expectedSimilarity " + expectedSimilarity);

		return Math.max(minErrIndex, minErrMinhash);
	}

	public void assertHMHSimilarity(String msg, int nIndexBits, int nMinhashBits, double similarity,
			double expectedSimilarity, long intersectCount, long expectedIntersectCount) {
		double simErr6Sigma = 0;

		if (nMinhashBits != 0) {
			simErr6Sigma = 6 * absoluteSimilarityError(nIndexBits, nMinhashBits, expectedSimilarity);
		}

		msg = msg + " - err " + simErr6Sigma + " nIindexBits " + nIndexBits + " nMinhashBits " + nMinhashBits +
				"\n\t- similarity " + similarity + " expectedSimilarity " + expectedSimilarity +
				"\n\t- intersectCount " + intersectCount + " expectedIntersectCount " + expectedIntersectCount;

		printDebug(msg);

		if (nMinhashBits == 0) {
			return;
		}

		assertTrue(msg, simErr6Sigma > Math.abs(expectedSimilarity - similarity));
		assertTrue(msg, isWithinRelativeError(expectedIntersectCount, intersectCount, simErr6Sigma));
	}

	public void assertSimilarityOp(double overlap, List<Value> common, List<List<Value>> vals, int nIndexBits,
			int nMinhashBits) {
		List<HLLValue> hlls = new ArrayList<HLLValue>();

		for (int i = 0; i < keys.length; i++) {
			Record record = assertSuccess("init other keys", keys[i],
					Operation.delete(),
					HLLOperation.add(HLLPolicy.Default, binName, vals.get(i), nIndexBits, nMinhashBits),
					HLLOperation.add(HLLPolicy.Default, binName, common, nIndexBits, nMinhashBits),
					Operation.get(binName));

			List<?> resultList = record.getList(binName);
			hlls.add((HLLValue)resultList.get(2));
		}

		// Keep record around win binName is removed.
		Record record = assertSuccess("other bin", key,
				Operation.delete(),
				HLLOperation.init(HLLPolicy.Default, binName + "other", nIndexBits, nMinhashBits),
				HLLOperation.setUnion(HLLPolicy.Default, binName, hlls),
				HLLOperation.describe(binName));
		List<?> resultList = record.getList(binName);
		List<?> description = (List<?>)resultList.get(1);

		assertDescription("check desc", description, nIndexBits, nMinhashBits);

		record = assertSuccess("similarity and intersectCount", key,
				HLLOperation.getSimilarity(binName, hlls),
				HLLOperation.getIntersectCount(binName, hlls));
		resultList = record.getList(binName);
		double sim = (Double)resultList.get(0);
		long intersectCount = (Long)resultList.get(1);
		double expectedSimilarity = overlap;
		long expectedIntersectCount = common.size();

		assertHMHSimilarity("check sim", nIndexBits, nMinhashBits, sim, expectedSimilarity, intersectCount,
				expectedIntersectCount);
	}

	@Test
	public void operateSimilarity() {
		double[] overlaps = new double[] {0.0001, 0.001, 0.01, 0.1, 0.5};

		for (double overlap : overlaps) {
			long expectedIntersectCount = (long)(nEntries * overlap);
			ArrayList<Value> common = new ArrayList<Value>();

			for (int i = 0; i < expectedIntersectCount; i++) {
				common.add(new StringValue("common" + i));
			}

			ArrayList<List<Value>> vals = new ArrayList<List<Value>>();
			long uniqueEntriesPerNode = (nEntries - expectedIntersectCount) / 3;

			for (int i = 0; i < keys.length; i++) {
				ArrayList<Value> subVals = new ArrayList<Value>();

				for (int j = 0; j < uniqueEntriesPerNode; j++) {
					subVals.add(new StringValue("key" + i + " " + j));
				}

				vals.add(subVals);
			}

			for (ArrayList<Integer> desc : legalDescriptions) {
				int nIndexBits = desc.get(0);
				int nMinhashBits = desc.get(1);

				if (nMinhashBits == 0) {
					continue;
				}

				assertSimilarityOp(overlap, common, vals, nIndexBits, nMinhashBits);
			}
		}
	}

	@Test
	public void operateEmptySimilarity() {
		for (ArrayList<Integer> desc : legalDescriptions) {
			int nIndexBits = desc.get(0);
			int nMinhashBits = desc.get(1);

			Record record = assertSuccess("init", key,
					Operation.delete(),
					HLLOperation.init(HLLPolicy.Default, binName, nIndexBits, nMinhashBits),
					Operation.get(binName));

			List<?> resultList = record.getList(binName);
			List<HLLValue> hlls = new ArrayList<HLLValue>();

			hlls.add((HLLValue)resultList.get(1));

			record = assertSuccess("test", key,
					HLLOperation.getSimilarity(binName, hlls),
					HLLOperation.getIntersectCount(binName, hlls));

			resultList = record.getList(binName);

			double sim = (Double)resultList.get(0);
			long intersectCount = (Long)resultList.get(1);

			String msg = "(" + nIndexBits + ", " + nMinhashBits + ")";

			assertEquals(msg, 0, intersectCount);
			assertEquals(msg, Double.NaN, sim, 0.0);
		}
	}

	@Test
	public void operateIntersectHLL() {
		String otherBinName = binName + "other";

		for (ArrayList<Integer> desc : legalDescriptions) {
			int indexBits = desc.get(0);
			int minhashBits = desc.get(1);

			if (minhashBits != 0) {
				break;
			}

			Record record = assertSuccess("init", key,
					Operation.delete(),
					HLLOperation.add(HLLPolicy.Default, binName, entries, indexBits, minhashBits),
					Operation.get(binName),
					HLLOperation.add(HLLPolicy.Default, otherBinName, entries, indexBits, 4),
					Operation.get(otherBinName));

			List<HLLValue> hlls = new ArrayList<HLLValue>();
			List<HLLValue> hmhs = new ArrayList<HLLValue>();
			List<?> resultList = record.getList(binName);

			hlls.add((HLLValue)resultList.get(1));
			hlls.add(hlls.get(0));

			resultList = record.getList(otherBinName);
			hmhs.add((HLLValue)resultList.get(1));
			hmhs.add(hmhs.get(0));

			record = assertSuccess("intersect", key,
					HLLOperation.getIntersectCount(binName, hlls),
					HLLOperation.getSimilarity(binName, hlls));
			resultList = record.getList(binName);

			long intersectCount = (Long)resultList.get(0);

			assertTrue("intersect value too high", intersectCount < 1.8 * entries.size());

			hlls.add(hlls.get(0));

			assertThrows("Expect parameter error", key, AerospikeException.class, ResultCode.PARAMETER_ERROR,
					HLLOperation.getIntersectCount(binName, hlls));
			assertThrows("Expect parameter error", key, AerospikeException.class, ResultCode.PARAMETER_ERROR,
					HLLOperation.getSimilarity(binName, hlls));

			record = assertSuccess("intersect", key,
					HLLOperation.getIntersectCount(binName, hmhs),
					HLLOperation.getSimilarity(binName, hmhs));
			resultList = record.getList(binName);
			intersectCount = (Long)resultList.get(0);

			assertTrue("intersect value too high", intersectCount < 1.8 * entries.size());

			hmhs.add(hmhs.get(0));

			assertThrows("Expect parameter error", key, AerospikeException.class, ResultCode.OP_NOT_APPLICABLE,
					HLLOperation.getIntersectCount(binName, hmhs));
			assertThrows("Expect parameter error", key, AerospikeException.class, ResultCode.OP_NOT_APPLICABLE,
					HLLOperation.getSimilarity(binName, hmhs));
		}
	}
}

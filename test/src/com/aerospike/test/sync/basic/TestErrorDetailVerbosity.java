/*
 * Copyright 2012-2026 Aerospike, Inc.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static org.junit.Assert.assertNull;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.ExpressionTrace;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.SubCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.operation.BitOperation;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.sync.TestSync;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Validates the extended error-detail feature (CLIENT-4221) against a server
 * that supports error detail verbosity.
 *
 * <p>Sub-code expectations track the per-status enum numbering finalized on the
 * C client (commits 8b6f8db4 / 2d2495a1) and confirmed against the live server:
 * the old flat status-echo sub-codes (1100, 1134, 1138, 1701, ...) are retired.
 * Where the status is already maximally specific the server emits AS_SUB_NONE
 * and omits the sub-code map key entirely, so no "(subcode=...)" suffix appears.
 */
public class TestErrorDetailVerbosity extends TestSync {

	private static final String binName = "edv-bin";
	private static Key intKey;
	private static Key strKey;
	private static Key listKey;

	@BeforeClass
	public static void setup() {
		// Extended error-detail (sub-code/message) plus the verbosity-3 expression
		// build trace (SERVER-1137). The SERVER-1137 feature branch is cut from the
		// 8.1.1 line and reports its base version as 8.1.1.0-start-*, so gate at
		// 8.1.1 rather than the 8.1.3 release that first shipped the base tier.
		org.junit.Assume.assumeTrue("Extended error-detail requires server version 8.1.3 or later",
			args.serverVersion.isGreaterOrEqual(8, 1, 3, 0));

		WritePolicy wp = new WritePolicy();
		intKey = new Key(args.namespace, args.set, "edv-int-key");
		strKey = new Key(args.namespace, args.set, "edv-str-key");
		listKey = new Key(args.namespace, args.set, "edv-list-key");

		client.put(wp, intKey, new Bin(binName, 1));
		client.put(wp, strKey, new Bin(binName, "hello"));

		List<Value> seed = new ArrayList<>();
		seed.add(Value.get(10));
		seed.add(Value.get(20));
		seed.add(Value.get(30));
		client.put(wp, listKey, new Bin(binName, seed));
	}

	// ---------------------------------------------------------------------
	// Verbosity level semantics.
	// ---------------------------------------------------------------------

	@Test
	public void testDefaultVerbosityIsZero() {
		Policy p = new Policy();
		assertEquals(0, p.errorDetailVerbosity);

		WritePolicy wp = new WritePolicy();
		assertEquals(0, wp.errorDetailVerbosity);
	}

	@Test
	public void testVerbosityDisabled() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 0;

		try {
			client.operate(wp, intKey, Operation.append(new Bin(binName, "bad")));
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.BIN_TYPE_ERROR, ae.getResultCode());
			// With verbosity 0 the server sends no detail: no sub-code, and the message
			// falls back to the default ResultCode string.
			assertEquals(SubCode.NONE, ae.getSubCode());
			String msg = ae.getBaseMessage();
			assertEquals(ResultCode.getResultString(ResultCode.BIN_TYPE_ERROR), msg);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testVerbositySubcodeOnly() {
		// Verbosity 1: server sends the sub-code but not the message. A sub-code
		// that resolves to a value (BIN_NOT_FOUND from an HLL count op on a
		// missing bin -> sub-code 1) surfaces as the bare "error subcode=N" form.
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 1;

		Key key = new Key(args.namespace, args.set, "edv-subonly-key");
		client.put(new WritePolicy(), key, new Bin("other-bin", 1));

		try {
			client.operate(wp, key, HLLOperation.refreshCount("no-hll-bin"));
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.BIN_NOT_FOUND, ae.getResultCode());
			assertEquals(SubCode.BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP, ae.getSubCode());
			String msg = ae.getBaseMessage();
			assertNotNull(msg);
			assertTrue("Expected subcode in: " + msg, msg.contains("subcode=1"));
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testVerbositySubcodeAndMessage() {
		// Verbosity 2: server sends both message and sub-code, formatted as
		// "<message> (subcode=<n>)".
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		Key key = new Key(args.namespace, args.set, "edv-submsg-key");
		client.put(new WritePolicy(), key, new Bin("other-bin", 1));

		try {
			client.operate(wp, key, HLLOperation.refreshCount("no-hll-bin"));
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.BIN_NOT_FOUND, ae.getResultCode());
			assertEquals(SubCode.BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP, ae.getSubCode());
			String msg = ae.getBaseMessage();
			assertNotNull(msg);
			assertTrue("Expected message text in: " + msg, msg.contains("count op"));
			assertTrue("Expected subcode in: " + msg, msg.contains("(subcode=1)"));
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	// ---------------------------------------------------------------------
	// Sub-code absent cases (AS_SUB_NONE): the status is already maximally
	// specific, so the server omits the sub-code map key and the client must
	// never format a "(subcode=...)" suffix. The message carries the context.
	// ---------------------------------------------------------------------

	@Test
	public void testAppendToIntegerBin() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		try {
			client.operate(wp, intKey, Operation.append(new Bin(binName, "bad-append")));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.BIN_TYPE_ERROR, "cannot append");
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testIncrementStringBin() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		try {
			client.operate(wp, strKey, Operation.add(new Bin(binName, 1)));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.BIN_TYPE_ERROR, "cannot increment");
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testHllAddOnIntegerBin() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		List<Value> hllList = new ArrayList<>();
		hllList.add(Value.get("element1"));

		try {
			client.operate(wp, intKey,
				HLLOperation.add(HLLPolicy.Default, binName, hllList, 8));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.BIN_TYPE_ERROR, "bin is not hll type");
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testDeleteGenerationMismatch() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;
		wp.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		wp.generation = 777;

		try {
			client.delete(wp, intKey);
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.GENERATION_ERROR, "generation");
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	// ---------------------------------------------------------------------
	// Sub-code-present cases: per-status enum sub-code numbering.
	// ---------------------------------------------------------------------

	@Test
	public void testHllRefreshCountMissingBin() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		Key key = new Key(args.namespace, args.set, "edv-no-hll-key");
		client.put(new WritePolicy(), key, new Bin("other-bin", 1));

		try {
			client.operate(wp, key, HLLOperation.refreshCount("no-hll-bin"));
		}
		catch (AerospikeException ae) {
			// AS_SUB_BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP = 1
			assertSubcode(ae, ResultCode.BIN_NOT_FOUND, SubCode.BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testListGetIndexOutOfBounds() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		try {
			client.operate(wp, listKey, ListOperation.get(binName, 99));
		}
		catch (AerospikeException ae) {
			// AS_SUB_OPNOT_CDT_INDEX_OUT_OF_BOUNDS = 1
			assertSubcode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testListGetByRankOutOfBounds() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		try {
			client.operate(wp, listKey, ListOperation.getByRank(binName, 99, ListReturnType.VALUE));
		}
		catch (AerospikeException ae) {
			// AS_SUB_OPNOT_CDT_RANK_OUT_OF_BOUNDS = 2
			assertSubcode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_CDT_RANK_OUT_OF_BOUNDS);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testListBoundedOverflow() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		ListPolicy bounded = new ListPolicy(ListOrder.ORDERED, ListWriteFlags.INSERT_BOUNDED);

		try {
			client.operate(wp, listKey, ListOperation.insert(bounded, binName, 10, Value.get(5)));
		}
		catch (AerospikeException ae) {
			// AS_SUB_OPNOT_CDT_BOUNDED_LIST_OVERFLOW = 3
			assertSubcode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_CDT_BOUNDED_LIST_OVERFLOW);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testHllFoldTargetTooLarge() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		Key key = new Key(args.namespace, args.set, "edv-hll-fold-key");
		client.delete(new WritePolicy(), key);
		client.operate(new WritePolicy(), key, HLLOperation.init(HLLPolicy.Default, binName, 8));

		try {
			client.operate(wp, key, HLLOperation.fold(binName, 14));
		}
		catch (AerospikeException ae) {
			// AS_SUB_OPNOT_HLL_FOLD_INDEX_BITS_TOO_LARGE = 8
			assertSubcode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_HLL_FOLD_INDEX_BITS_TOO_LARGE);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testBitGetOffsetOutOfRange() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		Key key = new Key(args.namespace, args.set, "edv-bits-key");
		client.put(new WritePolicy(), key, new Bin(binName, new byte[]{(byte)0xAA, (byte)0xBB, (byte)0xCC, (byte)0xDD}));

		try {
			client.operate(wp, key, BitOperation.get(binName, 2000000000, 8));
		}
		catch (AerospikeException ae) {
			// AS_SUB_PARAM_BITS_OFFSET_OUT_OF_RANGE = 2
			assertSubcode(ae, ResultCode.PARAMETER_ERROR, SubCode.PARAM_BITS_OFFSET_OUT_OF_RANGE);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testBitGetSizeZero() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		Key key = new Key(args.namespace, args.set, "edv-bits-key2");
		client.put(new WritePolicy(), key, new Bin(binName, new byte[]{(byte)0xAA, (byte)0xBB, (byte)0xCC, (byte)0xDD}));

		try {
			client.operate(wp, key, BitOperation.get(binName, 0, 0));
		}
		catch (AerospikeException ae) {
			// AS_SUB_PARAM_BITS_SIZE_OUT_OF_RANGE = 3
			assertSubcode(ae, ResultCode.PARAMETER_ERROR, SubCode.PARAM_BITS_SIZE_OUT_OF_RANGE);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testReadFilteredOut() {
		// FILTERED_OUT carries no sub-code (AS_SUB_NONE) and a contextual message;
		// the server's as_sub_filtered_t enum was removed, so there is no version gate.
		Policy p = new Policy();
		p.errorDetailVerbosity = 2;
		p.filterExp = Exp.build(Exp.eq(Exp.intBin(binName), Exp.val(99)));
		p.failOnFilteredOut = true;

		try {
			client.get(p, intKey);
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.FILTERED_OUT, "filtered out");
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	// ---------------------------------------------------------------------
	// Additional particle modify type mismatches (sub-code absent).
	// ---------------------------------------------------------------------

	@Test
	public void testPrependToIntegerBin() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		try {
			client.operate(wp, intKey, Operation.prepend(new Bin(binName, "bad-prepend")));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.BIN_TYPE_ERROR, "prepend");
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testIncrementDoubleOnIntegerBin() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		try {
			client.operate(wp, strKey, Operation.add(new Bin(binName, 1.5)));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.BIN_TYPE_ERROR);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	// ---------------------------------------------------------------------
	// Additional CDT list ops.
	// ---------------------------------------------------------------------

	@Test
	public void testListPopIndexOutOfBounds() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		try {
			client.operate(wp, listKey, ListOperation.pop(binName, 99));
		}
		catch (AerospikeException ae) {
			// AS_SUB_OPNOT_CDT_INDEX_OUT_OF_BOUNDS = 1
			assertSubcode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testListAddUniqueViolation() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		// Seed [10,20,30]; appending an existing value with ADD_UNIQUE fails.
		ListPolicy unique = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE);

		try {
			client.operate(wp, listKey, ListOperation.append(unique, binName, Value.get(20)));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.ELEMENT_EXISTS);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testListOpOnRawBytesBin() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		// A raw-bytes bin is not a list -> list_get triggers the wrong-type path.
		Key key = new Key(args.namespace, args.set, "edv-list-raw-key");
		client.put(new WritePolicy(), key, new Bin(binName, new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF}));

		try {
			client.operate(wp, key, ListOperation.get(binName, 0));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.BIN_TYPE_ERROR);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	// ---------------------------------------------------------------------
	// CDT map ops.
	// ---------------------------------------------------------------------

	@Test
	public void testMapCreateOnlyExistingKey() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		Key key = new Key(args.namespace, args.set, "edv-map-create-key");
		Map<Integer,String> seed = new HashMap<>();
		seed.put(1, "a");
		client.put(new WritePolicy(), key, new Bin(binName, seed));

		MapPolicy mp = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.CREATE_ONLY);

		try {
			client.operate(wp, key, MapOperation.put(mp, binName, Value.get(1), Value.get("b")));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.ELEMENT_EXISTS);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testMapUpdateOnlyMissingKey() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		Key key = new Key(args.namespace, args.set, "edv-map-update-key");
		Map<Integer,String> seed = new HashMap<>();
		seed.put(1, "a");
		client.put(new WritePolicy(), key, new Bin(binName, seed));

		MapPolicy mp = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.UPDATE_ONLY);

		try {
			client.operate(wp, key, MapOperation.put(mp, binName, Value.get(99), Value.get("b")));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.ELEMENT_NOT_FOUND);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testMapOpOnListBin() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		// listKey holds a list; a map op against it triggers the wrong-type path.
		try {
			client.operate(wp, listKey, MapOperation.getByKey(binName, Value.get(1), MapReturnType.VALUE));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.BIN_TYPE_ERROR);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testMapOpOnRawBytesBin() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		Key key = new Key(args.namespace, args.set, "edv-map-raw-key");
		client.put(new WritePolicy(), key, new Bin(binName, new byte[]{0x42, 0x42}));

		try {
			client.operate(wp, key, MapOperation.getByKey(binName, Value.get(1), MapReturnType.VALUE));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.BIN_TYPE_ERROR);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testListCtxIntoStringMapValue() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		// Map value at key 1 is a string; descending into it with a list op and
		// a map-key context is a type mismatch.
		Key key = new Key(args.namespace, args.set, "edv-map-ctx-key");
		Map<Integer,String> seed = new HashMap<>();
		seed.put(1, "leaf-string");
		client.put(new WritePolicy(), key, new Bin(binName, seed));

		try {
			client.operate(wp, key, ListOperation.get(binName, 0, CTX.mapKey(Value.get(1))));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.BIN_TYPE_ERROR);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	// ---------------------------------------------------------------------
	// Additional HLL ops.
	// ---------------------------------------------------------------------

	@Test
	public void testHllInitInvalidIndexBits() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		Key key = new Key(args.namespace, args.set, "edv-hll-bad-bits-key");

		try {
			// Index bit count out of the legal [4,16] range -> server-side reject.
			client.operate(wp, key, HLLOperation.init(HLLPolicy.Default, binName, 30));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.PARAMETER_ERROR);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testHllOpOnRawBytesBin() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		Key key = new Key(args.namespace, args.set, "edv-hll-raw-key");
		client.put(new WritePolicy(), key, new Bin(binName, new byte[]{0x01, 0x02, 0x03}));

		try {
			client.operate(wp, key, HLLOperation.getCount(binName));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.BIN_TYPE_ERROR);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	// ---------------------------------------------------------------------
	// Write / delete / read policy (sub-code absent unless noted).
	// ---------------------------------------------------------------------

	@Test
	public void testWriteCreateOnlyExistingRecord() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;
		wp.recordExistsAction = RecordExistsAction.CREATE_ONLY;

		try {
			// intKey already exists.
			client.put(wp, intKey, new Bin(binName, 2));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.KEY_EXISTS_ERROR);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testWriteReplaceOnlyMissingRecord() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;
		wp.recordExistsAction = RecordExistsAction.REPLACE_ONLY;

		Key key = new Key(args.namespace, args.set, "edv-replace-missing-key");
		client.delete(new WritePolicy(), key);

		try {
			client.put(wp, key, new Bin(binName, 1));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.KEY_NOT_FOUND_ERROR);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testWriteGenerationMismatch() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;
		wp.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
		wp.generation = 999;

		try {
			client.put(wp, intKey, new Bin(binName, 2));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.GENERATION_ERROR, "generation");
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testOperateFilteredOut() {
		// FILTERED_OUT carries no sub-code (AS_SUB_NONE) and a contextual message;
		// the server's as_sub_filtered_t enum was removed, so there is no version gate.
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;
		wp.filterExp = Exp.build(Exp.eq(Exp.intBin(binName), Exp.val(99)));
		wp.failOnFilteredOut = true;

		try {
			client.operate(wp, intKey, Operation.get(binName));
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.FILTERED_OUT, "filtered out");
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	// ---------------------------------------------------------------------
	// Happy path: verbosity set on a successful command must not break.
	// ---------------------------------------------------------------------

	@Test
	public void testSuccessNoErrorDetails() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		Key key = new Key(args.namespace, args.set, "edv-success-key");
		client.put(wp, key, new Bin(binName, 42));

		Policy rp = new Policy();
		rp.errorDetailVerbosity = 2;
		Record record = client.get(rp, key);

		assertNotNull(record);
		assertEquals(42, record.getInt(binName));
	}

	// ---------------------------------------------------------------------
	// Verbosity 3: expression build-failure trace (SERVER-1137).
	//
	// A type-mismatched comparison expression fails to *build* on the server.
	// As a filter_exp it yields "invalid filter expression in request"; as an
	// exp_write op it yields "invalid expression in operation request". Both carry
	// PARAMETER_ERROR + SubCode.NONE and, at verbosity 3, a structured build trace.
	// Assert trace PRESENCE and SHAPE, not exact byte_offset/snippet bytes.
	// ---------------------------------------------------------------------

	/** Expression whose operands are type-mismatched (int vs float), so the server build fails. */
	private static Exp badExp() {
		return Exp.eq(Exp.val(5), Exp.val(6.0));
	}

	@Test
	public void testFilterExpBuildFailureTrace() {
		Policy p = new Policy();
		p.errorDetailVerbosity = 3;
		p.filterExp = Exp.build(badExp());

		try {
			client.get(p, intKey);
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
			assertEquals(SubCode.NONE, ae.getSubCode());

			String msg = ae.getBaseMessage();
			assertNotNull(msg);
			assertTrue("Expected filter-build message in: " + msg,
				msg.contains("invalid filter expression in request"));

			ExpressionTrace t = ae.getExpressionTrace();
			assertNotNull("Expected a non-null expression trace at verbosity 3", t);
			assertEquals("Expected a build-phase trace", ExpressionTrace.PHASE_BUILD, t.getPhase());
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testExpWriteBuildFailureTrace() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 3;

		try {
			client.operate(wp, intKey,
				ExpOperation.write(binName, Exp.build(badExp()), ExpWriteFlags.DEFAULT));
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
			assertEquals(SubCode.NONE, ae.getSubCode());

			String msg = ae.getBaseMessage();
			assertNotNull(msg);
			assertTrue("Expected exp-op build message in: " + msg,
				msg.contains("invalid expression in operation request"));

			ExpressionTrace t = ae.getExpressionTrace();
			assertNotNull("Expected a non-null expression trace at verbosity 3", t);
			assertEquals("Expected a build-phase trace", ExpressionTrace.PHASE_BUILD, t.getPhase());
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testFilterExpBuildFailureVerbosity2HasNoTrace() {
		// Additive-superset check: the SAME inducer at verbosity 2 surfaces the same
		// message but NO trace. Verbosity 3 = verbosity 2 + trace.
		Policy p = new Policy();
		p.errorDetailVerbosity = 2;
		p.filterExp = Exp.build(badExp());

		try {
			client.get(p, intKey);
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
			assertEquals(SubCode.NONE, ae.getSubCode());

			String msg = ae.getBaseMessage();
			assertNotNull(msg);
			assertTrue("Expected filter-build message in: " + msg,
				msg.contains("invalid filter expression in request"));

			assertNull("Verbosity 2 must surface NO expression trace", ae.getExpressionTrace());
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	/**
	 * Assert the server-supplied {@code (resultCode, sub-code)} pair. The numeric
	 * sub-code must be exposed first-class via {@link AerospikeException#getSubCode()}
	 * (not merely embedded in the message string), and the "subcode=N" suffix must
	 * still appear in the message for parity with the C client.
	 */
	private void assertSubcode(AerospikeException ae, int expectedResultCode, int expectedSubcode) {
		assertEquals("Unexpected result code", expectedResultCode, ae.getResultCode());
		assertEquals("Unexpected subcode", expectedSubcode, ae.getSubCode());

		String msg = ae.getBaseMessage();
		assertNotNull("Expected server error message", msg);
		assertTrue("Expected 'subcode=" + expectedSubcode + "' in: " + msg,
			msg.contains("subcode=" + expectedSubcode));
	}

	/**
	 * Assert that the server surfaced a contextual message but NO subcode
	 * (AS_SUB_NONE): {@link AerospikeException#getSubCode()} is {@link SubCode#NONE}
	 * and the "(subcode=...)" suffix must never appear. Any expectedSubstrings are
	 * required in the message; pass none to skip the message-text check (mirrors a
	 * NULL expected_msg_substr in the C example).
	 */
	private void assertSubcodeAbsent(AerospikeException ae, int expectedResultCode, String... expectedSubstrings) {
		assertEquals("Unexpected result code", expectedResultCode, ae.getResultCode());
		assertEquals("Expected no subcode", SubCode.NONE, ae.getSubCode());

		String msg = ae.getBaseMessage();
		assertNotNull("Expected server error message", msg);

		for (String expected : expectedSubstrings) {
			assertTrue("Expected '" + expected + "' in: " + msg, msg.contains(expected));
		}
		assertFalse("Expected NO subcode suffix in: " + msg, msg.contains("subcode="));
	}
}

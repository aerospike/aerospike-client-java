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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.SubCode;
import com.aerospike.client.Value;
import com.aerospike.client.Value.HLLValue;
import com.aerospike.client.operation.BitOperation;
import com.aerospike.client.operation.BitPolicy;
import com.aerospike.client.operation.BitResizeFlags;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.sync.TestSync;
import java.util.ArrayList;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Live-server verification of the {@code (ResultCode, SubCode)} pairs the client
 * publishes in {@link SubCode}. {@code SubCode} is a hand-maintained mirror of the
 * server's per-status subcode enums ({@code as/include/base/proto.h}); these tests
 * pin the pairs reachable from a plain Java client (CDT / HLL / bitwise ops) so the
 * mirror can't drift silently.
 *
 * <p>{@link TestErrorDetailVerbosity} already covers a first slice of subcodes (CDT
 * index/rank, HLL fold-index-too-large, bit offset/size, HLL cannot-create, bounded
 * list overflow). This suite extends that to the remaining reachable subcodes:
 * {@link SubCode#PARAM_BITS_RESIZE_EXCEEDED}, {@link SubCode#OPNOT_HLL_INDEX_BITS_UNSET},
 * {@link SubCode#OPNOT_HLL_CANNOT_FOLD_MINHASH},
 * {@link SubCode#OPNOT_HLL_CANNOT_REDUCE_INDEX_BITS},
 * {@link SubCode#OPNOT_HLL_CANNOT_REDUCE_MINHASH_BITS}, and
 * {@link SubCode#OPNOT_HLL_INTERSECT_MINHASH_MISMATCH}.
 *
 * <p>Subcodes that need cluster state (PARTITION_UNAVAILABLE), config (FAIL_FORBIDDEN
 * stop-writes / durability), concurrency (MRT_BLOCKED), or ACL are out of reach here
 * and intentionally not covered. Requires an 8.1.3+ server.
 */
public class TestErrorDetailSubcode extends TestSync {

	private static final String binName = "eds-bin";

	@BeforeClass
	public static void setup() {
		org.junit.Assume.assumeTrue("Extended error-detail requires server version 8.1.3 or later",
			args.serverVersion.isGreaterOrEqual(8, 1, 3, 0));
	}

	/** WritePolicy at verbosity 2 (sub-code + message), the level these assertions expect. */
	private static WritePolicy verbosityWP() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;
		return wp;
	}

	/**
	 * Build a standalone HLL sketch with the given parameters on a throwaway key and
	 * return its serialized value, for use as a union / intersect input.
	 * {@code minHashBitCount = -1} yields a plain (minhash=0) sketch.
	 */
	private static HLLValue makeSketch(int indexBits, int minHashBits, String suffix) {
		Key k = new Key(args.namespace, args.set, "eds-sketch-" + suffix);

		List<Value> seed = new ArrayList<>();
		seed.add(Value.get("a"));
		seed.add(Value.get("b"));
		client.operate(new WritePolicy(), k,
			HLLOperation.add(HLLPolicy.Default, binName, seed, indexBits, minHashBits));

		Record rec = client.operate(new WritePolicy(), k,
			Operation.get(binName), HLLOperation.getCount(binName));
		List<?> results = rec.getList(binName);
		return (HLLValue)results.get(0);
	}

	// -----------------------------------------------------------
	// High-confidence single-bin triggers.
	// -----------------------------------------------------------

	@Test
	public void testBitResizeBeyondMaxBlob() {
		Key key = new Key(args.namespace, args.set, "eds-bits-resize-key");
		client.put(new WritePolicy(), key, new Bin(binName, new byte[]{0x01, 0x02}));

		try {
			// PROTO_SIZE_MAX = 128 MiB; the check is on the resulting size (>=).
			client.operate(verbosityWP(), key,
				BitOperation.resize(BitPolicy.Default, binName, 128 * 1024 * 1024, BitResizeFlags.DEFAULT));
		}
		catch (AerospikeException ae) {
			assertSubcode(ae, ResultCode.PARAMETER_ERROR, SubCode.PARAM_BITS_RESIZE_EXCEEDED);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testHllAddWithoutIndexBits() {
		Key key = new Key(args.namespace, args.set, "eds-hll-unset-key");
		client.delete(new WritePolicy(), key);

		List<Value> list = new ArrayList<>();
		list.add(Value.get("x"));

		try {
			// No existing sketch to inherit index_bits from, and index_bits left unset (-1).
			client.operate(verbosityWP(), key,
				HLLOperation.add(HLLPolicy.Default, binName, list, -1, -1));
		}
		catch (AerospikeException ae) {
			assertSubcode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_HLL_INDEX_BITS_UNSET);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testHllFoldOnMinhashSketch() {
		Key key = new Key(args.namespace, args.set, "eds-hll-foldmh-key");
		client.delete(new WritePolicy(), key);
		client.operate(new WritePolicy(), key,
			HLLOperation.init(HLLPolicy.Default, binName, 12, 4)); // minhash_bits = 4 > 0

		try {
			client.operate(verbosityWP(), key, HLLOperation.fold(binName, 8));
		}
		catch (AerospikeException ae) {
			assertSubcode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_HLL_CANNOT_FOLD_MINHASH);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	// -----------------------------------------------------------
	// HLL union / intersect triggers (crafted input sketches).
	// -----------------------------------------------------------

	@Test
	public void testHllUnionReduceIndexBits() {
		HLLValue input = makeSketch(6, -1, "reduceidx"); // index_bits = 6

		Key key = new Key(args.namespace, args.set, "eds-hll-reduceidx-key");
		client.delete(new WritePolicy(), key);
		client.operate(new WritePolicy(), key,
			HLLOperation.init(HLLPolicy.Default, binName, 12, -1)); // bin index_bits = 12

		List<HLLValue> inputs = new ArrayList<>();
		inputs.add(input);

		try {
			// 12 > 6, default policy has no ALLOW_FOLD -> cannot reduce.
			client.operate(verbosityWP(), key,
				HLLOperation.setUnion(HLLPolicy.Default, binName, inputs));
		}
		catch (AerospikeException ae) {
			assertSubcode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_HLL_CANNOT_REDUCE_INDEX_BITS);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testHllUnionReduceMinhashBits() {
		HLLValue input = makeSketch(10, 6, "reducemh"); // same index_bits, minhash = 6

		Key key = new Key(args.namespace, args.set, "eds-hll-reducemh-key");
		client.delete(new WritePolicy(), key);
		client.operate(new WritePolicy(), key,
			HLLOperation.init(HLLPolicy.Default, binName, 10, 4)); // bin minhash = 4

		List<HLLValue> inputs = new ArrayList<>();
		inputs.add(input);

		try {
			// index_bits equal (10), minhash 4 != 6, no ALLOW_FOLD -> cannot reduce minhash.
			client.operate(verbosityWP(), key,
				HLLOperation.setUnion(HLLPolicy.Default, binName, inputs));
		}
		catch (AerospikeException ae) {
			assertSubcode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_HLL_CANNOT_REDUCE_MINHASH_BITS);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testHllIntersectMinhashMismatch() {
		// 3+ inputs, internally consistent (same params) with non-zero minhash.
		HLLValue s1 = makeSketch(10, 4, "intersect1");
		HLLValue s2 = makeSketch(10, 4, "intersect2");
		HLLValue s3 = makeSketch(10, 4, "intersect3");

		Key key = new Key(args.namespace, args.set, "eds-hll-intersect-key");
		client.delete(new WritePolicy(), key);

		List<Value> seed = new ArrayList<>();
		seed.add(Value.get("z"));
		client.operate(new WritePolicy(), key,
			HLLOperation.add(HLLPolicy.Default, binName, seed, 10, -1)); // bin minhash = 0

		List<HLLValue> inputs = new ArrayList<>();
		inputs.add(s1);
		inputs.add(s2);
		inputs.add(s3);

		try {
			// n_elements = 3 > 2; bin minhash (0) mismatches inputs' minhash (4).
			client.operate(verbosityWP(), key,
				HLLOperation.getIntersectCount(binName, inputs));
		}
		catch (AerospikeException ae) {
			assertSubcode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_HLL_INTERSECT_MINHASH_MISMATCH);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	/**
	 * Assert the server-supplied {@code (resultCode, sub-code)} pair. The numeric
	 * sub-code must be exposed first-class via {@link AerospikeException#getSubcode()}
	 * (not merely embedded in the message string), and the "subcode=N" suffix must
	 * still appear in the message for parity with the C client.
	 */
	private void assertSubcode(AerospikeException ae, int expectedResultCode, int expectedSubcode) {
		assertEquals("Unexpected result code", expectedResultCode, ae.getResultCode());
		assertEquals("Unexpected subcode", expectedSubcode, ae.getSubcode());

		String msg = ae.getBaseMessage();
		assertNotNull("Expected server error message", msg);
		assertTrue("Expected 'subcode=" + expectedSubcode + "' in: " + msg,
			msg.contains("subcode=" + expectedSubcode));
	}
}

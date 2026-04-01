/*
 * Copyright 2012-2025 Aerospike, Inc.
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
import com.aerospike.client.Value;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.sync.TestSync;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestErrorDetailVerbosity extends TestSync {

	private static final String binName = "edv-bin";
	private static Key intKey;
	private static Key strKey;

	@BeforeClass
	public static void setup() {
		WritePolicy wp = new WritePolicy();
		intKey = new Key(args.namespace, args.set, "edv-int-key");
		strKey = new Key(args.namespace, args.set, "edv-str-key");

		client.put(wp, intKey, new Bin(binName, 1));
		client.put(wp, strKey, new Bin(binName, "hello"));
	}

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
			// With verbosity 0, the message should be the default ResultCode string.
			String msg = ae.getBaseMessage();
			assertEquals(ResultCode.getResultString(ResultCode.BIN_TYPE_ERROR), msg);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testVerbositySubcodeOnly() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 1;

		try {
			client.operate(wp, intKey, Operation.append(new Bin(binName, "bad")));
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.BIN_TYPE_ERROR, ae.getResultCode());
			String msg = ae.getBaseMessage();
			assertNotNull(msg);
			assertTrue("Expected subcode in: " + msg, msg.contains("subcode="));
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testVerbositySubcodeAndMessage() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		try {
			client.operate(wp, intKey, Operation.append(new Bin(binName, "bad")));
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.BIN_TYPE_ERROR, ae.getResultCode());
			String msg = ae.getBaseMessage();
			assertNotNull(msg);
			assertTrue("Expected 'cannot append' in: " + msg, msg.contains("cannot append"));
			assertTrue("Expected subcode in: " + msg, msg.contains("subcode="));
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testAppendToIntegerBin() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		try {
			client.operate(wp, intKey, Operation.append(new Bin(binName, "bad-append")));
		}
		catch (AerospikeException ae) {
			assertErrorDetails(ae, ResultCode.BIN_TYPE_ERROR, "cannot append", "subcode=1100");
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
			assertErrorDetails(ae, ResultCode.GENERATION_ERROR, "delete generation mismatch", "subcode=1701");
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
			assertErrorDetails(ae, ResultCode.BIN_TYPE_ERROR, "cannot increment", "subcode=1100");
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
			assertErrorDetails(ae, ResultCode.BIN_TYPE_ERROR, "bin is not hll type", "subcode=1138");
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testHllRefreshCountMissingBin() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		Key key3 = new Key(args.namespace, args.set, "edv-no-hll-key");
		client.put(new WritePolicy(), key3, new Bin("other-bin", 1));

		try {
			client.operate(wp, key3, HLLOperation.refreshCount("no-hll-bin"));
		}
		catch (AerospikeException ae) {
			assertErrorDetails(ae, ResultCode.BIN_NOT_FOUND, "subcode=1134");
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testSuccessNoErrorDetails() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;

		// A successful write with verbosity=2 should not cause issues.
		Key key = new Key(args.namespace, args.set, "edv-success-key");
		client.put(wp, key, new Bin(binName, 42));

		Policy rp = new Policy();
		rp.errorDetailVerbosity = 2;
		Record record = client.get(rp, key);

		assertNotNull(record);
		assertEquals(42, record.getInt(binName));
	}

	private void assertErrorDetails(AerospikeException ae, int expectedResultCode, String... expectedSubstrings) {
		assertEquals("Unexpected result code", expectedResultCode, ae.getResultCode());

		String msg = ae.getBaseMessage();
		assertNotNull("Expected server error message", msg);

		for (String expected : expectedSubstrings) {
			assertTrue("Expected '" + expected + "' in: " + msg, msg.contains(expected));
		}
	}
}

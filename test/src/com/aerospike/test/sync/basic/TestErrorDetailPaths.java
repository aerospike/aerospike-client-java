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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.ExpressionTrace;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.ResultCode;
import com.aerospike.client.SubCode;
import com.aerospike.client.Txn;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.test.sync.TestSync;
import java.util.ArrayList;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Extended-error coverage for command paths that are wired to surface the server
 * error-detail field but were otherwise exercised only through operate/get/put/delete:
 * the header-read path ({@code exists} / {@code getHeader}), the UDF apply path
 * ({@code execute}), and the single-record-in-transaction path (a non-null {@link Txn}).
 *
 * <p>The trigger op is incidental here; the assertion is purely the extended-error
 * surface (subcode / message / expression trace) flowing through each path. Requires an
 * 8.1.3+ server; the transaction cases additionally require a Strong-Consistency
 * namespace (gated per-test on {@code args.scMode}).
 */
public class TestErrorDetailPaths extends TestSync {

	private static final String binName = "edp-bin";
	private static Key intKey;
	private static Key listKey;

	@BeforeClass
	public static void setup() {
		org.junit.Assume.assumeTrue("Extended error-detail requires server version 8.1.3 or later",
			args.serverVersion.isGreaterOrEqual(8, 1, 3, 0));

		RegisterTask task = client.register(null, TestErrorDetailPaths.class.getClassLoader(),
			"udf/record_example.lua", "record_example.lua", Language.LUA);
		task.waitTillComplete();

		WritePolicy wp = new WritePolicy();
		intKey = new Key(args.namespace, args.set, "edp-int-key");
		listKey = new Key(args.namespace, args.set, "edp-list-key");

		client.put(wp, intKey, new Bin(binName, 1));

		List<Value> seed = new ArrayList<>();
		seed.add(Value.get(10));
		seed.add(Value.get(20));
		seed.add(Value.get(30));
		client.put(wp, listKey, new Bin(binName, seed));
	}

	/** A well-formed record filter that evaluates false against the seeded record. */
	private static Expression filteredOutExp() {
		return Exp.build(Exp.eq(Exp.intBin(binName), Exp.val(99)));
	}

	/** A type-mismatched comparison (int vs float) that fails to *build* server-side. */
	private static Expression buildErrorExp() {
		return Exp.build(Exp.eq(Exp.val(5), Exp.val(6.0)));
	}

	// -----------------------------------------------------------
	// Header-read path: exists (ExistsCommand).
	// -----------------------------------------------------------

	@Test
	public void testExistsFilteredOutMessage() {
		Policy p = new Policy();
		p.errorDetailVerbosity = 2;
		p.filterExp = filteredOutExp();
		p.failOnFilteredOut = true;

		try {
			client.exists(p, intKey);
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.FILTERED_OUT, "filtered out");
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testExistsBuildTrace() {
		Policy p = new Policy();
		p.errorDetailVerbosity = 3;
		p.filterExp = buildErrorExp();

		try {
			client.exists(p, intKey);
		}
		catch (AerospikeException ae) {
			assertBuildTrace(ae);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	// -----------------------------------------------------------
	// Header-read path: getHeader (ReadHeaderCommand).
	// -----------------------------------------------------------

	@Test
	public void testGetHeaderFilteredOutMessage() {
		Policy p = new Policy();
		p.errorDetailVerbosity = 2;
		p.filterExp = filteredOutExp();
		p.failOnFilteredOut = true;

		try {
			client.getHeader(p, intKey);
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.FILTERED_OUT, "filtered out");
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testGetHeaderBuildTrace() {
		Policy p = new Policy();
		p.errorDetailVerbosity = 3;
		p.filterExp = buildErrorExp();

		try {
			client.getHeader(p, intKey);
		}
		catch (AerospikeException ae) {
			assertBuildTrace(ae);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	// -----------------------------------------------------------
	// UDF apply path: execute (ExecuteCommand). The filter is evaluated before the
	// UDF body runs, so a trivial registered module suffices; the failure is the
	// filter, surfaced through the execute path's toException.
	// -----------------------------------------------------------

	@Test
	public void testExecuteFilteredOutNoDetail() {
		// Unlike the single-record read/write/delete verbs, the UDF-apply path (server
		// udf.c) does not stage a "filtered out" detail on a record-filter FILTERED_OUT
		// - it returns the bare status. Pin that so a future server change that starts
		// staging it is flagged for review. (The build-trace case below still proves the
		// error-detail field flows through the execute path.)
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;
		wp.filterExp = filteredOutExp();
		wp.failOnFilteredOut = true;

		try {
			client.execute(wp, intKey, "record_example", "getGeneration");
		}
		catch (AerospikeException ae) {
			assertEquals(ResultCode.FILTERED_OUT, ae.getResultCode());
			assertEquals(SubCode.NONE, ae.getSubCode());
			assertNull("Expected no expression trace", ae.getExpressionTrace());
			// No staged server detail: the message falls back to the default result string.
			assertEquals(ResultCode.getResultString(ResultCode.FILTERED_OUT), ae.getBaseMessage());
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testExecuteBuildTrace() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 3;
		wp.filterExp = buildErrorExp();

		try {
			client.execute(wp, intKey, "record_example", "getGeneration");
		}
		catch (AerospikeException ae) {
			assertBuildTrace(ae);
			return;
		}
		assertTrue("Expected AerospikeException", false);
	}

	// -----------------------------------------------------------
	// Single-record-in-transaction path: RecordParser.parseFields with a non-null
	// Txn (as opposed to parseFieldsError). MRT requires a Strong-Consistency namespace.
	// -----------------------------------------------------------

	@Test
	public void testTxnSubcodeBearingOp() {
		org.junit.Assume.assumeTrue("Transactions require a Strong-Consistency namespace", args.scMode);

		Txn txn = new Txn();
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;
		wp.txn = txn;

		try {
			client.operate(wp, listKey, ListOperation.get(binName, 99));
		}
		catch (AerospikeException ae) {
			assertSubcode(ae, ResultCode.OP_NOT_APPLICABLE, SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS);
			return;
		}
		finally {
			client.abort(txn);
		}
		assertTrue("Expected AerospikeException", false);
	}

	@Test
	public void testTxnFilteredReadMessage() {
		org.junit.Assume.assumeTrue("Transactions require a Strong-Consistency namespace", args.scMode);

		Txn txn = new Txn();
		Policy p = new Policy();
		p.errorDetailVerbosity = 2;
		p.txn = txn;
		p.filterExp = filteredOutExp();
		p.failOnFilteredOut = true;

		try {
			client.get(p, intKey);
		}
		catch (AerospikeException ae) {
			assertSubcodeAbsent(ae, ResultCode.FILTERED_OUT, "filtered out");
			return;
		}
		finally {
			client.abort(txn);
		}
		assertTrue("Expected AerospikeException", false);
	}

	// -----------------------------------------------------------
	// Assertion helpers (mirror TestErrorDetailVerbosity).
	// -----------------------------------------------------------

	/** Pin the extended-error surface of an expression build failure at verbosity 3. */
	private void assertBuildTrace(AerospikeException ae) {
		assertEquals("Unexpected result code", ResultCode.PARAMETER_ERROR, ae.getResultCode());
		assertEquals("Unexpected subcode", SubCode.NONE, ae.getSubCode());

		ExpressionTrace t = ae.getExpressionTrace();
		assertNotNull("Expected a build trace at verbosity 3", t);
		assertEquals("Expected a build-phase trace", ExpressionTrace.PHASE_BUILD, t.getPhase());
		assertTrue("Msgpack build traces carry byte_offset", t.getByteOffset() >= 0);
	}

	private void assertSubcode(AerospikeException ae, int expectedResultCode, int expectedSubcode) {
		assertEquals("Unexpected result code", expectedResultCode, ae.getResultCode());
		assertEquals("Unexpected subcode", expectedSubcode, ae.getSubCode());

		assertNotNull("Expected server error message", ae.getBaseMessage());

		// getMessage() renders "Error <resultCode>,<subCode>"; the parsed server
		// message is appended verbatim (see AerospikeException.getMessage()).
		String prefix = "Error " + expectedResultCode + "," + expectedSubcode;
		String msg = ae.getMessage();
		assertTrue("Expected message to start with \"" + prefix + "\": " + msg,
			msg.startsWith(prefix));
	}

	private void assertSubcodeAbsent(AerospikeException ae, int expectedResultCode, String... expectedSubstrings) {
		assertEquals("Unexpected result code", expectedResultCode, ae.getResultCode());
		assertEquals("Expected no subcode", SubCode.NONE, ae.getSubCode());

		String msg = ae.getBaseMessage();
		assertNotNull("Expected server error message", msg);

		for (String expected : expectedSubstrings) {
			assertTrue("Expected '" + expected + "' in: " + msg, msg.contains(expected));
		}
		String prefix = "Error " + expectedResultCode + "," + SubCode.NONE;
		String rendered = ae.getMessage();
		assertTrue("Expected no-subcode prefix \"" + prefix + "\" in: " + rendered,
			rendered.startsWith(prefix));
	}
}

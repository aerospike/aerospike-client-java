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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.ExpressionTrace;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.SubCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.test.sync.TestSync;

/**
 * Expression error-detail coverage (CLIENT-4221) ported from the Python QE
 * suites on aerospike-tests-python branch dylan/ael-error-details
 * (test_msgpack_exp_error_details.py and the language-agnostic rows of
 * test_ael_error_details.py / test_ael_expop_error_details.py).
 *
 * <p>Four areas:
 * <ol>
 * <li>Eval-phase (PHASE_EVAL) expression traces for filter runtime faults:
 *     div/mod by zero, INT64_MIN overflow, CDT out-of-bounds (with sub-code),
 *     unordered-map compare.</li>
 * <li>Verbosity tier-1 suppression semantics: an AS_SUB_NONE error at
 *     verbosity 1 stages no error details at all, while a real sub-code CDT
 *     fault ships sub-code only (no message, no trace).</li>
 * <li>Verb parity: build / fault / FALSE / absent / metadata-FALSE / tier-2 /
 *     clean-pass filter scenarios across put, delete and operate, including
 *     the verb-specific metadata-filter messages.</li>
 * <li>Exp-op context breadth: exp-read build/fault/absent details,
 *     EVAL_NO_FAIL swallowing, legal non-boolean value reads, invalid read
 *     flags, and write-policy flag outcomes that stage no details.</li>
 * </ol>
 *
 * <p>The Java client sends classic msgpack expressions (no AEL source), so
 * traces follow the msgpack contract: build traces always carry byte_offset,
 * eval traces never do, and lang / ael_offset / ael_span are never present.
 * Eval-trace keys outcome (7) and operands (13) are not yet decoded by the
 * Java client, so those Python assertions are not ported.
 */
public class TestExpErrorDetail extends TestSync {

	private static final String BIN_INT = "x";       // 10
	private static final String BIN_FLOAT = "y";     // 2.5
	private static final String BIN_STR = "name";    // "ael"
	private static final String BIN_LIST = "xs";     // [1, 2, 3]
	private static final String BIN_MAP1 = "um1";    // unordered map
	private static final String BIN_MAP2 = "um2";    // unordered map
	private static final String BIN_MISSING = "missing";

	private static Key stdKey;
	private static Key scratchKey;

	@BeforeClass
	public static void setup() {
		org.junit.Assume.assumeTrue("Extended error-detail requires server version 8.1.3 or later",
			args.serverVersion.isGreaterOrEqual(8, 1, 3, 0));

		stdKey = new Key(args.namespace, args.set, "eed-std-key");
		scratchKey = new Key(args.namespace, args.set, "eed-scratch-key");

		List<Value> xs = new ArrayList<>();
		xs.add(Value.get(1));
		xs.add(Value.get(2));
		xs.add(Value.get(3));

		Map<String,Integer> um1 = new HashMap<>();
		um1.put("a", 1);
		Map<String,Integer> um2 = new HashMap<>();
		um2.put("b", 2);

		client.put(new WritePolicy(), stdKey,
			new Bin(BIN_INT, 10),
			new Bin(BIN_FLOAT, 2.5),
			new Bin(BIN_STR, "ael"),
			new Bin(BIN_LIST, xs),
			new Bin(BIN_MAP1, um1),
			new Bin(BIN_MAP2, um2));

		reseedScratch();
	}

	private static void reseedScratch() {
		client.put(new WritePolicy(), scratchKey, new Bin(BIN_INT, 10), new Bin("keep", 1));
	}

	// ---------------------------------------------------------------------
	// Shared expression inducers.
	// ---------------------------------------------------------------------

	/** Build failure: type-mismatched comparison (int vs float). */
	private static Expression buildErrorExp() {
		return Exp.build(Exp.eq(Exp.val(5), Exp.val(6.0)));
	}

	/** Eval fault: integer division by zero (gt(div(5, 0), 1)). */
	private static Expression divZeroFilterExp() {
		return Exp.build(Exp.gt(Exp.div(Exp.val(5), Exp.val(0)), Exp.val(1)));
	}

	/** Eval fault: CDT list index 9 over [1,2,3] (carries a real sub-code). */
	private static Exp cdtOobExp() {
		return ListExp.getByIndex(ListReturnType.VALUE, Exp.Type.INT, Exp.val(9), Exp.listBin(BIN_LIST));
	}

	// ---------------------------------------------------------------------
	// Runners.
	// ---------------------------------------------------------------------

	private AerospikeException expectFilteredGet(int verbosity, Expression filter, int expectedRc) {
		Policy p = new Policy();
		p.errorDetailVerbosity = verbosity;
		p.filterExp = filter;
		p.failOnFilteredOut = true;

		try {
			client.get(p, stdKey);
		}
		catch (AerospikeException ae) {
			assertEquals("Unexpected result code", expectedRc, ae.getResultCode());
			return ae;
		}
		fail("Expected AerospikeException with result code " + expectedRc);
		return null;
	}

	private AerospikeException expectOperateError(Key key, int verbosity, int expectedRc, Operation op) {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = verbosity;

		try {
			client.operate(wp, key, op);
		}
		catch (AerospikeException ae) {
			assertEquals("Unexpected result code", expectedRc, ae.getResultCode());
			return ae;
		}
		fail("Expected AerospikeException with result code " + expectedRc);
		return null;
	}

	// ---------------------------------------------------------------------
	// Assertion helpers.
	// ---------------------------------------------------------------------

	/**
	 * Assert an eval-phase (runtime) trace. Per the msgpack contract, runtime
	 * traces never carry byte_offset (getByteOffset() == -1).
	 */
	private static ExpressionTrace assertEvalTrace(AerospikeException ae, String op, int depth, String[] path) {
		ExpressionTrace t = ae.getExpressionTrace();
		assertNotNull("Expected a non-null expression trace at verbosity 3", t);
		assertEquals("Expected an eval-phase trace", ExpressionTrace.PHASE_EVAL, t.getPhase());
		assertEquals("Unexpected trace op", op, t.getOp());
		assertEquals("Unexpected trace depth", depth, t.getDepth());
		assertArrayEquals("Unexpected trace path", path, t.getPath());
		assertEquals("Runtime traces must not carry byte_offset", -1, t.getByteOffset());
		assertNotNull("Expected an op-stream snippet", t.getSnippet());
		return t;
	}

	/**
	 * Assert a build-phase trace. Per the msgpack contract, build traces
	 * always carry byte_offset.
	 */
	private static ExpressionTrace assertBuildTrace(AerospikeException ae) {
		ExpressionTrace t = ae.getExpressionTrace();
		assertNotNull("Expected a non-null expression trace at verbosity 3", t);
		assertEquals("Expected a build-phase trace", ExpressionTrace.PHASE_BUILD, t.getPhase());
		assertTrue("Msgpack build traces must carry byte_offset", t.getByteOffset() >= 0);
		return t;
	}

	private static void assertMessageContains(AerospikeException ae, String expected) {
		String msg = ae.getBaseMessage();
		assertNotNull("Expected server error message", msg);
		assertTrue("Expected '" + expected + "' in: " + msg, msg.contains(expected));
	}

	/**
	 * Assert the server staged NO error details (no field 45): the message
	 * falls back to the default result-code string, there is no sub-code and
	 * no trace.
	 */
	private static void assertNoDetails(AerospikeException ae, int expectedRc) {
		assertEquals("Unexpected result code", expectedRc, ae.getResultCode());
		assertEquals("Expected no subcode", SubCode.NONE, ae.getSubCode());
		assertEquals("Expected the default result-code message",
			ResultCode.getResultString(expectedRc), ae.getBaseMessage());
		assertNull("Expected no expression trace", ae.getExpressionTrace());
	}

	// ---------------------------------------------------------------------
	// 1. Eval-phase (runtime) filter traces at verbosity 3.
	//
	// Python: test_msgpack_exp_error_details.py CASES_MP_FAULT and the
	// language-agnostic rows of test_ael_error_details.py CASES_FILTER_FAULT.
	// ---------------------------------------------------------------------

	@Test
	public void testFilterFaultDivByZeroTrace() {
		AerospikeException ae = expectFilteredGet(3, divZeroFilterExp(), ResultCode.FILTERED_OUT);
		assertEquals(SubCode.NONE, ae.getSubCode());
		assertMessageContains(ae, "integer division by zero");

		ExpressionTrace t = assertEvalTrace(ae, "div", 2, new String[] {"gt", "div"});
		assertTrue("Expected div op in snippet: " + t.getSnippet(), t.getSnippet().contains("div("));
	}

	@Test
	public void testFilterFaultModByZeroTrace() {
		Expression exp = Exp.build(
			Exp.eq(Exp.mod(Exp.intBin(BIN_INT), Exp.val(0)), Exp.val(1)));

		AerospikeException ae = expectFilteredGet(3, exp, ResultCode.FILTERED_OUT);
		assertEquals(SubCode.NONE, ae.getSubCode());
		assertMessageContains(ae, "integer modulo by zero");

		ExpressionTrace t = assertEvalTrace(ae, "mod", 2, new String[] {"eq", "mod"});
		assertTrue("Expected mod op in snippet: " + t.getSnippet(), t.getSnippet().contains("mod("));
	}

	@Test
	public void testFilterFaultDivOverflowTrace() {
		// INT64_MIN / -1 overflows 64-bit signed division.
		Expression exp = Exp.build(
			Exp.gt(Exp.div(Exp.val(Long.MIN_VALUE), Exp.val(-1)), Exp.val(1)));

		AerospikeException ae = expectFilteredGet(3, exp, ResultCode.FILTERED_OUT);
		assertEquals(SubCode.NONE, ae.getSubCode());
		assertMessageContains(ae, "integer division overflow");

		assertEvalTrace(ae, "div", 2, new String[] {"gt", "div"});
	}

	@Test
	public void testFilterFaultUnorderedMapCompareTrace() {
		// Both bins are unordered maps; an ordered equality compare faults.
		Expression exp = Exp.build(Exp.eq(Exp.mapBin(BIN_MAP1), Exp.mapBin(BIN_MAP2)));

		AerospikeException ae = expectFilteredGet(3, exp, ResultCode.FILTERED_OUT);
		assertEquals(SubCode.NONE, ae.getSubCode());
		assertMessageContains(ae, "cannot compare an unordered map");

		assertEvalTrace(ae, "eq", 1, new String[] {"eq"});
	}

	@Test
	public void testFilterFaultCdtOutOfBoundsSubcodeAndTrace() {
		// A CDT sub-op fault carries a real sub-code through the FILTERED_OUT
		// status (the CDT layer's out-of-bounds subcode = 1).
		Expression exp = Exp.build(Exp.eq(cdtOobExp(), Exp.val(1)));

		AerospikeException ae = expectFilteredGet(3, exp, ResultCode.FILTERED_OUT);
		assertEquals(SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS, ae.getSubCode());
		assertMessageContains(ae, "out of bounds");

		assertEvalTrace(ae, "call", 2, new String[] {"eq", "call"});
	}

	// ---------------------------------------------------------------------
	// 2. Verbosity tier-1 suppression semantics.
	//
	// Python: CASES_FILTER_VERBOSITY / CASES_MP_VERBOSITY. Build failures and
	// div-by-zero faults carry AS_SUB_NONE, so at tier 1 (sub-code only) they
	// have nothing to send and field 45 is suppressed entirely; a real sub-code
	// CDT fault ships sub-code only (no message, no trace).
	// ---------------------------------------------------------------------

	@Test
	public void testVerbosity1BuildErrorSuppressed() {
		AerospikeException ae = expectFilteredGet(1, buildErrorExp(), ResultCode.PARAMETER_ERROR);
		assertNoDetails(ae, ResultCode.PARAMETER_ERROR);
	}

	@Test
	public void testVerbosity1EvalFaultSuppressed() {
		AerospikeException ae = expectFilteredGet(1, divZeroFilterExp(), ResultCode.FILTERED_OUT);
		assertNoDetails(ae, ResultCode.FILTERED_OUT);
	}

	@Test
	public void testVerbosity1CdtFaultSubcodeOnly() {
		Expression exp = Exp.build(Exp.eq(cdtOobExp(), Exp.val(1)));

		AerospikeException ae = expectFilteredGet(1, exp, ResultCode.FILTERED_OUT);
		assertEquals(SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS, ae.getSubCode());
		assertNull("Tier 1 must surface no trace", ae.getExpressionTrace());

		String msg = ae.getBaseMessage();
		assertNotNull(msg);
		assertTrue("Expected bare subcode form in: " + msg, msg.contains("subcode=1"));
		assertFalse("Tier 1 must surface no message text in: " + msg, msg.contains("out of bounds"));
	}

	@Test
	public void testVerbosity2EvalFaultMessageNoTrace() {
		// Tier 2: message present, trace suppressed.
		AerospikeException ae = expectFilteredGet(2, divZeroFilterExp(), ResultCode.FILTERED_OUT);
		assertMessageContains(ae, "integer division by zero");
		assertNull("Verbosity 2 must surface NO expression trace", ae.getExpressionTrace());
	}

	// ---------------------------------------------------------------------
	// 3. Verb parity: the filter stages identically across single-record
	// verbs (shared server rw_utils), except metadata-phase FALSE which is
	// verb-dependent.
	//
	// Python: test_ael_error_details.py PARITY_CASES.
	// ---------------------------------------------------------------------

	private static final String[] PARITY_VERBS = {"put", "delete", "operate"};

	private static WritePolicy filterPolicy(int verbosity, Expression filter) {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = verbosity;
		wp.filterExp = filter;
		wp.failOnFilteredOut = true;
		return wp;
	}

	/** Reseed the scratch record, run the verb, and return the caught exception (or null). */
	private AerospikeException runVerb(String verb, WritePolicy wp) {
		reseedScratch();
		try {
			switch (verb) {
			case "put":
				client.put(wp, scratchKey, new Bin("other", 1));
				break;
			case "delete":
				client.delete(wp, scratchKey);
				break;
			default:
				client.operate(wp, scratchKey, Operation.get(BIN_INT));
				break;
			}
		}
		catch (AerospikeException ae) {
			return ae;
		}
		return null;
	}

	private AerospikeException expectVerbError(String verb, WritePolicy wp, int expectedRc) {
		AerospikeException ae = runVerb(verb, wp);
		assertNotNull("[" + verb + "] Expected AerospikeException", ae);
		assertEquals("[" + verb + "] Unexpected result code", expectedRc, ae.getResultCode());
		return ae;
	}

	@Test
	public void testParityBuildError() {
		for (String verb : PARITY_VERBS) {
			AerospikeException ae = expectVerbError(verb,
				filterPolicy(3, buildErrorExp()), ResultCode.PARAMETER_ERROR);
			assertMessageContains(ae, "invalid metadata expression in request");
			assertBuildTrace(ae);
		}
	}

	@Test
	public void testParityEvalFault() {
		for (String verb : PARITY_VERBS) {
			AerospikeException ae = expectVerbError(verb,
				filterPolicy(3, divZeroFilterExp()), ResultCode.FILTERED_OUT);
			assertMessageContains(ae, "integer division by zero");
			assertEvalTrace(ae, "div", 2, new String[] {"gt", "div"});
		}
	}

	@Test
	public void testParityFilterFalse() {
		// Clean FALSE explain. The trace's outcome (2) and operand-pair keys
		// are not yet decoded by the Java client; assert phase and deciding op.
		Expression exp = Exp.build(Exp.eq(Exp.intBin(BIN_INT), Exp.val(11)));

		for (String verb : PARITY_VERBS) {
			AerospikeException ae = expectVerbError(verb,
				filterPolicy(3, exp), ResultCode.FILTERED_OUT);
			assertMessageContains(ae, "filter expression evaluated to false");

			ExpressionTrace t = ae.getExpressionTrace();
			assertNotNull("[" + verb + "] Expected an explain trace at verbosity 3", t);
			assertEquals("[" + verb + "] Expected an eval-phase trace",
				ExpressionTrace.PHASE_EVAL, t.getPhase());
			assertEquals("[" + verb + "] Expected the deciding comparison op", "eq", t.getOp());
		}
	}

	@Test
	public void testParityFilterAbsent() {
		// First absent bin reference through the chain decides the outcome.
		Expression exp = Exp.build(Exp.eq(Exp.intBin(BIN_MISSING), Exp.val(2)));

		for (String verb : PARITY_VERBS) {
			AerospikeException ae = expectVerbError(verb,
				filterPolicy(3, exp), ResultCode.FILTERED_OUT);
			assertMessageContains(ae, "filter references an absent bin or key");

			ExpressionTrace t = ae.getExpressionTrace();
			assertNotNull("[" + verb + "] Expected an explain trace at verbosity 3", t);
			assertEquals("[" + verb + "] Expected an eval-phase trace",
				ExpressionTrace.PHASE_EVAL, t.getPhase());
			assertEquals("[" + verb + "] Expected the absent accessor op", "bin", t.getOp());
		}
	}

	@Test
	public void testParityMetadataFalseVerbSpecificMessage() {
		// A metadata-only filter FALSE is staged per verb (write.c / delete
		// / read paths) and is message-only: NO trace even at verbosity 3.
		Expression exp = Exp.build(Exp.eq(Exp.ttl(), Exp.val(-5)));

		Map<String,String> expected = new HashMap<>();
		expected.put("put", "write filtered out by metadata filter");
		expected.put("delete", "delete filtered out by metadata filter");
		expected.put("operate", "read filtered out by metadata filter");

		for (String verb : PARITY_VERBS) {
			AerospikeException ae = expectVerbError(verb,
				filterPolicy(3, exp), ResultCode.FILTERED_OUT);
			assertMessageContains(ae, expected.get(verb));
			assertNull("[" + verb + "] Metadata-phase FALSE must stage no trace",
				ae.getExpressionTrace());
		}
	}

	@Test
	public void testParityTier2MessageNoTrace() {
		for (String verb : PARITY_VERBS) {
			AerospikeException ae = expectVerbError(verb,
				filterPolicy(2, divZeroFilterExp()), ResultCode.FILTERED_OUT);
			assertMessageContains(ae, "integer division by zero");
			assertNull("[" + verb + "] Verbosity 2 must surface NO trace", ae.getExpressionTrace());
		}
	}

	@Test
	public void testParityCleanPass() {
		// Filter TRUE: every verb succeeds with verbosity set.
		Expression exp = Exp.build(Exp.eq(Exp.intBin(BIN_INT), Exp.val(10)));

		for (String verb : PARITY_VERBS) {
			AerospikeException ae = runVerb(verb, filterPolicy(3, exp));
			assertNull("[" + verb + "] Expected success, got: " + ae, ae);
		}
	}

	// ---------------------------------------------------------------------
	// 4. Exp-op context breadth: expression read ops.
	//
	// Python: test_ael_expop_error_details.py CASES_READOP_* (behavior-level
	// rows; the AEL source diagnostics themselves are not portable).
	// ---------------------------------------------------------------------

	@Test
	public void testExpReadBuildFailureTrace() {
		AerospikeException ae = expectOperateError(stdKey, 3, ResultCode.PARAMETER_ERROR,
			ExpOperation.read("result", buildErrorExp(), ExpReadFlags.DEFAULT));

		assertEquals(SubCode.NONE, ae.getSubCode());
		assertMessageContains(ae, "invalid expression in operation request");
		assertBuildTrace(ae);
	}

	@Test
	public void testExpReadNonBoolRootLegal() {
		// A non-boolean root is illegal for a filter but is the whole point of
		// a value read: $.x + 1 -> 11, rc 0, no error details.
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 3;

		Record record = client.operate(wp, stdKey,
			ExpOperation.read("result", Exp.build(Exp.add(Exp.intBin(BIN_INT), Exp.val(1))),
				ExpReadFlags.DEFAULT));

		assertNotNull(record);
		assertEquals(11, record.getInt("result"));
	}

	@Test
	public void testExpReadInvalidFlagsNoDetails() {
		// A write-only policy flag on a read op is rejected structurally
		// before any expression is built: rc 4 with NO error details staged.
		AerospikeException ae = expectOperateError(stdKey, 3, ResultCode.PARAMETER_ERROR,
			ExpOperation.read("result", Exp.build(Exp.add(Exp.intBin(BIN_INT), Exp.val(1))),
				ExpWriteFlags.CREATE_ONLY));

		assertNoDetails(ae, ResultCode.PARAMETER_ERROR);
	}

	@Test
	public void testExpReadEvalFaultDivByZero() {
		AerospikeException ae = expectOperateError(stdKey, 3, ResultCode.OP_NOT_APPLICABLE,
			ExpOperation.read("result", Exp.build(Exp.div(Exp.intBin(BIN_INT), Exp.val(0))),
				ExpReadFlags.DEFAULT));

		assertEquals(SubCode.NONE, ae.getSubCode());
		assertMessageContains(ae, "integer division by zero");
		assertEvalTrace(ae, "div", 1, new String[] {"div"});
	}

	@Test
	public void testExpReadCdtOutOfBoundsSubcode() {
		AerospikeException ae = expectOperateError(stdKey, 3, ResultCode.OP_NOT_APPLICABLE,
			ExpOperation.read("result", Exp.build(cdtOobExp()), ExpReadFlags.DEFAULT));

		assertEquals(SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS, ae.getSubCode());
		assertMessageContains(ae, "out of bounds");
		assertEvalTrace(ae, "call", 1, new String[] {"call"});
	}

	@Test
	public void testExpReadAbsentBin() {
		AerospikeException ae = expectOperateError(stdKey, 3, ResultCode.OP_NOT_APPLICABLE,
			ExpOperation.read("result", Exp.build(Exp.intBin(BIN_MISSING)), ExpReadFlags.DEFAULT));

		assertEquals(SubCode.NONE, ae.getSubCode());
		assertMessageContains(ae, "expression references an absent bin or key");
		assertEvalTrace(ae, "bin", 1, new String[] {"bin"});
	}

	@Test
	public void testExpReadWrongTypeBinReadsAbsent() {
		// A present bin read at the wrong type folds to UNK -> ABSENT
		// (BIN_FLOAT holds a float, read as INT).
		AerospikeException ae = expectOperateError(stdKey, 3, ResultCode.OP_NOT_APPLICABLE,
			ExpOperation.read("result", Exp.build(Exp.intBin(BIN_FLOAT)), ExpReadFlags.DEFAULT));

		assertEquals(SubCode.NONE, ae.getSubCode());
		assertMessageContains(ae, "expression references an absent bin or key");
		assertEvalTrace(ae, "bin", 1, new String[] {"bin"});
	}

	@Test
	public void testExpReadUnknownLiteralReadsAbsent() {
		// A bare unknown() produces no value -> ABSENT; op=unknown.
		AerospikeException ae = expectOperateError(stdKey, 3, ResultCode.OP_NOT_APPLICABLE,
			ExpOperation.read("result", Exp.build(Exp.unknown()), ExpReadFlags.DEFAULT));

		assertEquals(SubCode.NONE, ae.getSubCode());
		assertMessageContains(ae, "expression references an absent bin or key");
		assertEvalTrace(ae, "unknown", 1, new String[] {"unknown"});
	}

	@Test
	public void testExpReadEvalNoFailSwallowsAbsent() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 3;

		Record record = client.operate(wp, stdKey,
			ExpOperation.read("result", Exp.build(Exp.intBin(BIN_MISSING)),
				ExpReadFlags.EVAL_NO_FAIL));

		assertNotNull(record);
	}

	@Test
	public void testExpReadEvalNoFailSwallowsFault() {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 3;

		Record record = client.operate(wp, stdKey,
			ExpOperation.read("result", Exp.build(Exp.div(Exp.intBin(BIN_INT), Exp.val(0))),
				ExpReadFlags.EVAL_NO_FAIL));

		assertNotNull(record);
	}

	// ---------------------------------------------------------------------
	// 4 (continued). Exp-op context breadth: expression write ops.
	//
	// Python: test_ael_expop_error_details.py CASES_WRITEOP_FAULT and
	// CASES_WRITEOP_POLICY.
	// ---------------------------------------------------------------------

	@Test
	public void testExpWriteEvalFaultDivByZero() {
		reseedScratch();

		AerospikeException ae = expectOperateError(scratchKey, 3, ResultCode.OP_NOT_APPLICABLE,
			ExpOperation.write("wb", Exp.build(Exp.div(Exp.intBin(BIN_INT), Exp.val(0))),
				ExpWriteFlags.DEFAULT));

		assertEquals(SubCode.NONE, ae.getSubCode());
		assertMessageContains(ae, "integer division by zero");
		assertEvalTrace(ae, "div", 1, new String[] {"div"});
	}

	@Test
	public void testExpWriteCdtOutOfBoundsSubcode() {
		AerospikeException ae = expectOperateError(stdKey, 3, ResultCode.OP_NOT_APPLICABLE,
			ExpOperation.write("wb", Exp.build(cdtOobExp()), ExpWriteFlags.DEFAULT));

		assertEquals(SubCode.OPNOT_CDT_INDEX_OUT_OF_BOUNDS, ae.getSubCode());
		assertMessageContains(ae, "out of bounds");
		assertEvalTrace(ae, "call", 1, new String[] {"call"});
	}

	@Test
	public void testExpWriteCreateOnlyExistingBinNoDetails() {
		// A policy rejection is not an expression diagnostic: rc 6 with NO
		// error details staged, even at verbosity 3.
		reseedScratch();

		AerospikeException ae = expectOperateError(scratchKey, 3, ResultCode.BIN_EXISTS_ERROR,
			ExpOperation.write(BIN_INT, Exp.build(Exp.add(Exp.intBin(BIN_INT), Exp.val(1))),
				ExpWriteFlags.CREATE_ONLY));

		assertNoDetails(ae, ResultCode.BIN_EXISTS_ERROR);
	}

	@Test
	public void testExpWriteUpdateOnlyMissingBinNoDetails() {
		reseedScratch();

		AerospikeException ae = expectOperateError(scratchKey, 3, ResultCode.BIN_NOT_FOUND,
			ExpOperation.write(BIN_MISSING, Exp.build(Exp.val(1)), ExpWriteFlags.UPDATE_ONLY));

		assertNoDetails(ae, ResultCode.BIN_NOT_FOUND);
	}

	@Test
	public void testExpWriteNilWithoutAllowDeleteNoDetails() {
		// A NIL result would delete the target bin; without ALLOW_DELETE that
		// is OP_NOT_APPLICABLE with NO error details (contrast the eval-fault
		// rows above, which stage message + trace under the same rc).
		reseedScratch();

		AerospikeException ae = expectOperateError(scratchKey, 3, ResultCode.OP_NOT_APPLICABLE,
			ExpOperation.write(BIN_INT, Exp.build(Exp.nil()), ExpWriteFlags.DEFAULT));

		assertNoDetails(ae, ResultCode.OP_NOT_APPLICABLE);
	}

	@Test
	public void testExpWriteNilAllowDeleteDeletesBin() {
		reseedScratch();

		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 3;

		client.operate(wp, scratchKey,
			ExpOperation.write(BIN_INT, Exp.build(Exp.nil()), ExpWriteFlags.ALLOW_DELETE));

		Record record = client.get(null, scratchKey);
		assertNotNull(record);
		assertNull("Expected bin to be deleted", record.getValue(BIN_INT));
		assertNotNull("Expected untouched bin to remain", record.getValue("keep"));
	}

	@Test
	public void testExpWritePolicyNoFailSwallowsViolation() {
		reseedScratch();

		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 3;

		client.operate(wp, scratchKey,
			ExpOperation.write(BIN_INT, Exp.build(Exp.add(Exp.intBin(BIN_INT), Exp.val(1))),
				ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL));

		// The CREATE_ONLY violation was swallowed; the bin is unchanged.
		Record record = client.get(null, scratchKey);
		assertNotNull(record);
		assertEquals(10, record.getInt(BIN_INT));
	}

	@Test
	public void testExpWriteEvalNoFailSwallowsFault() {
		reseedScratch();

		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 3;

		Record record = client.operate(wp, scratchKey,
			ExpOperation.write(BIN_INT, Exp.build(Exp.div(Exp.intBin(BIN_INT), Exp.val(0))),
				ExpWriteFlags.EVAL_NO_FAIL));

		assertNotNull(record);
	}
}

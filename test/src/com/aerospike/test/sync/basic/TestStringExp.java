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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.SubCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.exp.StringExp;
import com.aerospike.client.operation.StringNumericType;
import com.aerospike.client.operation.StringPolicy;
import com.aerospike.client.operation.StringRegexFlags;
import com.aerospike.client.operation.StringWriteFlags;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.util.Unpacker;
import com.aerospike.test.sync.TestSync;

/**
 * Integration tests for the string filter-expression builders exposed by
 * {@link StringExp}. Each test puts a representative bin, builds an
 * {@link Expression} that wraps a {@code StringExp.*} call, evaluates it via
 * {@link ExpOperation#read} into a virtual bin, and asserts the result.
 *
 * <p>String expressions require server version 8.2.0+; the tests are skipped
 * on older clusters via {@link Assume}.
 *
 * <p>Unlike {@link com.aerospike.client.operation.StringOperation}, the
 * expression path does <strong>not</strong> take a CTX directly. To target a
 * string nested in a list/map, callers project the nested value via
 * {@link ListExp#getByIndex} or {@link MapExp#getByKey} and feed the result
 * as {@code src}. Two such cases are exercised at the end of this file.
 */
public class TestStringExp extends TestSync {
	private static final String BIN = "sbin";
	private static final String VAR = "v";
	private static final Key KEY = new Key(args.namespace, args.set, "stringexp-key");
	private static final StringPolicy POLICY = StringPolicy.Default;

	@BeforeClass
	public static void serverVersionCheck() {
		Assume.assumeTrue(
			"Skipping: string expressions require server version 8.2.0 or later",
			args.serverVersion.isGreaterOrEqual(8, 2, 0, 0));
	}

	//-----------------------------------------------------------------
	// Helpers
	//-----------------------------------------------------------------

	private static void put(String value) {
		client.delete(null, KEY);
		client.put(null, KEY, new Bin(BIN, value));
	}

	private static void putRaw(Bin bin) {
		client.delete(null, KEY);
		client.put(null, KEY, bin);
	}

	private static Record eval(Exp e) {
		return client.operate(null, KEY,
			ExpOperation.read(VAR, Exp.build(e), ExpReadFlags.DEFAULT));
	}

	/**
	 * Evaluate at error-detail verbosity 2. The expression runtime collapses every
	 * sub-op failure into one generic fault (exp_rt.c eval_call -> EXP_ERR_CALL_ARG),
	 * so OP_NOT_APPLICABLE is all the result code ever says on this path; the sub-code
	 * and staged message are what identify the actual rejection.
	 */
	private static Record evalDetailed(Exp e) {
		WritePolicy wp = new WritePolicy();
		wp.errorDetailVerbosity = 2;
		return client.operate(wp, KEY,
			ExpOperation.read(VAR, Exp.build(e), ExpReadFlags.DEFAULT));
	}

	//=================================================================
	// Read expressions
	//=================================================================

	@Test
	public void strlenReturnsCodepointCount() {
		put("hello world");
		Record r = eval(StringExp.strlen(Exp.stringBin(BIN)));
		assertEquals(11L, r.getLong(VAR));
	}

	@Test
	public void substrFromOffsetAndRange() {
		put("hello world");
		// Single-arg form: offset to end.
		Record r1 = eval(StringExp.substr(Exp.val(6), Exp.stringBin(BIN)));
		assertEquals("world", r1.getString(VAR));
		// Two-arg form: [start, end) — end is exclusive.
		Record r2 = eval(StringExp.substr(Exp.val(0), Exp.val(5), Exp.stringBin(BIN)));
		assertEquals("hello", r2.getString(VAR));
	}

	@Test
	public void charAtReturnsSingleCharacter() {
		put("Hello123World");
		Record r = eval(StringExp.charAt(Exp.val(5), Exp.stringBin(BIN)));
		assertEquals("1", r.getString(VAR));
	}

	@Test
	public void findReturnsIndexOfFirstAndNthMatch() {
		put("ababab");
		// Default (first match).
		Record r1 = eval(StringExp.find(Exp.val("ab"), Exp.stringBin(BIN)));
		assertEquals(0L, r1.getLong(VAR));
		// Occurrence overload (1-based) — second occurrence starts at index 2.
		Record r2 = eval(StringExp.find(Exp.val("ab"), Exp.val(2), Exp.stringBin(BIN)));
		assertEquals(2L, r2.getLong(VAR));
	}

	@Test
	public void findSkipsOverlappingMatches() {
		// Self-overlapping needle "aa" in "aaaa": after match at 0, search
		// resumes at 2 — so the 2nd occurrence is at 2, not 1. Mirrors the
		// StringOperation.find contract and ICU usearch behavior.
		put("aaaa");
		assertEquals(0L,
			eval(StringExp.find(Exp.val("aa"), Exp.val(1), Exp.stringBin(BIN))).getLong(VAR));
		assertEquals(2L,
			eval(StringExp.find(Exp.val("aa"), Exp.val(2), Exp.stringBin(BIN))).getLong(VAR));
		assertEquals(-1L,
			eval(StringExp.find(Exp.val("aa"), Exp.val(3), Exp.stringBin(BIN))).getLong(VAR));
	}

	@Test
	public void containsReturnsBoolean() {
		put("hello world");
		Record present = eval(StringExp.contains(Exp.val("hello"), Exp.stringBin(BIN)));
		Record absent = eval(StringExp.contains(Exp.val("xyz"), Exp.stringBin(BIN)));
		assertTrue(present.getBoolean(VAR));
		assertFalse(absent.getBoolean(VAR));
	}

	@Test
	public void startsWithMatchesPrefix() {
		put("Hello123World");
		assertTrue(eval(StringExp.startsWith(Exp.val("Hello"), Exp.stringBin(BIN))).getBoolean(VAR));
		assertFalse(eval(StringExp.startsWith(Exp.val("World"), Exp.stringBin(BIN))).getBoolean(VAR));
	}

	@Test
	public void endsWithMatchesSuffix() {
		put("Hello123World");
		assertTrue(eval(StringExp.endsWith(Exp.val("World"), Exp.stringBin(BIN))).getBoolean(VAR));
		assertFalse(eval(StringExp.endsWith(Exp.val("Hello"), Exp.stringBin(BIN))).getBoolean(VAR));
	}

	@Test
	public void toIntegerParsesDigitsAsLong() {
		put("12345");
		Record r = eval(StringExp.toInteger(Exp.stringBin(BIN)));
		assertEquals(12345L, r.getLong(VAR));
	}

	@Test
	public void toDoubleParsesDecimalNumbers() {
		put("3.14");
		Record r = eval(StringExp.toDouble(Exp.stringBin(BIN)));
		assertEquals(3.14, r.getDouble(VAR), 0.001);
	}

	@Test
	public void byteLengthReturnsUtf8Bytes() {
		put("hello");
		Record r = eval(StringExp.byteLength(Exp.stringBin(BIN)));
		assertEquals(5L, r.getLong(VAR));
	}

	//-----------------------------------------------------------------
	// Codepoint-vs-byte anchors (mirror of TestOperateString)
	//-----------------------------------------------------------------

	@Test
	public void strlenCountsCodepointsAndByteLengthCountsBytes() {
		// "café" = 4 codepoints, 5 UTF-8 bytes; "日本語" = 3 codepoints, 9 bytes.
		put("café");
		assertEquals(4L, eval(StringExp.strlen(Exp.stringBin(BIN))).getLong(VAR));
		assertEquals(5L, eval(StringExp.byteLength(Exp.stringBin(BIN))).getLong(VAR));

		put("日本語");
		assertEquals(3L, eval(StringExp.strlen(Exp.stringBin(BIN))).getLong(VAR));
		assertEquals(9L, eval(StringExp.byteLength(Exp.stringBin(BIN))).getLong(VAR));
	}

	@Test
	public void charAtReturnsWholeSupplementaryCodepoint() {
		// 👋 is U+1F44B (4 UTF-8 bytes, a surrogate pair in Java UTF-16).
		// charAt must return the whole codepoint, not a half-surrogate.
		put("a👋b");
		Record r = eval(StringExp.charAt(Exp.val(1), Exp.stringBin(BIN)));
		assertEquals("👋", r.getString(VAR));
	}

	@Test
	public void isNumericMatchesIntegerStringsByDefaultAndByType() {
		put("12345");
		// Default (ANY): both ints and floats pass.
		assertTrue(eval(StringExp.isNumeric(Exp.stringBin(BIN))).getBoolean(VAR));
		// INT-only: still passes for pure-digit string.
		assertTrue(eval(StringExp.isNumeric(StringNumericType.INT, Exp.stringBin(BIN))).getBoolean(VAR));
		put("3.14");
		// INT-only: fails for a float-shaped string.
		assertFalse(eval(StringExp.isNumeric(StringNumericType.INT, Exp.stringBin(BIN))).getBoolean(VAR));
		put("hello");
		assertFalse(eval(StringExp.isNumeric(Exp.stringBin(BIN))).getBoolean(VAR));
	}

	@Test
	public void isNumericFloatRequiresFractionalDigit() {
		put("3.14");
		assertTrue(eval(StringExp.isNumeric(StringNumericType.FLOAT, Exp.stringBin(BIN))).getBoolean(VAR));
		put("5");
		// FLOAT needs a '.' followed by a digit, so a pure-digit string fails
		// it while ANY still passes via the integer branch.
		assertFalse(eval(StringExp.isNumeric(StringNumericType.FLOAT, Exp.stringBin(BIN))).getBoolean(VAR));
		assertTrue(eval(StringExp.isNumeric(StringNumericType.ANY, Exp.stringBin(BIN))).getBoolean(VAR));
		put("1e5");
		assertFalse(eval(StringExp.isNumeric(StringNumericType.FLOAT, Exp.stringBin(BIN))).getBoolean(VAR));
	}

	@Test
	public void isUpperAndIsLowerDistinguishCase() {
		put("HELLO");
		assertTrue(eval(StringExp.isUpper(Exp.stringBin(BIN))).getBoolean(VAR));
		assertFalse(eval(StringExp.isLower(Exp.stringBin(BIN))).getBoolean(VAR));

		put("hello");
		assertFalse(eval(StringExp.isUpper(Exp.stringBin(BIN))).getBoolean(VAR));
		assertTrue(eval(StringExp.isLower(Exp.stringBin(BIN))).getBoolean(VAR));
	}

	@Test
	public void toBlobReturnsUtf8Bytes() {
		put("hello");
		Record r = eval(StringExp.toBlob(Exp.stringBin(BIN)));
		assertArrayEquals("hello".getBytes(), (byte[])r.getValue(VAR));
	}

	@Test
	public void splitWithAndWithoutSeparator() {
		put("one,two,three");
		Record r1 = eval(StringExp.split(Exp.val(","), Exp.stringBin(BIN)));
		assertEquals(Arrays.asList("one", "two", "three"), r1.getList(VAR));

		// No-separator form: per spec §2.4, returns one element per Unicode codepoint.
		put("abc");
		Record r2 = eval(StringExp.split(Exp.stringBin(BIN)));
		assertEquals(Arrays.asList("a", "b", "c"), r2.getList(VAR));
	}

	@Test
	public void b64DecodeReturnsOriginalBlob() {
		put("aGVsbG8=");
		Record r = eval(StringExp.b64Decode(Exp.stringBin(BIN)));
		assertArrayEquals("hello".getBytes(), (byte[])r.getValue(VAR));
	}

	@Test
	public void regexCompareWithAndWithoutCaseInsensitiveFlag() {
		put("Hello123World");
		assertTrue(eval(StringExp.regexCompare(
			Exp.val("[0-9]+"), Exp.stringBin(BIN))).getBoolean(VAR));

		put("HELLO");
		assertFalse(eval(StringExp.regexCompare(
			Exp.val("hello"), Exp.stringBin(BIN))).getBoolean(VAR));
		assertTrue(eval(StringExp.regexCompare(
			Exp.val("hello"), StringRegexFlags.CASE_INSENSITIVE,
			Exp.stringBin(BIN))).getBoolean(VAR));
	}

	// Note: a literal-source variant (e.g. StringExp.regexCompare(Exp.val("[A-Z]+"),
	// Exp.val("HELLO"))) is not exercised here. The server's expression engine returns
	// OP_NOT_APPLICABLE (26) for that shape — the engine evaluates the literal but does
	// not tag the resulting value as a STRING particle, so string_read's type check at
	// particle_string.c:1040 rejects it. Spec §3.7 claims any string-yielding expression
	// is accepted; the server does not honor that today. Bin-sourced regexCompare is
	// covered in regexCompareWithAndWithoutCaseInsensitiveFlag above.

	//=================================================================
	// Modify expressions (return the modified string; do not persist)
	//=================================================================

	@Test
	public void insertSplicesIntoSource() {
		put("hello world");
		Record r = eval(StringExp.insert(
			POLICY, Exp.val(5), Exp.val(" beautiful"), Exp.stringBin(BIN)));
		assertEquals("hello beautiful world", r.getString(VAR));
		// Modify expressions do not persist — original bin is unchanged.
		assertEquals("hello world", client.get(null, KEY).getString(BIN));
	}

	@Test
	public void overwriteReplacesRange() {
		put("hello world");
		Record r = eval(StringExp.overwrite(
			POLICY, Exp.val(6), Exp.val("earth"), Exp.stringBin(BIN)));
		assertEquals("hello earth", r.getString(VAR));
	}

	@Test
	public void concatAppendsListOfValues() {
		put("hello");
		Exp values = Exp.val(Arrays.asList(" ", "big", " world"));
		Record r = eval(StringExp.concat(POLICY, values, Exp.stringBin(BIN)));
		assertEquals("hello big world", r.getString(VAR));
	}

	@Test
	public void appendAddsValueToEnd() {
		put("hello");
		Record r = eval(StringExp.append(POLICY, Exp.val(" world"), Exp.stringBin(BIN)));
		assertEquals("hello world", r.getString(VAR));
	}

	@Test
	public void appendPreservesMultibyteCodepoints() {
		// Unicode/DBCS-aware: "日本" + "語" -> "日本語".
		put("日本");
		Record r = eval(StringExp.append(POLICY, Exp.val("語"), Exp.stringBin(BIN)));
		assertEquals("日本語", r.getString(VAR));
	}

	@Test
	public void prependAddsValueToStart() {
		put("world");
		Record r = eval(StringExp.prepend(POLICY, Exp.val("hello "), Exp.stringBin(BIN)));
		assertEquals("hello world", r.getString(VAR));
	}

	@Test
	public void prependPreservesMultibyteCodepoints() {
		// Unicode/DBCS-aware: "語" prepended with "日本" -> "日本語".
		put("語");
		Record r = eval(StringExp.prepend(POLICY, Exp.val("日本"), Exp.stringBin(BIN)));
		assertEquals("日本語", r.getString(VAR));
	}

	@Test
	public void snipRemovesRange() {
		put("hello beautiful world");
		Record r = eval(StringExp.snip(POLICY, Exp.val(5), Exp.val(15), Exp.stringBin(BIN)));
		assertEquals("hello world", r.getString(VAR));
	}

	@Test
	public void snipFromStartTruncatesToEnd() {
		put("hello world");
		Record r = eval(StringExp.snip(POLICY, Exp.val(5), Exp.stringBin(BIN)));
		assertEquals("hello", r.getString(VAR));
	}

	@Test
	public void snipFromNegativeStartCountsFromEnd() {
		put("hello world");
		Record r = eval(StringExp.snip(POLICY, Exp.val(-5), Exp.stringBin(BIN)));
		assertEquals("hello ", r.getString(VAR));
	}

	@Test
	public void snipFromPacksStartWithoutFlagsElement() {
		// The server parses snip's arguments positionally (start, end, flags), so a
		// trailing flags element on the 1-index form is read as `end`. Exp.Module packs
		// the module payload verbatim as element 3 of the CALL array, so unpacking the
		// built expression recovers the argument list exactly as it goes on the wire.
		assertEquals(Arrays.asList(53L, 5L), snipCallArgs(
			StringExp.snip(POLICY, Exp.val(5), Exp.stringBin(BIN))));

		assertEquals(Arrays.asList(53L, 5L, 15L, 0L), snipCallArgs(
			StringExp.snip(POLICY, Exp.val(5), Exp.val(15), Exp.stringBin(BIN))));
	}

	private static List<?> snipCallArgs(Exp e) {
		byte[] blob = Exp.build(e).getBytes();
		List<?> call = (List<?>)Unpacker.unpackObjectList(blob, 0, blob.length);
		return (List<?>)call.get(3);
	}

	@Test
	public void replaceTouchesOnlyFirstMatch() {
		put("hello world world");
		Record r = eval(StringExp.replace(
			POLICY, Exp.val("world"), Exp.val("earth"), Exp.stringBin(BIN)));
		assertEquals("hello earth world", r.getString(VAR));
	}

	@Test
	public void replaceAllSubstitutesEveryMatch() {
		put("aabaa");
		Record r = eval(StringExp.replaceAll(
			POLICY, Exp.val("a"), Exp.val("x"), Exp.stringBin(BIN)));
		assertEquals("xxbxx", r.getString(VAR));
	}

	@Test
	public void upperAndLowerProduceCorrectCase() {
		put("hello World");
		assertEquals("HELLO WORLD",
			eval(StringExp.upper(POLICY, Exp.stringBin(BIN))).getString(VAR));
		assertEquals("hello world",
			eval(StringExp.lower(POLICY, Exp.stringBin(BIN))).getString(VAR));
	}

	@Test
	public void caseFoldLowercasesIndependentlyOfLocale() {
		put("HELLO World");
		Record r = eval(StringExp.caseFold(POLICY, Exp.stringBin(BIN)));
		assertEquals("hello world", r.getString(VAR));
	}

	@Test
	public void normalizeNFCLeavesAlreadyNormalizedStringUnchanged() {
		put("hello");
		Record r = eval(StringExp.normalizeNFC(POLICY, Exp.stringBin(BIN)));
		assertEquals("hello", r.getString(VAR));
	}

	@Test
	public void trimVariantsStripAppropriateEdges() {
		put("  hello world  ");
		assertEquals("hello world",
			eval(StringExp.trim(POLICY, Exp.stringBin(BIN))).getString(VAR));
		assertEquals("hello world  ",
			eval(StringExp.trimStart(POLICY, Exp.stringBin(BIN))).getString(VAR));
		assertEquals("  hello world",
			eval(StringExp.trimEnd(POLICY, Exp.stringBin(BIN))).getString(VAR));
	}

	@Test
	public void padStartFillsLeftToTargetLength() {
		put("hello");
		Record r = eval(StringExp.padStart(
			POLICY, Exp.val(10), Exp.val("*"), Exp.stringBin(BIN)));
		assertEquals("*****hello", r.getString(VAR));
	}

	@Test
	public void padEndFillsRightToTargetLength() {
		put("hello");
		Record r = eval(StringExp.padEnd(
			POLICY, Exp.val(10), Exp.val("."), Exp.stringBin(BIN)));
		assertEquals("hello.....", r.getString(VAR));
	}

	@Test
	public void repeatDuplicatesContents() {
		put("ab");
		Record r = eval(StringExp.repeat(POLICY, Exp.val(3), Exp.stringBin(BIN)));
		assertEquals("ababab", r.getString(VAR));
	}

	@Test
	public void regexReplaceFirstAndGlobal() {
		put("abc123def456");
		// Default: first match only.
		Record r1 = eval(StringExp.regexReplace(
			POLICY, Exp.val("[0-9]+"), Exp.val("NUM"),
			StringRegexFlags.DEFAULT, Exp.stringBin(BIN)));
		assertEquals("abcNUMdef456", r1.getString(VAR));

		// GLOBAL: every match.
		Record r2 = eval(StringExp.regexReplace(
			POLICY, Exp.val("[0-9]+"), Exp.val("NUM"),
			StringRegexFlags.GLOBAL, Exp.stringBin(BIN)));
		assertEquals("abcNUMdefNUM", r2.getString(VAR));
	}

	//=================================================================
	// Write flags on the expression path
	//
	// as_bin_string_modify_exp funnels into the same string_modify() as the
	// operate path, so the StringWriteFlags behave the same — the "bin" they
	// test is the value the source expression produced. CREATE_ONLY is accepted
	// only by the additive create-ops; CREATE_ONLY with UPDATE_ONLY, and any
	// flag outside an op's mask, are rejected during argument parsing, upstream
	// of every NO_FAIL test.
	//=================================================================

	private static final StringPolicy CREATE_ONLY =
		new StringPolicy(StringWriteFlags.CREATE_ONLY);
	private static final StringPolicy UPDATE_ONLY =
		new StringPolicy(StringWriteFlags.UPDATE_ONLY);
	private static final StringPolicy NO_FAIL =
		new StringPolicy(StringWriteFlags.NO_FAIL);

	private static AerospikeException assertEvalFails(Exp e, int subCode, String message) {
		AerospikeException ae = assertThrows(AerospikeException.class, () -> evalDetailed(e));
		assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());
		assertEquals(subCode, ae.getSubCode());
		assertEquals(message, ae.getBaseMessage());
		return ae;
	}

	private static void assertStringParamError(Exp e, String message) {
		assertEvalFails(e, SubCode.PARAM_STRING_OP_PARAMS_INVALID, message);
	}

	@Test
	public void updateOnlyAppliesToLiveSource() {
		put("hello");
		Record r = eval(StringExp.append(UPDATE_ONLY, Exp.val(" world"), Exp.stringBin(BIN)));
		assertEquals("hello world", r.getString(VAR));
	}

	@Test
	public void updateOnlyAppliesToNonCreateModifyOp() {
		// UPDATE_ONLY is valid on every string modify op, not just the
		// create-capable ones.
		put("hello");
		Record r = eval(StringExp.upper(UPDATE_ONLY, Exp.stringBin(BIN)));
		assertEquals("HELLO", r.getString(VAR));
	}

	@Test
	public void createOnlyOnLiveSourceIsRejected() {
		// The source expression yielded a live value, so CREATE_ONLY has nothing
		// to create. Reported with no sub-code: the server raises this one inside
		// string_modify, past the argument parser that stages the param sub-code.
		put("hello");
		assertEvalFails(
			StringExp.append(CREATE_ONLY, Exp.val("!"), Exp.stringBin(BIN)),
			SubCode.NONE, "string_append: value exists but CREATE_ONLY flag is set");
	}

	@Test
	public void createOnlyWithNoFailOnLiveSourceYieldsUnmodifiedSource() {
		// The one CREATE_ONLY rejection NO_FAIL does suppress — it is tested
		// inside string_modify, not during argument parsing.
		put("hello");
		StringPolicy policy = new StringPolicy(
			StringWriteFlags.CREATE_ONLY | StringWriteFlags.NO_FAIL);
		Record r = eval(StringExp.append(policy, Exp.val("!"), Exp.stringBin(BIN)));
		assertEquals("hello", r.getString(VAR));
	}

	@Test
	public void createOnlyOnNonCreateModifyOpRaisesParamError() {
		// upper carries the update-only flag mask, so CREATE_ONLY is not a legal
		// flag for it at all — distinguishing it from the additive create-ops.
		put("hello");
		assertStringParamError(
			StringExp.upper(CREATE_ONLY, Exp.stringBin(BIN)),
			"string_upper: flags 0x1 not valid for this op");
	}

	@Test
	public void createOnlyWithUpdateOnlyRaisesParamError() {
		put("hello");
		String message = "string_append: CREATE_ONLY and UPDATE_ONLY flags are mutually exclusive";

		StringPolicy both = new StringPolicy(
			StringWriteFlags.CREATE_ONLY | StringWriteFlags.UPDATE_ONLY);
		assertStringParamError(StringExp.append(both, Exp.val("!"), Exp.stringBin(BIN)), message);

		// NO_FAIL cannot suppress an argument-parse rejection.
		StringPolicy bothNoFail = new StringPolicy(
			StringWriteFlags.CREATE_ONLY | StringWriteFlags.UPDATE_ONLY
				| StringWriteFlags.NO_FAIL);
		assertStringParamError(StringExp.append(bothNoFail, Exp.val("!"), Exp.stringBin(BIN)), message);
	}

	@Test
	public void noFailSuppressesPrepareFailure() {
		put("hello");
		// An empty pad string is rejected by padStart's prepare stage.
		assertStringParamError(
			StringExp.padStart(POLICY, Exp.val(10), Exp.val(""), Exp.stringBin(BIN)),
			"string_pad_start: target length 10 must be non-negative and pad string must not be empty");

		Record r = eval(StringExp.padStart(
			NO_FAIL, Exp.val(10), Exp.val(""), Exp.stringBin(BIN)));
		assertEquals("hello", r.getString(VAR));
	}

	@Test
	public void regexReplaceWithInvalidPatternIsRejected() {
		put("hello");
		assertEvalFails(
			StringExp.regexReplace(POLICY, Exp.val("("), Exp.val("X"),
				StringRegexFlags.DEFAULT, Exp.stringBin(BIN)),
			SubCode.PARAM_STRING_REGEX_INVALID,
			"string_regex_replace: regex pattern is invalid or could not be compiled");
	}

	@Test
	public void regexReplaceNoFailSuppressesInvalidPattern() {
		// Proves the policy flags reach the wire: the identical call under
		// StringPolicy.Default faults in the test above.
		put("hello");
		Record r = eval(StringExp.regexReplace(
			NO_FAIL, Exp.val("("), Exp.val("X"),
			StringRegexFlags.DEFAULT, Exp.stringBin(BIN)));
		assertEquals("hello", r.getString(VAR));
	}

	@Test
	public void regexReplacePacksRegexFlagsAheadOfPolicyFlags() {
		// DOTALL and NO_FAIL are both 1 << 2, and MULTILINE and UPDATE_ONLY are
		// both 1 << 1, so the two arguments are only told apart by behaviour.
		// DOTALL must land in the regex slot for "." to span the newline; sent in
		// the other order the server compiles the pattern MULTILINE and it does
		// not match.
		put("a\nb");
		Record r = eval(StringExp.regexReplace(
			UPDATE_ONLY, Exp.val("a.b"), Exp.val("X"),
			StringRegexFlags.DOTALL, Exp.stringBin(BIN)));
		assertEquals("X", r.getString(VAR));
	}

	@Test
	public void regexReplacePacksFourElementsWithPolicyLast() {
		// Exp.Module packs the module payload verbatim as element 3 of the CALL
		// array, so unpacking the built expression recovers the argument list
		// exactly as it goes on the wire.
		byte[] blob = Exp.build(StringExp.regexReplace(
			NO_FAIL, Exp.val("[0-9]+"), Exp.val("NUM"),
			StringRegexFlags.GLOBAL, Exp.stringBin(BIN))).getBytes();
		List<?> call = (List<?>)Unpacker.unpackObjectList(blob, 0, blob.length);
		List<?> args = (List<?>)call.get(3);

		assertEquals(4, args.size());
		assertEquals(66L, args.get(0));
		assertEquals(Arrays.asList("[0-9]+", "NUM"), ((List<?>)args.get(1)).get(1));
		assertEquals((long)StringRegexFlags.GLOBAL, args.get(2));
		assertEquals((long)StringWriteFlags.NO_FAIL, args.get(3));
	}

	//=================================================================
	// Type conversion expression
	//=================================================================

	@Test
	public void toStringConvertsIntegerBin() {
		putRaw(new Bin(BIN, 42));
		Record r = eval(StringExp.toString(Exp.intBin(BIN)));
		assertEquals("42", r.getString(VAR));
	}

	//=================================================================
	// Chained expressions — modify result feeds another StringExp
	//=================================================================

	@Test
	public void chainedTrimThenUpperComposes() {
		put("  hello world  ");
		// trim -> upper, both inside one expression tree.
		Exp chain = StringExp.upper(
			POLICY,
			StringExp.trim(POLICY, Exp.stringBin(BIN)));
		Record r = eval(chain);
		assertEquals("HELLO WORLD", r.getString(VAR));
	}

	//=================================================================
	// Filter-expression usage — predicate gates record retrieval
	//=================================================================

	@Test
	public void startsWithFilterGatesGet() {
		put("hello world");
		Policy p = new Policy();

		// Matching filter -> record returned.
		p.filterExp = Exp.build(StringExp.startsWith(
			Exp.val("hello"), Exp.stringBin(BIN)));
		assertEquals("hello world", client.get(p, KEY).getString(BIN));

		// Non-matching filter -> filtered out, get returns null.
		p.filterExp = Exp.build(StringExp.startsWith(
			Exp.val("world"), Exp.stringBin(BIN)));
		assertEquals(null, client.get(p, KEY));
	}

	//=================================================================
	// Nested-source — string inside a list/map projected via Exp getters
	//
	// StringExp does not accept CTX directly; callers compose with
	// ListExp.getByIndex / MapExp.getByKey to project the nested string
	// into an Exp and pass it as src.
	//=================================================================

	@Test
	public void strlenOnStringNestedInListProjectedViaListExp() {
		List<Value> list = new ArrayList<Value>();
		list.add(Value.get("alpha"));
		list.add(Value.get("beta"));
		list.add(Value.get("hello world"));
		client.delete(null, KEY);
		client.put(null, KEY, new Bin(BIN, list));

		Exp nestedString = ListExp.getByIndex(
			ListReturnType.VALUE, Exp.Type.STRING, Exp.val(2), Exp.listBin(BIN));
		Record r = eval(StringExp.strlen(nestedString));
		assertEquals(11L, r.getLong(VAR));
	}

	@Test
	public void upperOnStringNestedInMapProjectedViaMapExp() {
		Map<Value, Value> map = new HashMap<Value, Value>();
		map.put(Value.get("a"), Value.get("hello"));
		map.put(Value.get("b"), Value.get("world"));
		client.delete(null, KEY);
		client.put(null, KEY, new Bin(BIN, map));

		Exp nestedString = MapExp.getByKey(
			MapReturnType.VALUE, Exp.Type.STRING, Exp.val("a"), Exp.mapBin(BIN));
		Record r = eval(StringExp.upper(POLICY, nestedString));
		assertEquals("HELLO", r.getString(VAR));
	}
}

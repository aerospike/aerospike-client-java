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
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.operation.StringNumericType;
import com.aerospike.client.operation.StringOperation;
import com.aerospike.client.operation.StringPolicy;
import com.aerospike.client.operation.StringRegexFlags;
import com.aerospike.client.operation.StringWriteFlags;
import com.aerospike.client.util.Unpacker;
import com.aerospike.test.sync.TestSync;

/**
 * Integration tests for the string operations exposed by {@link StringOperation}.
 *
 * <p>The tests are organized around the operation behavior they verify rather
 * than around individual API methods, so each test exercises a single intent
 * (e.g. "uppercase mutates the bin", "find returns the first match index").
 *
 * <p>String operations require server version 8.1.3+; the tests are skipped
 * on older clusters via {@link Assume}.
 */
public class TestOperateString extends TestSync {
	private static final String BIN = "sbin";
	private static final Key KEY = new Key(args.namespace, args.set, "stringop-key");
	private static final StringPolicy POLICY = StringPolicy.Default;
	// Ceiling the server puts on a modify op's estimated result size.
	private static final int RESULT_SIZE_CAP = 8 * 1024 * 1024;

	@BeforeClass
	public static void serverVersionCheck() {
		Assume.assumeTrue(
			"Skipping: string operations require server version 8.1.3 or later",
			args.serverVersion.isGreaterOrEqual(8, 1, 3, 0));
	}

	//-----------------------------------------------------------------
	// Helpers
	//-----------------------------------------------------------------

	private static void put(String value) {
		client.delete(null, KEY);
		client.put(null, KEY, new Bin(BIN, value));
	}

	private static void put(Bin... bins) {
		client.delete(null, KEY);
		client.put(null, KEY, bins);
	}

	private static Record operate(Operation... ops) {
		return client.operate(null, KEY, ops);
	}

	private static String stringValue() {
		return client.get(null, KEY).getString(BIN);
	}

	//=================================================================
	// Read operations
	//=================================================================

	@Test
	public void strlenReturnsCodepointCount() {
		put("hello world");
		Record r = operate(StringOperation.strlen(BIN));
		assertEquals(11L, r.getLong(BIN));
	}

	@Test
	public void strlenOnEmptyStringIsZero() {
		put("");
		Record r = operate(StringOperation.strlen(BIN));
		assertEquals(0L, r.getLong(BIN));
	}

	@Test
	public void byteLengthReturnsUtf8Bytes() {
		put("hello");
		Record r = operate(StringOperation.byteLength(BIN));
		assertEquals(5L, r.getLong(BIN));
	}

	//-----------------------------------------------------------------
	// Multi-byte / codepoint-vs-byte tests
	//
	// Server-side indices and strlen are in Unicode code points, not bytes
	// and not Java UTF-16 chars. These tests anchor the contract for Java
	// callers whose String.length() intuition is UTF-16 code-unit count.
	//-----------------------------------------------------------------

	@Test
	public void strlenCountsCodepointsNotJavaChars() {
		// "café" = 4 codepoints; UTF-8 = 5 bytes; Java .length() = 4.
		put("café");
		assertEquals(4L, operate(StringOperation.strlen(BIN)).getLong(BIN));

		// "日本語" = 3 codepoints; UTF-8 = 9 bytes; Java .length() = 3.
		put("日本語");
		assertEquals(3L, operate(StringOperation.strlen(BIN)).getLong(BIN));

		// "👋hi" — emoji is U+1F44B, a supplementary codepoint encoded as
		// a UTF-16 surrogate pair in Java. Codepoints = 3; Java .length() = 4.
		put("👋hi");
		assertEquals(3L, operate(StringOperation.strlen(BIN)).getLong(BIN));
	}

	@Test
	public void byteLengthCountsBytesNotCodepoints() {
		put("café");
		assertEquals(5L, operate(StringOperation.byteLength(BIN)).getLong(BIN));
		put("日本語");
		assertEquals(9L, operate(StringOperation.byteLength(BIN)).getLong(BIN));
		put("👋hi");
		// 👋 = 4 UTF-8 bytes, "hi" = 2 bytes.
		assertEquals(6L, operate(StringOperation.byteLength(BIN)).getLong(BIN));
	}

	@Test
	public void substrIndexesCodepointsNotBytes() {
		// "日本語hi" — substr(start=3, end=5) returns codepoints 3..4 = "hi".
		// A byte-indexed substr would land mid-way through "日" (each CJK char
		// occupies 3 UTF-8 bytes).
		put("日本語hi");
		Record r = operate(StringOperation.substr(BIN, 3, 5));
		assertEquals("hi", r.getString(BIN));
	}

	@Test
	public void charAtReturnsWholeCodepoint() {
		// charAt at the emoji position should return the full 4-byte codepoint,
		// not a half-surrogate.
		put("a👋b");
		Record r = operate(StringOperation.charAt(BIN, 1));
		assertEquals("👋", r.getString(BIN));
	}

	@Test
	public void findReturnsCodepointIndex() {
		// "café-world": "world" starts at codepoint 5 (UTF-16 .indexOf would
		// also return 5 here because "é" is a single Java char, but the contract
		// is codepoint-indexed).
		put("café-world");
		assertEquals(5L, operate(StringOperation.find(BIN, "world")).getLong(BIN));

		// "👋-world": "world" starts at codepoint index 2 (after emoji and dash).
		// Java's .indexOf would return 3 (UTF-16 code-unit index), so this
		// test catches a regression that returned UTF-16 indices.
		put("👋-world");
		assertEquals(2L, operate(StringOperation.find(BIN, "world")).getLong(BIN));
	}

	@Test
	public void findAndContainsMatchAcrossNormalizationForms() {
		// "café" can be stored as NFC (U+00E9, 1 codepoint, 2 UTF-8 bytes) or NFD
		// (U+0065 U+0301, 2 codepoints, 3 UTF-8 bytes). They render identically but
		// are distinct byte sequences, and the server treats them as equal: find /
		// contains take the binary memmem path only when both operands are ASCII or
		// both are NFC, and otherwise route through an ICU UStringSearch whose
		// collator has full normalization enabled (particle_string.c get_canon_search).
		final String NFC = "caf\u00E9";       // "café" composed
		final String NFD = "cafe\u0301";      // "café" decomposed

		put(NFC);
		// Both NFC — binary fast path.
		assertEquals(0L, operate(StringOperation.find(BIN, NFC)).getLong(BIN));
		assertTrue(operate(StringOperation.contains(BIN, NFC)).getBoolean(BIN));
		// Forms differ, so the canonical path runs and still matches.
		assertEquals(0L, operate(StringOperation.find(BIN, NFD)).getLong(BIN));
		assertTrue(operate(StringOperation.contains(BIN, NFD)).getBoolean(BIN));
	}

	@Test
	public void replaceMatchesAcrossNormalizationForms() {
		// replace carries the same canonical-equivalence guarantee as find /
		// contains above: string_modify_op_replace_K_icu routes through
		// get_canon_search (particle_string.c) whenever the forms differ.
		final String NFC = "caf\u00E9";       // "café" composed
		final String NFD = "cafe\u0301";      // "café" decomposed

		// Composed haystack, decomposed needle.
		put(NFC + " au lait");
		operate(StringOperation.replace(POLICY, BIN, NFD, "tea"));
		assertEquals("tea au lait", stringValue());

		// Decomposed haystack, composed needle.
		put(NFD + " au lait");
		operate(StringOperation.replace(POLICY, BIN, NFC, "tea"));
		assertEquals("tea au lait", stringValue());
	}

	@Test
	public void startsWithAndEndsWithMatchAcrossNormalizationForms() {
		// get_canon_search has four call sites, not two: prefix and suffix
		// matching are canonical as well, so an affix in either form matches a
		// bin stored in the other.
		final String NFC = "caf\u00E9";
		final String NFD = "cafe\u0301";

		put(NFC + " au lait");
		assertTrue(operate(StringOperation.startsWith(BIN, NFD)).getBoolean(BIN));

		put(NFD + " au lait");
		assertTrue(operate(StringOperation.startsWith(BIN, NFC)).getBoolean(BIN));

		put("au lait " + NFC);
		assertTrue(operate(StringOperation.endsWith(BIN, NFD)).getBoolean(BIN));

		put("au lait " + NFD);
		assertTrue(operate(StringOperation.endsWith(BIN, NFC)).getBoolean(BIN));
	}

	@Test
	public void substrFromOffsetToEnd() {
		put("hello world");
		Record r = operate(StringOperation.substr(BIN, 6));
		assertEquals("world", r.getString(BIN));
	}

	@Test
	public void substrSlicesARange() {
		put("hello world");
		Record r = operate(StringOperation.substr(BIN, 0, 5));
		assertEquals("hello", r.getString(BIN));
	}

	@Test
	public void substrSupportsNegativeStart() {
		put("hello world");
		Record r = operate(StringOperation.substr(BIN, -5));
		assertEquals("world", r.getString(BIN));
	}

	@Test
	public void charAtReturnsSingleCharacter() {
		put("Hello123World");
		Record r = operate(StringOperation.charAt(BIN, 5));
		assertEquals("1", r.getString(BIN));
	}

	@Test
	public void findReturnsIndexOfFirstMatch() {
		put("hello world");
		Record r = operate(StringOperation.find(BIN, "world"));
		assertEquals(6L, r.getLong(BIN));
	}

	@Test
	public void findReturnsMinusOneWhenAbsent() {
		put("hello world");
		Record r = operate(StringOperation.find(BIN, "xyz"));
		assertEquals(-1L, r.getLong(BIN));
	}

	@Test
	public void findSkipsOverlappingMatchesAscii() {
		// "aa" is a self-overlapping needle (prefix "a" == suffix "a"). After
		// matching at index 0 the search resumes *after* the match (index 2),
		// so the 2nd occurrence is at 2 — not 1. This matches replace() and
		// the ICU usearch path used for non-ASCII haystacks.
		put("aaaa");
		assertEquals(0L, operate(StringOperation.find(BIN, "aa", 1)).getLong(BIN));
		assertEquals(2L, operate(StringOperation.find(BIN, "aa", 2)).getLong(BIN));
		assertEquals(-1L, operate(StringOperation.find(BIN, "aa", 3)).getLong(BIN));
	}

	@Test
	public void findSkipsOverlappingMatchesUnicode() {
		// Same overlap-skip rule on the ICU path. "👋👋" is self-overlapping in
		// codepoints; matches land at codepoint indices 0 and 2, not 0 and 1.
		put("👋👋👋👋");
		assertEquals(0L, operate(StringOperation.find(BIN, "👋👋", 1)).getLong(BIN));
		assertEquals(2L, operate(StringOperation.find(BIN, "👋👋", 2)).getLong(BIN));
		assertEquals(-1L, operate(StringOperation.find(BIN, "👋👋", 3)).getLong(BIN));
	}

	@Test
	public void containsReturnsBoolean() {
		put("hello world");
		Record present = operate(StringOperation.contains(BIN, "hello"));
		Record absent = operate(StringOperation.contains(BIN, "xyz"));
		assertTrue(present.getBoolean(BIN));
		assertFalse(absent.getBoolean(BIN));
	}

	@Test
	public void startsWithMatchesPrefix() {
		put("Hello123World");
		assertTrue(operate(StringOperation.startsWith(BIN, "Hello")).getBoolean(BIN));
		assertFalse(operate(StringOperation.startsWith(BIN, "World")).getBoolean(BIN));
	}

	@Test
	public void endsWithMatchesSuffix() {
		put("Hello123World");
		assertTrue(operate(StringOperation.endsWith(BIN, "World")).getBoolean(BIN));
		assertFalse(operate(StringOperation.endsWith(BIN, "Hello")).getBoolean(BIN));
	}

	@Test
	public void isUpperOnlyTrueForUppercase() {
		put("HELLO");
		assertTrue(operate(StringOperation.isUpper(BIN)).getBoolean(BIN));
		put("hello");
		assertFalse(operate(StringOperation.isUpper(BIN)).getBoolean(BIN));
	}

	@Test
	public void isLowerOnlyTrueForLowercase() {
		put("hello");
		assertTrue(operate(StringOperation.isLower(BIN)).getBoolean(BIN));
		put("HELLO");
		assertFalse(operate(StringOperation.isLower(BIN)).getBoolean(BIN));
	}

	@Test
	public void isNumericMatchesIntegerStrings() {
		put("12345");
		assertTrue(operate(StringOperation.isNumeric(BIN)).getBoolean(BIN));
		put("Hello123World");
		assertFalse(operate(StringOperation.isNumeric(BIN)).getBoolean(BIN));
	}

	@Test
	public void isNumericFloatRequiresFractionalDigit() {
		// FLOAT is is_valid_double && has_decimal_fraction, so a pure-digit
		// string is false under FLOAT but true under ANY via the int branch.
		put("3.14");
		assertTrue(operate(StringOperation.isNumeric(BIN, StringNumericType.FLOAT)).getBoolean(BIN));
		put("5");
		assertFalse(operate(StringOperation.isNumeric(BIN, StringNumericType.FLOAT)).getBoolean(BIN));
		assertTrue(operate(StringOperation.isNumeric(BIN, StringNumericType.ANY)).getBoolean(BIN));
		put("5.");
		assertFalse(operate(StringOperation.isNumeric(BIN, StringNumericType.FLOAT)).getBoolean(BIN));
		put("1e5");
		assertFalse(operate(StringOperation.isNumeric(BIN, StringNumericType.FLOAT)).getBoolean(BIN));
		assertFalse(operate(StringOperation.isNumeric(BIN, StringNumericType.ANY)).getBoolean(BIN));
	}

	@Test
	public void toIntegerParsesDigitsAsLong() {
		put("12345");
		Record r = operate(StringOperation.toInteger(BIN));
		assertEquals(12345L, r.getLong(BIN));
	}

	@Test
	public void toDoubleParsesDecimalNumbers() {
		put("3.14");
		Record r = operate(StringOperation.toDouble(BIN));
		assertEquals(3.14, r.getDouble(BIN), 0.001);
	}

	@Test
	public void splitReturnsListOfTokens() {
		put("one,two,three");
		Record r = operate(StringOperation.split(BIN, ","));
		assertEquals(Arrays.asList("one", "two", "three"), r.getList(BIN));
	}

	@Test
	public void splitWithoutMatchReturnsSingletonList() {
		put("Hello123World");
		Record r = operate(StringOperation.split(BIN, "|"));
		assertEquals(Arrays.asList("Hello123World"), r.getList(BIN));
	}

	@Test
	public void regexCompareDistinguishesMatchVsMiss() {
		put("Hello123World");
		assertTrue(operate(StringOperation.regexCompare(BIN, "[0-9]+")).getBoolean(BIN));
		put("HELLO");
		assertFalse(operate(StringOperation.regexCompare(BIN, "[0-9]+")).getBoolean(BIN));
	}

	@Test
	public void regexCompareHonorsCaseInsensitiveFlag() {
		put("HELLO");
		assertTrue(operate(StringOperation.regexCompare(
			BIN, "hello", StringRegexFlags.CASE_INSENSITIVE)).getBoolean(BIN));
	}

	@Test
	public void toBlobReturnsUtf8Bytes() {
		put("hello");
		Record r = operate(StringOperation.toBlob(BIN));
		assertArrayEquals("hello".getBytes(), (byte[])r.getValue(BIN));
	}

	@Test
	public void b64DecodeReturnsOriginalBlob() {
		put("aGVsbG8=");
		Record r = operate(StringOperation.b64Decode(BIN));
		assertArrayEquals("hello".getBytes(), (byte[])r.getValue(BIN));
	}

	//=================================================================
	// Modify operations
	//=================================================================

	@Test
	public void upperMutatesBinInPlace() {
		put("hello world");
		operate(StringOperation.upper(POLICY, BIN));
		assertEquals("HELLO WORLD", stringValue());
	}

	@Test
	public void lowerMutatesBinInPlace() {
		put("HELLO WORLD");
		operate(StringOperation.lower(POLICY, BIN));
		assertEquals("hello world", stringValue());
	}

	@Test
	public void caseFoldLowercasesIndependentlyOfLocale() {
		put("HELLO World");
		operate(StringOperation.caseFold(POLICY, BIN));
		assertEquals("hello world", stringValue());
	}

	@Test
	public void normalizeNFCLeavesAlreadyNormalizedStringUnchanged() {
		put("hello");
		operate(StringOperation.normalizeNFC(POLICY, BIN));
		assertEquals("hello", stringValue());
	}

	@Test
	public void normalizeNFCComposesDecomposedSequence() {
		// "e\u0301" is the NFD ("decomposed") form of "é": Latin small "e"
		// followed by combining acute accent. normalizeNFC must compose it to
		// U+00E9 (NFC, single codepoint) — proving the op actually transforms
		// non-normalized input, not just the no-op case.
		put("e\u0301");
		operate(StringOperation.normalizeNFC(POLICY, BIN));
		assertEquals("\u00E9", stringValue());
		// Composed form is 1 codepoint; the decomposed input would be 2.
		assertEquals(1L, operate(StringOperation.strlen(BIN)).getLong(BIN));
	}

	@Test
	public void insertAtMiddleSplicesValue() {
		put("hello world");
		operate(StringOperation.insert(POLICY, BIN, 5, " beautiful"));
		assertEquals("hello beautiful world", stringValue());
	}

	@Test
	public void insertAtZeroPrependsValue() {
		put("world");
		operate(StringOperation.insert(POLICY, BIN, 0, "hello "));
		assertEquals("hello world", stringValue());
	}

	@Test
	public void insertAtEndAppendsValue() {
		put("hello");
		operate(StringOperation.insert(POLICY, BIN, 5, " world"));
		assertEquals("hello world", stringValue());
	}

	@Test
	public void insertWithNegativeIndexCountsFromEnd() {
		put("hello world");
		operate(StringOperation.insert(POLICY, BIN, -5, "big "));
		assertEquals("hello big world", stringValue());
	}

	@Test
	public void overwriteReplacesCharactersStartingAtIndex() {
		put("hello world");
		operate(StringOperation.overwrite(POLICY, BIN, 6, "earth"));
		assertEquals("hello earth", stringValue());
	}

	@Test
	public void overwriteAtZeroReplacesPrefix() {
		put("hello world");
		operate(StringOperation.overwrite(POLICY, BIN, 0, "HELLO"));
		assertEquals("HELLO world", stringValue());
	}

	@Test
	public void overwriteCanExtendBeyondOriginalLength() {
		put("hello");
		operate(StringOperation.overwrite(POLICY, BIN, 3, "ping!"));
		assertEquals("helping!", stringValue());
	}

	@Test
	public void snipRemovesCharacterRange() {
		put("hello beautiful world");
		operate(StringOperation.snip(POLICY, BIN, 5, 15));
		assertEquals("hello world", stringValue());
	}

	@Test
	public void snipFromStartTrimsPrefix() {
		put("hello world");
		operate(StringOperation.snip(POLICY, BIN, 0, 6));
		assertEquals("world", stringValue());
	}

	@Test
	public void snipToEndTrimsSuffix() {
		put("hello world");
		operate(StringOperation.snip(POLICY, BIN, 5, 11));
		assertEquals("hello", stringValue());
	}

	@Test
	public void snipFromStartTruncatesToEnd() {
		put("hello world");
		operate(StringOperation.snip(POLICY, BIN, 5));
		assertEquals("hello", stringValue());
	}

	@Test
	public void snipFromNegativeStartCountsFromEnd() {
		put("hello world");
		operate(StringOperation.snip(POLICY, BIN, -5));
		assertEquals("hello ", stringValue());
	}

	@Test
	public void snipFromPacksStartWithoutFlagsElement() {
		// The server parses snip's arguments positionally (start, end, flags), so a
		// trailing flags element on the 1-index form is read as `end`.
		Operation op = StringOperation.snip(POLICY, BIN, 5);
		byte[] payload = (byte[])op.value.getObject();
		List<?> args = (List<?>)Unpacker.unpackObjectList(payload, 0, payload.length);
		assertEquals(2, args.size());
		assertEquals(53L, args.get(0));
		assertEquals(5L, args.get(1));

		Operation range = StringOperation.snip(POLICY, BIN, 5, 11);
		byte[] rangePayload = (byte[])range.value.getObject();
		List<?> rangeArgs = (List<?>)Unpacker.unpackObjectList(rangePayload, 0, rangePayload.length);
		assertEquals(Arrays.asList(53L, 5L, 11L, 0L), rangeArgs);
	}

	@Test
	public void replaceTouchesOnlyFirstOccurrence() {
		put("hello world world");
		operate(StringOperation.replace(POLICY, BIN, "world", "earth"));
		assertEquals("hello earth world", stringValue());
	}

	@Test
	public void replaceWithNoMatchLeavesBinUnchanged() {
		put("hello world");
		operate(StringOperation.replace(POLICY, BIN, "xyz", "abc"));
		assertEquals("hello world", stringValue());
	}

	@Test
	public void replaceCanGrowTheString() {
		put("hi world");
		operate(StringOperation.replace(POLICY, BIN, "hi", "hello"));
		assertEquals("hello world", stringValue());
	}

	@Test
	public void replaceWithEmptyDeletesMatch() {
		put("hello world");
		operate(StringOperation.replace(POLICY, BIN, " world", ""));
		assertEquals("hello", stringValue());
	}

	@Test
	public void replaceAllSubstitutesEveryMatch() {
		put("aabaa");
		operate(StringOperation.replaceAll(POLICY, BIN, "a", "x"));
		assertEquals("xxbxx", stringValue());
	}

	@Test
	public void replaceAllWithNoMatchLeavesBinUnchanged() {
		put("hello");
		operate(StringOperation.replaceAll(POLICY, BIN, "z", "!"));
		assertEquals("hello", stringValue());
	}

	@Test
	public void replaceAllSkipsOverlappingMatches() {
		// Self-overlapping needle "aa" in "aaaa": replacement resumes after each
		// match, yielding "XX" — not "XaX" (which would require allowing the
		// 2nd match to start at index 1). Anchors the contract that find() now
		// mirrors.
		put("aaaa");
		operate(StringOperation.replaceAll(POLICY, BIN, "aa", "X"));
		assertEquals("XX", stringValue());
	}

	@Test
	public void trimRemovesWhitespaceOnBothEnds() {
		put("  hello world  ");
		operate(StringOperation.trim(POLICY, BIN));
		assertEquals("hello world", stringValue());
	}

	@Test
	public void trimOnCleanStringIsNoOp() {
		put("hello");
		operate(StringOperation.trim(POLICY, BIN));
		assertEquals("hello", stringValue());
	}

	@Test
	public void trimStartRemovesLeadingWhitespaceOnly() {
		put("  hello  ");
		operate(StringOperation.trimStart(POLICY, BIN));
		assertEquals("hello  ", stringValue());
	}

	@Test
	public void trimEndRemovesTrailingWhitespaceOnly() {
		put("  hello  ");
		operate(StringOperation.trimEnd(POLICY, BIN));
		assertEquals("  hello", stringValue());
	}

	@Test
	public void padStartFillsLeftToTargetLength() {
		put("hello");
		operate(StringOperation.padStart(POLICY, BIN, 10, "*"));
		assertEquals("*****hello", stringValue());
	}

	@Test
	public void padStartIsNoOpWhenAlreadyLongEnough() {
		put("hello world");
		operate(StringOperation.padStart(POLICY, BIN, 5, "*"));
		assertEquals("hello world", stringValue());
	}

	@Test
	public void padEndFillsRightToTargetLength() {
		put("hello");
		operate(StringOperation.padEnd(POLICY, BIN, 10, "."));
		assertEquals("hello.....", stringValue());
	}

	@Test
	public void padStartRepeatsMultiCharFiller() {
		put("hi");
		operate(StringOperation.padStart(POLICY, BIN, 8, "ab"));
		assertEquals("abababhi", stringValue());
	}

	@Test
	public void repeatDuplicatesContents() {
		put("ab");
		operate(StringOperation.repeat(POLICY, BIN, 3));
		assertEquals("ababab", stringValue());
	}

	@Test
	public void repeatOnceLeavesBinUnchanged() {
		put("hello");
		operate(StringOperation.repeat(POLICY, BIN, 1));
		assertEquals("hello", stringValue());
	}

	@Test
	public void concatAppendsSingleString() {
		put("  hello world  ");
		operate(StringOperation.concat(POLICY, BIN, "!"));
		assertEquals("  hello world  !", stringValue());
	}

	@Test
	public void concatAppendsListOfValues() {
		put("hello");
		operate(StringOperation.concat(POLICY, BIN, Arrays.asList(" ", "big", " world")));
		assertEquals("hello big world", stringValue());
	}

	@Test
	public void appendAddsValueToEnd() {
		put("hello");
		operate(StringOperation.append(POLICY, BIN, " world"));
		assertEquals("hello world", stringValue());
	}

	@Test
	public void appendToEmptyStringYieldsValue() {
		put("");
		operate(StringOperation.append(POLICY, BIN, "hi"));
		assertEquals("hi", stringValue());
	}

	@Test
	public void appendPreservesMultibyteCodepoints() {
		// Unicode/DBCS-aware: appending a multi-byte string must not corrupt
		// either side. "日本" + "語" -> "日本語" (3 codepoints, 9 UTF-8 bytes).
		put("日本");
		operate(StringOperation.append(POLICY, BIN, "語"));
		assertEquals("日本語", stringValue());
		assertEquals(3L, operate(StringOperation.strlen(BIN)).getLong(BIN));
	}

	@Test
	public void prependAddsValueToStart() {
		put("world");
		operate(StringOperation.prepend(POLICY, BIN, "hello "));
		assertEquals("hello world", stringValue());
	}

	@Test
	public void prependToEmptyStringYieldsValue() {
		put("");
		operate(StringOperation.prepend(POLICY, BIN, "hi"));
		assertEquals("hi", stringValue());
	}

	@Test
	public void prependPreservesMultibyteCodepoints() {
		// Unicode/DBCS-aware: prepending a multi-byte string must not corrupt
		// either side. "語" prepended with "日本" -> "日本語".
		put("語");
		operate(StringOperation.prepend(POLICY, BIN, "日本"));
		assertEquals("日本語", stringValue());
		assertEquals(3L, operate(StringOperation.strlen(BIN)).getLong(BIN));
	}

	@Test
	public void appendOnMissingBinCreatesTheBinFromEmpty() {
		// Create-ops {insert, concat, append, prepend} bootstrap an empty string
		// and create a missing bin. NO_FAIL is irrelevant — the op always succeeds.
		client.delete(null, KEY);
		client.put(null, KEY, new Bin("other", "untouched"));

		operate(StringOperation.append(POLICY, BIN, "x"));

		Record r = client.get(null, KEY);
		assertEquals("x", r.getString(BIN));
		assertEquals("untouched", r.getString("other"));
	}

	@Test
	public void regexReplaceTargetsFirstMatchByDefault() {
		put("abc123def456");
		operate(StringOperation.regexReplace(POLICY, BIN, "[0-9]+", "NUM", StringRegexFlags.DEFAULT));
		assertEquals("abcNUMdef456", stringValue());
	}

	@Test
	public void regexReplaceWithGlobalFlagReplacesEveryMatch() {
		put("abc123def456");
		operate(StringOperation.regexReplace(POLICY, BIN, "[0-9]+", "NUM", StringRegexFlags.GLOBAL));
		assertEquals("abcNUMdefNUM", stringValue());
	}

	@Test
	public void regexReplaceWithNoMatchLeavesBinUnchanged() {
		put("hello");
		operate(StringOperation.regexReplace(POLICY, BIN, "[0-9]+", "NUM", StringRegexFlags.GLOBAL));
		assertEquals("hello", stringValue());
	}

	//=================================================================
	// Multi-op pipelines
	//=================================================================

	@Test
	public void readsAcrossMultipleBinsInOneOperate() {
		put(
			new Bin("text", "  hello world  "),
			new Bin("number_str", "12345"),
			new Bin("upper_str", "HELLO"));

		Record r = operate(
			StringOperation.strlen("text"),
			StringOperation.toInteger("number_str"),
			StringOperation.isUpper("upper_str"));

		// strlen and toInteger return INT; isUpper returns BOOL.
		assertEquals(15L, r.getLong("text"));
		assertEquals(12345L, r.getLong("number_str"));
		assertTrue(r.getBoolean("upper_str"));
	}

	@Test
	public void modifyAndReadInOneOperatePipelineCommitsThenObserves() {
		put("  hello world  ");

		Record r = operate(
			StringOperation.trim(POLICY, BIN),
			StringOperation.upper(POLICY, BIN),
			StringOperation.strlen(BIN));

		// String ops set RESPOND_ALL_OPS (like BIT/EXP/HLL/MAP), so the three ops
		// targeting the same bin come back as an ordered per-op result list rather
		// than a single collapsed value. strlen runs last and therefore observes the
		// post-trim+upper length.
		List<?> results = r.getList(BIN);
		assertEquals(11L, results.get(results.size() - 1));
		assertEquals("HELLO WORLD", stringValue());
	}

	@Test
	public void chainedReplaceAllAndPaddingComposeAsExpected() {
		put("aabaa");

		operate(
			StringOperation.replaceAll(POLICY, BIN, "a", "x"),
			StringOperation.padEnd(POLICY, BIN, 10, "."));

		assertEquals("xxbxx.....", stringValue());
	}

	@Test
	public void snipThenConcatInOneOperate() {
		put("hello beautiful world");

		operate(
			StringOperation.snip(POLICY, BIN, 5, 15),
			StringOperation.concat(POLICY, BIN, "!"));

		assertEquals("hello world!", stringValue());
	}

	@Test
	public void splitResultListEntriesAreReadableStrings() {
		put("one,two,three");
		Record r = operate(StringOperation.split(BIN, ","));

		List<?> tokens = r.getList(BIN);
		assertEquals(3, tokens.size());
		// Each entry should round-trip as a String regardless of internal encoding.
		for (Object t : tokens) {
			assertTrue("expected String element but got " + (t == null ? "null" : t.getClass()),
				t instanceof String);
		}
	}

	//=================================================================
	// CTX navigation — string nested in list/map bins
	//
	// Exercises the §2.3.1 CTX-wrapper wire envelope: when CTX is non-empty
	// the op-data becomes [0xFF, ctx_list, [sub_op, args...]] — three outer
	// elements, with the sub-op and its args in their own nested array so
	// the inner arity is self-describing (SERVER-1483). The server dispatches
	// these through as_bin_string_modify_ctx_tr / its read-side twin, which
	// is a separate code path from the top-level-bin variant exercised above.
	//=================================================================

	private static void putList(List<Value> values) {
		client.delete(null, KEY);
		client.put(null, KEY, new Bin(BIN, values));
	}

	private static void putMap(Map<Value, Value> entries) {
		client.delete(null, KEY);
		client.put(null, KEY, new Bin(BIN, entries));
	}

	@Test
	public void readOpOnStringNestedInList() {
		// list = ["alpha", "beta", "hello world"]; strlen at index 2 = 11
		List<Value> list = new ArrayList<Value>();
		list.add(Value.get("alpha"));
		list.add(Value.get("beta"));
		list.add(Value.get("hello world"));
		putList(list);

		Record r = operate(StringOperation.strlen(BIN, CTX.listIndex(2)));
		assertEquals(11L, r.getLong(BIN));
	}

	@Test
	public void readBooleanOpOnStringNestedInMap() {
		// map = {"a": "Hello", "b": "World"}; startsWith("World","Wor") = true
		Map<Value, Value> map = new HashMap<Value, Value>();
		map.put(Value.get("a"), Value.get("Hello"));
		map.put(Value.get("b"), Value.get("World"));
		putMap(map);

		Record r = operate(StringOperation.startsWith(BIN, "Wor", CTX.mapKey(Value.get("b"))));
		assertTrue(r.getBoolean(BIN));
	}

	@Test
	public void modifyOpOnStringNestedInList() {
		// list = ["alpha", "beta", "gamma"]; upper at index 1 -> "BETA"
		List<Value> list = new ArrayList<Value>();
		list.add(Value.get("alpha"));
		list.add(Value.get("beta"));
		list.add(Value.get("gamma"));
		putList(list);

		operate(StringOperation.upper(POLICY, BIN, CTX.listIndex(1)));

		List<?> after = client.get(null, KEY).getList(BIN);
		assertEquals(Arrays.asList("alpha", "BETA", "gamma"), after);
	}

	@Test
	public void modifyOpOnStringNestedInMap() {
		// map = {"a": "hello world", "b": "foo"}; replace at key "a"
		Map<Value, Value> map = new HashMap<Value, Value>();
		map.put(Value.get("a"), Value.get("hello world"));
		map.put(Value.get("b"), Value.get("foo"));
		putMap(map);

		operate(StringOperation.replace(POLICY, BIN, "world", "earth",
			CTX.mapKey(Value.get("a"))));

		Map<?, ?> after = client.get(null, KEY).getMap(BIN);
		assertEquals("hello earth", after.get("a"));
		assertEquals("foo", after.get("b"));
	}

	@Test
	public void modifyOpOnStringDeeplyNestedListInMap() {
		// map = {"items": ["one", "two", "three"]}; upper at items[1] -> "TWO"
		List<Value> inner = new ArrayList<Value>();
		inner.add(Value.get("one"));
		inner.add(Value.get("two"));
		inner.add(Value.get("three"));

		Map<Value, Value> map = new HashMap<Value, Value>();
		map.put(Value.get("items"), Value.get(inner));
		putMap(map);

		operate(StringOperation.upper(POLICY, BIN,
			CTX.mapKey(Value.get("items")), CTX.listIndex(1)));

		Map<?, ?> after = client.get(null, KEY).getMap(BIN);
		List<?> items = (List<?>)after.get("items");
		assertEquals(Arrays.asList("one", "TWO", "three"), items);
	}

	@Test
	public void appendOnStringNestedInList() {
		// list = ["alpha", "beta", "gamma"]; append "!" at index 1 -> "beta!"
		List<Value> list = new ArrayList<Value>();
		list.add(Value.get("alpha"));
		list.add(Value.get("beta"));
		list.add(Value.get("gamma"));
		putList(list);

		operate(StringOperation.append(POLICY, BIN, "!", CTX.listIndex(1)));

		List<?> after = client.get(null, KEY).getList(BIN);
		assertEquals(Arrays.asList("alpha", "beta!", "gamma"), after);
	}

	@Test
	public void prependOnStringNestedInMap() {
		// map = {"a": "world", "b": "foo"}; prepend "hello " at key "a"
		Map<Value, Value> map = new HashMap<Value, Value>();
		map.put(Value.get("a"), Value.get("world"));
		map.put(Value.get("b"), Value.get("foo"));
		putMap(map);

		operate(StringOperation.prepend(POLICY, BIN, "hello ",
			CTX.mapKey(Value.get("a"))));

		Map<?, ?> after = client.get(null, KEY).getMap(BIN);
		assertEquals("hello world", after.get("a"));
		assertEquals("foo", after.get("b"));
	}

	@Test
	public void modifyOpWithFlagsOnStringNestedInList() {
		// append takes 1-2 args, so its trailing flags slot is optional. Under CTX
		// the flags sit in the nested inner array, whose own header declares the
		// arity — in the flat envelope they were indistinguishable from a 2nd arg.
		List<Value> list = new ArrayList<Value>();
		list.add(Value.get("alpha"));
		list.add(Value.get("beta"));
		list.add(Value.get("gamma"));
		putList(list);

		StringPolicy noFail = new StringPolicy(StringWriteFlags.NO_FAIL);
		operate(StringOperation.append(noFail, BIN, "!", CTX.listIndex(1)));

		List<?> after = client.get(null, KEY).getList(BIN);
		assertEquals(Arrays.asList("alpha", "beta!", "gamma"), after);
	}

	@Test
	public void noFailFlagDecidesOutcomeOnUnreachableCtxPath() {
		// Same op and same arity, differing only in the trailing flags value:
		// NO_FAIL swallows the unreachable path, DEFAULT surfaces it. That the
		// outcome tracks the flag is what proves the trailing element is read as
		// flags rather than as an extra argument.
		List<Value> list = new ArrayList<Value>();
		list.add(Value.get("alpha"));
		list.add(Value.get("beta"));
		putList(list);

		StringPolicy noFail = new StringPolicy(StringWriteFlags.NO_FAIL);
		operate(StringOperation.append(noFail, BIN, "!", CTX.listIndex(99)));
		assertEquals(Arrays.asList("alpha", "beta"), client.get(null, KEY).getList(BIN));

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> operate(StringOperation.append(POLICY, BIN, "!", CTX.listIndex(99))));
		assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());
	}

	//=================================================================
	// toString op — op-type 19, no payload, no sub-op id, no CTX
	//
	// Spec §2.6 and §4.1: covers integer/float/string/blob -> string
	// conversions, plus the INCOMPATIBLE_TYPE error path for list/map
	// bins that the wire format cannot represent.
	//=================================================================

	@Test
	public void toStringConvertsIntegerBinToString() {
		client.delete(null, KEY);
		client.put(null, KEY, new Bin(BIN, 42));
		Record r = operate(StringOperation.toString(BIN));
		assertEquals("42", r.getString(BIN));
	}

	@Test
	public void toStringConvertsDoubleBinToString() {
		client.delete(null, KEY);
		client.put(null, KEY, new Bin(BIN, 3.14));
		Record r = operate(StringOperation.toString(BIN));
		// Float-to-string formatting is server-side; assert it parses back.
		assertEquals(3.14, Double.parseDouble(r.getString(BIN)), 0.0001);
	}

	@Test
	public void toStringOnStringBinIsIdentity() {
		put("hello");
		Record r = operate(StringOperation.toString(BIN));
		assertEquals("hello", r.getString(BIN));
	}

	@Test
	public void toStringConvertsBlobBinToString() {
		client.delete(null, KEY);
		client.put(null, KEY, new Bin(BIN, new byte[] {'h', 'i'}));
		Record r = operate(StringOperation.toString(BIN));
		// Server's blob-to-string representation is well-defined for ASCII bytes.
		assertEquals("hi", r.getString(BIN));
	}

	@Test
	public void toStringOnBlobWithInvalidUtf8RaisesOpNotApplicable() {
		// {0xED, 0xA0, 0x80} is the UTF-8 encoding of U+D800 (ill-formed
		// surrogate). The server's blob→string conversion validates the bytes
		// via cf_str_is_valid_utf8 and rejects non-well-formed input with
		// OP_NOT_APPLICABLE (mirrors the server's ToStringTest.Blob_InvalidUtf8
		// unit test). Companion to TestStringInvalidUtf8 which exercises the
		// same fixture on the read/modify ops via a STRING-typed bin.
		client.delete(null, KEY);
		client.put(null, KEY, new Bin(BIN,
			new byte[] {(byte)0xED, (byte)0xA0, (byte)0x80}));
		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> operate(StringOperation.toString(BIN)));
		assertEquals(ResultCode.OP_NOT_APPLICABLE, ae.getResultCode());
	}

	@Test
	public void toStringOnListBinReturnsIncompatibleType() {
		List<Value> list = new ArrayList<Value>();
		list.add(Value.get("a"));
		list.add(Value.get("b"));
		putList(list);

		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> operate(StringOperation.toString(BIN)));
		assertEquals(ResultCode.BIN_TYPE_ERROR, ae.getResultCode());
	}

	//=================================================================
	// Missing-bin path
	//
	// Behavior keys off the op, not the flag. The eight additive
	// create-ops {insert, overwrite, concat, append, prepend, padStart,
	// padEnd, repeat} create a missing bin from an empty string;
	// transform/subtractive ops are a silent no-op (success, bin not
	// created). There is no BIN_NOT_FOUND path. NO_FAIL no longer governs
	// this path — it only suppresses an in-op execution failure (and still
	// does not suppress BIN_TYPE_ERROR on a wrong-type bin).
	//=================================================================

	@Test
	public void modifyOnMissingBinIsNoOp() {
		// A non-create modify op (upper) on a missing bin is a silent no-op
		// (success, bin not created) regardless of NO_FAIL — there is no
		// BIN_NOT_FOUND path. Record exists but the target bin does not.
		client.delete(null, KEY);
		client.put(null, KEY, new Bin("other", "untouched"));

		operate(StringOperation.upper(POLICY, BIN));

		// BIN must not have been created; the existing bin must be intact.
		Record r = client.get(null, KEY);
		assertEquals(null, r.getValue(BIN));
		assertEquals("untouched", r.getString("other"));
	}

	@Test
	public void noFailDoesNotChangeMissingBinNoOp() {
		// The missing-bin no-op for non-create ops is flag-independent; NO_FAIL
		// neither creates the bin nor raises an error.
		client.delete(null, KEY);
		client.put(null, KEY, new Bin("other", "untouched"));

		StringPolicy noFail = new StringPolicy(StringWriteFlags.NO_FAIL);
		operate(StringOperation.upper(noFail, BIN));

		Record r = client.get(null, KEY);
		assertEquals(null, r.getValue(BIN));
		assertEquals("untouched", r.getString("other"));
	}

	// All eight additive ops create a missing bin from empty in server 8.1.3
	// (string ops + SERVER-97 PR 1452, which adds overwrite/repeat/padStart/
	// padEnd to the create-op set). Transform/subtractive ops still no-op.
	// append is covered above in the append section.

	@Test
	public void insertOnMissingBinCreatesTheBinFromEmpty() {
		client.delete(null, KEY);
		client.put(null, KEY, new Bin("other", "untouched"));

		operate(StringOperation.insert(POLICY, BIN, 0, "hi"));

		assertEquals("hi", client.get(null, KEY).getString(BIN));
	}

	@Test
	public void concatOnMissingBinCreatesTheBinFromEmpty() {
		client.delete(null, KEY);
		client.put(null, KEY, new Bin("other", "untouched"));

		operate(StringOperation.concat(POLICY, BIN, "hi"));

		assertEquals("hi", client.get(null, KEY).getString(BIN));
	}

	@Test
	public void prependOnMissingBinCreatesTheBinFromEmpty() {
		client.delete(null, KEY);
		client.put(null, KEY, new Bin("other", "untouched"));

		operate(StringOperation.prepend(POLICY, BIN, "hi"));

		assertEquals("hi", client.get(null, KEY).getString(BIN));
	}

	@Test
	public void overwriteOnMissingBinCreatesTheBinFromEmpty() {
		client.delete(null, KEY);
		client.put(null, KEY, new Bin("other", "untouched"));

		operate(StringOperation.overwrite(POLICY, BIN, 0, "hi"));

		assertEquals("hi", client.get(null, KEY).getString(BIN));
	}

	@Test
	public void padStartOnMissingBinCreatesTheBinFromEmpty() {
		client.delete(null, KEY);
		client.put(null, KEY, new Bin("other", "untouched"));

		operate(StringOperation.padStart(POLICY, BIN, 5, "x"));

		assertEquals("xxxxx", client.get(null, KEY).getString(BIN));
	}

	@Test
	public void padEndOnMissingBinCreatesTheBinFromEmpty() {
		client.delete(null, KEY);
		client.put(null, KEY, new Bin("other", "untouched"));

		operate(StringOperation.padEnd(POLICY, BIN, 5, "x"));

		assertEquals("xxxxx", client.get(null, KEY).getString(BIN));
	}

	@Test
	public void repeatOnMissingBinCreatesAnEmptyBin() {
		// repeat(n) on empty = "" — the bin is created holding an empty string
		// (server test: expect_string_bin(b, "")).
		client.delete(null, KEY);
		client.put(null, KEY, new Bin("other", "untouched"));

		operate(StringOperation.repeat(POLICY, BIN, 3));

		assertEquals("", client.get(null, KEY).getString(BIN));
	}

	//=================================================================
	// Prepare / parameter errors
	//
	// These exercise the server's prepare-phase validation
	// (particle_string.c: find occurrence != 0, empty/negative pad
	// arguments, repeat count >= 0, regex_replace pattern compile).
	// All should surface as PARAMETER_ERROR; an invalid regex surfaces
	// as OP_NOT_APPLICABLE per the server's ICU integration.
	//=================================================================

	private static void assertParamError(Operation op) {
		AerospikeException ae = assertThrows(AerospikeException.class,
			() -> operate(op));
		assertEquals(ResultCode.PARAMETER_ERROR, ae.getResultCode());
	}

	@Test
	public void findWithZeroOccurrenceRaisesParameter() {
		put("hello");
		// 0 is reserved as "no occurrence"; the server's find prepare rejects it.
		assertParamError(StringOperation.find(BIN, "x", 0));
	}

	@Test
	public void padStartWithEmptyPadStringRaisesParameter() {
		put("hello");
		assertParamError(StringOperation.padStart(POLICY, BIN, 10, ""));
	}

	@Test
	public void padEndWithEmptyPadStringRaisesParameter() {
		put("hello");
		assertParamError(StringOperation.padEnd(POLICY, BIN, 10, ""));
	}

	@Test
	public void padStartWithNegativeTargetRaisesParameter() {
		put("hello");
		assertParamError(StringOperation.padStart(POLICY, BIN, -1, "*"));
	}

	@Test
	public void repeatWithNegativeCountRaisesParameter() {
		put("hello");
		assertParamError(StringOperation.repeat(POLICY, BIN, -1));
	}

	@Test
	public void regexReplaceWithInvalidPatternRaisesParameterError() {
		put("hello");
		// Unclosed character class — PCRE2 compile fails inside the op.
		// Server returns PARAMETER_ERROR (the server doc table lists this row as
		// "OP_NOT_APPLICABLE / error"; observed behavior on 8.1.3 is PARAMETER).
		assertParamError(StringOperation.regexReplace(
			POLICY, BIN, "[unclosed", "NUM", StringRegexFlags.DEFAULT));
	}

	@Test
	public void regexReplaceNoFailSuppressesInvalidPattern() {
		// regexReplace carries both a regex-flags and a policy-flags argument; the
		// policy slot is the third and last. NO_FAIL there suppresses the compile
		// failure the test above asserts, leaving the bin untouched.
		put("hello");

		StringPolicy noFail = new StringPolicy(StringWriteFlags.NO_FAIL);
		operate(StringOperation.regexReplace(
			noFail, BIN, "[unclosed", "NUM", StringRegexFlags.DEFAULT));

		assertEquals("hello", stringValue());
	}

	//=================================================================
	// Result-size cap
	//
	// Modify ops bound their estimated result at prepare time
	// (particle_string.c string_modify_set_estimated_size). Exceeding the
	// bound is PARAMETER_ERROR and nothing is written, so it is reported
	// independently of RECORD_TOO_BIG — which the same ops raise for a
	// result that clears the cap but outgrows the namespace record limit.
	//=================================================================

	@Test
	public void repeatPastResultCapRaisesParameter() {
		put("hello");
		// Estimated as old_size * count.
		assertParamError(StringOperation.repeat(POLICY, BIN, RESULT_SIZE_CAP));
	}

	@Test
	public void padStartPastResultCapRaisesParameter() {
		put("hello");
		// Estimated as targetLength * 4 — worst-case UTF-8 expansion.
		assertParamError(StringOperation.padStart(POLICY, BIN, RESULT_SIZE_CAP / 4 + 1, "*"));
	}

	@Test
	public void padEndPastResultCapRaisesParameter() {
		put("hello");
		assertParamError(StringOperation.padEnd(POLICY, BIN, RESULT_SIZE_CAP / 4 + 1, "*"));
	}

	@Test
	public void concatPastResultCapRaisesParameter() {
		put("hello");
		// Estimated as old_size + argument size, so only the argument can carry
		// the result past the cap.
		char[] filler = new char[RESULT_SIZE_CAP];
		Arrays.fill(filler, 'x');
		assertParamError(StringOperation.concat(POLICY, BIN, new String(filler)));
	}
}

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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.operation.StringOperation;
import com.aerospike.client.operation.StringPolicy;
import com.aerospike.client.operation.StringRegexFlags;
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

	@BeforeClass
	public static void serverVersionCheck() {
		Assume.assumeTrue(
			"Skipping: string operations require server version 8.1.3 or later",
			args.serverVersion.isGreaterOrEqual(8, 1, 2, 0));
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
	public void containsReturnsBooleanIntegers() {
		put("hello world");
		Record present = operate(StringOperation.contains(BIN, "hello"));
		Record absent = operate(StringOperation.contains(BIN, "xyz"));
		assertEquals(1L, present.getLong(BIN));
		assertEquals(0L, absent.getLong(BIN));
	}

	@Test
	public void startsWithMatchesPrefix() {
		put("Hello123World");
		assertEquals(1L, operate(StringOperation.startsWith(BIN, "Hello")).getLong(BIN));
		assertEquals(0L, operate(StringOperation.startsWith(BIN, "World")).getLong(BIN));
	}

	@Test
	public void endsWithMatchesSuffix() {
		put("Hello123World");
		assertEquals(1L, operate(StringOperation.endsWith(BIN, "World")).getLong(BIN));
		assertEquals(0L, operate(StringOperation.endsWith(BIN, "Hello")).getLong(BIN));
	}

	@Test
	public void isUpperOnlyTrueForUppercase() {
		put("HELLO");
		assertEquals(1L, operate(StringOperation.isUpper(BIN)).getLong(BIN));
		put("hello");
		assertEquals(0L, operate(StringOperation.isUpper(BIN)).getLong(BIN));
	}

	@Test
	public void isLowerOnlyTrueForLowercase() {
		put("hello");
		assertEquals(1L, operate(StringOperation.isLower(BIN)).getLong(BIN));
		put("HELLO");
		assertEquals(0L, operate(StringOperation.isLower(BIN)).getLong(BIN));
	}

	@Test
	public void isNumericMatchesIntegerStrings() {
		put("12345");
		assertEquals(1L, operate(StringOperation.isNumeric(BIN)).getLong(BIN));
		put("Hello123World");
		assertEquals(0L, operate(StringOperation.isNumeric(BIN)).getLong(BIN));
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
		assertEquals(1L, operate(StringOperation.regexCompare(BIN, "[0-9]+")).getLong(BIN));
		put("HELLO");
		assertEquals(0L, operate(StringOperation.regexCompare(BIN, "[0-9]+")).getLong(BIN));
	}

	@Test
	public void regexCompareHonorsCaseInsensitiveFlag() {
		put("HELLO");
		assertEquals(1L, operate(StringOperation.regexCompare(
			BIN, "hello", StringRegexFlags.CASE_INSENSITIVE)).getLong(BIN));
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

		// strlen and toInteger return INT; isUpper returns 0/1.
		assertEquals(15L, r.getLong("text"));
		assertEquals(12345L, r.getLong("number_str"));
		assertEquals(1L, r.getLong("upper_str"));
	}

	@Test
	public void modifyAndReadInOneOperatePipelineCommitsThenObserves() {
		put("  hello world  ");

		Record r = operate(
			StringOperation.trim(POLICY, BIN),
			StringOperation.upper(POLICY, BIN),
			StringOperation.strlen(BIN));

		// strlen runs after trim+upper so it sees the post-modification length.
		assertEquals(11L, r.getLong(BIN));
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
}

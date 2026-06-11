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
package com.aerospike.client.exp;

import com.aerospike.client.operation.StringPolicy;
import com.aerospike.client.operation.StringRegexFlags;
import com.aerospike.client.util.Pack;
import com.aerospike.client.util.Packer;

/**
 * String expression generator. Produces {@link Exp} nodes that read or transform
 * string values inside an Aerospike {@link Expression}. Mirrors the operations
 * exposed by {@link com.aerospike.client.operation.StringOperation}, but composes
 * inside expressions instead of being sent as standalone operate ops.
 * <p>
 * Each builder takes an {@code Exp src} that produces the string to operate on.
 * Common sources:
 * <ul>
 * <li>{@link Exp#stringBin(String)} — read a string bin.</li>
 * <li>{@link Exp#val(String)} — a string literal.</li>
 * <li>Another {@code StringExp} expression — chains read/transform ops.</li>
 * </ul>
 * <p>
 * Modify-style expressions (e.g. {@link #upper}, {@link #replace}) return the
 * <strong>modified string value</strong>; they do not mutate the underlying bin.
 * To persist a change, write the returned value back via
 * {@link com.aerospike.client.exp.Exp.Build} or use
 * {@link com.aerospike.client.operation.StringOperation} for direct ops.
 * <p>
 * Index orientation is left-to-right with codepoint addressing. Negative indexes
 * count from the end of the string ({@code -1} = last codepoint). Out-of-bounds
 * indexes are clamped to the valid range; no error is returned.
 * <p>
 * Unlike {@link com.aerospike.client.operation.StringOperation}, these builders
 * do <strong>not</strong> accept a {@link com.aerospike.client.cdt.CTX}. To apply
 * a string expression to a value nested inside a list or map, compose with
 * {@link com.aerospike.client.exp.ListExp#getByIndex} or
 * {@link com.aerospike.client.exp.MapExp#getByKey} (which do take CTX) to extract
 * the leaf, then pass the resulting {@code Exp} as {@code src}.
 * <p>
 * String expressions require server version 8.1.3 or later.
 *
 * <pre>{@code
 * // Filter records whose "name" bin starts with "hello".
 * Expression filter = Exp.build(
 *     Exp.eq(
 *         StringExp.startsWith(Exp.val("hello"), Exp.stringBin("name")),
 *         Exp.val(1)));
 * }</pre>
 */
public final class StringExp {
	private static final int MODULE = 3;       // CALL_STRING
	private static final int MODULE_REPR = 4;  // CALL_REPR

	// Read ops
	private static final int STRLEN = 0;
	private static final int SUBSTR = 1;
	private static final int CHAR_AT = 2;
	private static final int FIND = 3;
	private static final int CONTAINS = 4;
	private static final int STARTS_WITH = 5;
	private static final int ENDS_WITH = 6;
	private static final int TO_INTEGER = 7;
	private static final int TO_DOUBLE = 8;
	private static final int BYTE_LENGTH = 9;
	private static final int IS_NUMERIC = 10;
	private static final int IS_UPPER = 11;
	private static final int IS_LOWER = 12;
	private static final int TO_BLOB = 13;
	private static final int SPLIT = 14;
	private static final int B64_DECODE = 15;
	private static final int REGEX_COMPARE = 16;

	// Modify ops
	private static final int INSERT = 50;
	private static final int OVERWRITE = 51;
	private static final int CONCAT = 52;
	private static final int SNIP = 53;
	private static final int REPLACE = 54;
	private static final int REPLACE_ALL = 55;
	private static final int UPPER = 56;
	private static final int LOWER = 57;
	private static final int CASE_FOLD = 58;
	private static final int NORMALIZE_NFC = 59;
	private static final int TRIM_START = 60;
	private static final int TRIM_END = 61;
	private static final int TRIM = 62;
	private static final int PAD_START = 63;
	private static final int PAD_END = 64;
	private static final int REPEAT = 65;
	private static final int REGEX_REPLACE = 66;

	//-----------------------------------------------------------------
	// Read expressions
	//-----------------------------------------------------------------

	/**
	 * Create expression that returns the number of Unicode codepoints in {@code src}
	 * as an int64. Equivalent to {@link String#codePointCount(int, int)} on the source.
	 * <p>
	 * The returned value is the codepoint count — <strong>not</strong> the count of
	 * user-perceived characters (grapheme clusters). They agree for ASCII / simple
	 * Latin text but diverge for combining marks, emoji modifiers, and ZWJ sequences
	 * (see {@link com.aerospike.client.operation.StringOperation#strlen} for examples).
	 * For UTF-8 byte length, use {@link #byteLength(Exp)}.
	 *
	 * <pre>{@code
	 * // "hello world" -> 11
	 * Exp len = StringExp.strlen(Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param src	source string expression
	 * @return		integer-typed expression yielding the codepoint count
	 */
	public static Exp strlen(Exp src) {
		byte[] bytes = Pack.pack(STRLEN);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns the substring of {@code src} from codepoint
	 * {@code start} to the end. Negative {@code start} counts from the end of the
	 * string.
	 *
	 * <pre>{@code
	 * // "hello world" from 6 -> "world"
	 * Exp tail = StringExp.substr(Exp.val(6), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param start	starting codepoint index (negative counts from end)
	 * @param src	source string expression
	 * @return		string-typed expression yielding the substring
	 */
	public static Exp substr(Exp start, Exp src) {
		byte[] bytes = Pack.pack(SUBSTR, start);
		return addRead(src, bytes, Exp.Type.STRING);
	}

	/**
	 * Create expression that returns the codepoints of {@code src} from {@code start}
	 * (inclusive) to {@code end} (exclusive). Negative indexes count from the end.
	 * If, after negative-index normalization, {@code start >= end}, the result is the
	 * empty string.
	 *
	 * <pre>{@code
	 * // "hello world" [0, 5) -> "hello"
	 * Exp head = StringExp.substr(Exp.val(0), Exp.val(5), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param start	starting codepoint index, inclusive (negative counts from end)
	 * @param end	end codepoint index, exclusive (negative counts from end)
	 * @param src	source string expression
	 * @return		string-typed expression yielding the substring
	 */
	public static Exp substr(Exp start, Exp end, Exp src) {
		byte[] bytes = Pack.pack(SUBSTR, start, end);
		return addRead(src, bytes, Exp.Type.STRING);
	}

	/**
	 * Create expression that returns the codepoint at {@code index} of {@code src}
	 * as a one-codepoint string. Negative indexes count from the end.
	 *
	 * <pre>{@code
	 * // "Hello123World" at 5 -> "1"
	 * Exp c = StringExp.charAt(Exp.val(5), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param index	codepoint index (negative counts from end)
	 * @param src	source string expression
	 * @return		string-typed expression yielding a single-codepoint string
	 */
	public static Exp charAt(Exp index, Exp src) {
		byte[] bytes = Pack.pack(CHAR_AT, index);
		return addRead(src, bytes, Exp.Type.STRING);
	}

	/**
	 * Create expression that returns the codepoint index of the first occurrence of
	 * {@code needle} in {@code src}, or {@code -1} if not found.
	 *
	 * <pre>{@code
	 * // "hello world" find "world" -> 6
	 * Exp idx = StringExp.find(Exp.val("world"), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param needle	substring to search for (any expression yielding a string)
	 * @param src		source string expression
	 * @return			integer-typed expression: codepoint index, or -1 if absent
	 */
	public static Exp find(Exp needle, Exp src) {
		byte[] bytes = Pack.pack(FIND, needle);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that returns the codepoint index of the {@code occurrence}-th
	 * match of {@code needle} ({@code 1} = first, {@code -1} = last), or {@code -1}
	 * if not found.
	 *
	 * <pre>{@code
	 * // "ababab" 2nd occurrence of "ab" -> 2
	 * Exp idx = StringExp.find(Exp.val("ab"), Exp.val(2), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param needle		substring to search for
	 * @param occurrence	1-based occurrence to return (negative counts from the last)
	 * @param src			source string expression
	 * @return				integer-typed expression: codepoint index, or -1 if absent
	 */
	public static Exp find(Exp needle, Exp occurrence, Exp src) {
		byte[] bytes = Pack.pack(FIND, needle, occurrence);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that tests whether {@code src} contains {@code needle} as a
	 * substring. Returns an integer flag: {@code 1} on match, {@code 0} otherwise.
	 *
	 * <pre>{@code
	 * Expression filter = Exp.build(Exp.eq(
	 *     StringExp.contains(Exp.val("hello"), Exp.stringBin("text")),
	 *     Exp.val(1)));
	 * }</pre>
	 *
	 * @param needle	substring to test for
	 * @param src		source string expression
	 * @return			integer-typed expression: 1 on match, 0 otherwise
	 */
	public static Exp contains(Exp needle, Exp src) {
		byte[] bytes = Pack.pack(CONTAINS, needle);
		return addRead(src, bytes, Exp.Type.BOOL);
	}

	/**
	 * Create expression that tests whether {@code src} begins with {@code prefix}.
	 * Returns an integer flag: {@code 1} on match, {@code 0} otherwise.
	 *
	 * <pre>{@code
	 * Exp matched = StringExp.startsWith(Exp.val("Hello"), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param prefix	prefix to test for
	 * @param src		source string expression
	 * @return			integer-typed expression: 1 on match, 0 otherwise
	 */
	public static Exp startsWith(Exp prefix, Exp src) {
		byte[] bytes = Pack.pack(STARTS_WITH, prefix);
		return addRead(src, bytes, Exp.Type.BOOL);
	}

	/**
	 * Create expression that tests whether {@code src} ends with {@code suffix}.
	 * Returns an integer flag: {@code 1} on match, {@code 0} otherwise.
	 *
	 * <pre>{@code
	 * Exp matched = StringExp.endsWith(Exp.val("World"), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param suffix	suffix to test for
	 * @param src		source string expression
	 * @return			integer-typed expression: 1 on match, 0 otherwise
	 */
	public static Exp endsWith(Exp suffix, Exp src) {
		byte[] bytes = Pack.pack(ENDS_WITH, suffix);
		return addRead(src, bytes, Exp.Type.BOOL);
	}

	/**
	 * Create expression that parses {@code src} as an int64. The expression returns
	 * an error if the source cannot be parsed as an integer.
	 *
	 * <pre>{@code
	 * // "12345" -> 12345
	 * Exp n = StringExp.toInteger(Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param src	source string expression
	 * @return		integer-typed expression yielding the parsed int64
	 */
	public static Exp toInteger(Exp src) {
		byte[] bytes = Pack.pack(TO_INTEGER);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that parses {@code src} as a 64-bit float. The expression
	 * returns an error if the source cannot be parsed as a double.
	 *
	 * <pre>{@code
	 * // "3.14" -> 3.14
	 * Exp v = StringExp.toDouble(Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param src	source string expression
	 * @return		float-typed expression yielding the parsed double
	 */
	public static Exp toDouble(Exp src) {
		byte[] bytes = Pack.pack(TO_DOUBLE);
		return addRead(src, bytes, Exp.Type.FLOAT);
	}

	/**
	 * Create expression that returns the UTF-8 byte length of {@code src} as an int64.
	 * Differs from {@link #strlen} for non-ASCII content where one codepoint can encode
	 * to multiple bytes.
	 *
	 * <pre>{@code
	 * // "hello" -> 5
	 * Exp len = StringExp.byteLength(Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param src	source string expression
	 * @return		integer-typed expression yielding the UTF-8 byte length
	 */
	public static Exp byteLength(Exp src) {
		byte[] bytes = Pack.pack(BYTE_LENGTH);
		return addRead(src, bytes, Exp.Type.INT);
	}

	/**
	 * Create expression that tests whether {@code src} contains a valid integer or
	 * float literal. Returns an integer flag: {@code 1} on match, {@code 0} otherwise.
	 *
	 * <pre>{@code
	 * Exp numeric = StringExp.isNumeric(Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param src	source string expression
	 * @return		integer-typed expression: 1 if numeric, 0 otherwise
	 */
	public static Exp isNumeric(Exp src) {
		byte[] bytes = Pack.pack(IS_NUMERIC);
		return addRead(src, bytes, Exp.Type.BOOL);
	}

	/**
	 * Create expression that tests whether {@code src} parses as a number of the
	 * requested {@link com.aerospike.client.operation.StringNumericType}. Returns an
	 * integer flag: {@code 1} on match, {@code 0} otherwise.
	 *
	 * <pre>{@code
	 * // restrict to integer-only validation
	 * Exp isInt = StringExp.isNumeric(StringNumericType.INT, Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param numericType	one of the {@link com.aerospike.client.operation.StringNumericType} constants
	 * @param src			source string expression
	 * @return				integer-typed expression: 1 if numeric of the given type, 0 otherwise
	 */
	public static Exp isNumeric(int numericType, Exp src) {
		byte[] bytes = Pack.pack(IS_NUMERIC, numericType);
		return addRead(src, bytes, Exp.Type.BOOL);
	}

	/**
	 * Create expression that tests whether every cased codepoint in {@code src} is
	 * uppercase. Returns an integer flag: {@code 1} on match, {@code 0} otherwise.
	 *
	 * <pre>{@code
	 * Exp upper = StringExp.isUpper(Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param src	source string expression
	 * @return		integer-typed expression: 1 if all-uppercase, 0 otherwise
	 */
	public static Exp isUpper(Exp src) {
		byte[] bytes = Pack.pack(IS_UPPER);
		return addRead(src, bytes, Exp.Type.BOOL);
	}

	/**
	 * Create expression that tests whether every cased codepoint in {@code src} is
	 * lowercase. Returns an integer flag: {@code 1} on match, {@code 0} otherwise.
	 *
	 * <pre>{@code
	 * Exp lower = StringExp.isLower(Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param src	source string expression
	 * @return		integer-typed expression: 1 if all-lowercase, 0 otherwise
	 */
	public static Exp isLower(Exp src) {
		byte[] bytes = Pack.pack(IS_LOWER);
		return addRead(src, bytes, Exp.Type.BOOL);
	}

	/**
	 * Create expression that returns the UTF-8 bytes of {@code src} as a blob.
	 *
	 * <pre>{@code
	 * // "hello" -> [0x68, 0x65, 0x6c, 0x6c, 0x6f]
	 * Exp blob = StringExp.toBlob(Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param src	source string expression
	 * @return		blob-typed expression yielding the UTF-8 byte array
	 */
	public static Exp toBlob(Exp src) {
		byte[] bytes = Pack.pack(TO_BLOB);
		return addRead(src, bytes, Exp.Type.BLOB);
	}

	/**
	 * Create expression that splits {@code src} by Unicode codepoint — each codepoint
	 * becomes its own list element.
	 *
	 * <pre>{@code
	 * // "abc" -> ["a", "b", "c"]
	 * Exp parts = StringExp.split(Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param src	source string expression
	 * @return		list-typed expression yielding a list of single-codepoint strings
	 */
	public static Exp split(Exp src) {
		byte[] bytes = Pack.pack(SPLIT);
		return addRead(src, bytes, Exp.Type.LIST);
	}

	/**
	 * Create expression that splits {@code src} by the {@code separator} substring.
	 * If the separator is absent, the result is a singleton list containing the whole
	 * source.
	 *
	 * <pre>{@code
	 * // "one,two,three" with "," -> ["one", "two", "three"]
	 * Exp tokens = StringExp.split(Exp.val(","), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param separator	substring used to split the source
	 * @param src		source string expression
	 * @return			list-typed expression yielding the token list
	 */
	public static Exp split(Exp separator, Exp src) {
		byte[] bytes = Pack.pack(SPLIT, separator);
		return addRead(src, bytes, Exp.Type.LIST);
	}

	/**
	 * Create expression that base64-decodes {@code src} and returns the decoded
	 * bytes as a blob.
	 *
	 * <pre>{@code
	 * // "aGVsbG8=" -> "hello".getBytes()
	 * Exp decoded = StringExp.b64Decode(Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param src	source string expression holding base64 text
	 * @return		blob-typed expression yielding the decoded bytes
	 */
	public static Exp b64Decode(Exp src) {
		byte[] bytes = Pack.pack(B64_DECODE);
		return addRead(src, bytes, Exp.Type.BLOB);
	}

	/**
	 * Create expression that tests whether {@code pattern} (ICU regex syntax) matches
	 * {@code src}. Returns an integer flag: {@code 1} on match, {@code 0} otherwise.
	 *
	 * <pre>{@code
	 * // matches if "text" contains any digit run
	 * Exp matched = StringExp.regexCompare(Exp.val("[0-9]+"), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param pattern	ICU-syntax regex pattern (must be valid UTF-8)
	 * @param src		source string expression
	 * @return			integer-typed expression: 1 on match, 0 otherwise
	 */
	public static Exp regexCompare(Exp pattern, Exp src) {
		byte[] bytes = Pack.pack(REGEX_COMPARE, pattern);
		return addRead(src, bytes, Exp.Type.BOOL);
	}

	/**
	 * Create expression that tests whether {@code pattern} matches {@code src} under
	 * the supplied {@link StringRegexFlags}. Flags can be combined with bitwise OR.
	 * Returns an integer flag: {@code 1} on match, {@code 0} otherwise.
	 *
	 * <pre>{@code
	 * Exp matched = StringExp.regexCompare(
	 *     Exp.val("hello"), StringRegexFlags.CASE_INSENSITIVE,
	 *     Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param pattern		ICU-syntax regex pattern (must be valid UTF-8)
	 * @param regexFlags	bitwise-OR of {@link StringRegexFlags} constants
	 * @param src			source string expression
	 * @return				integer-typed expression: 1 on match, 0 otherwise
	 */
	public static Exp regexCompare(Exp pattern, int regexFlags, Exp src) {
		byte[] bytes = Pack.pack(REGEX_COMPARE, pattern, regexFlags);
		return addRead(src, bytes, Exp.Type.BOOL);
	}

	//-----------------------------------------------------------------
	// Modify expressions
	//-----------------------------------------------------------------

	/**
	 * Create expression that splices {@code value} into {@code src} at codepoint
	 * {@code index} and returns the resulting string. Negative indexes count from the
	 * end. Does not modify the underlying bin.
	 *
	 * <pre>{@code
	 * // "hello world" insert " beautiful" at 5 -> "hello beautiful world"
	 * Exp out = StringExp.insert(StringPolicy.Default,
	 *     Exp.val(5), Exp.val(" beautiful"), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param index		codepoint index at which to insert (negative counts from end)
	 * @param value		text to insert
	 * @param src		source string expression
	 * @return			string-typed expression yielding the modified string
	 */
	public static Exp insert(StringPolicy policy, Exp index, Exp value, Exp src) {
		byte[] bytes = Pack.pack(INSERT, index, value, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that overwrites codepoints in {@code src} starting at codepoint
	 * {@code index} with {@code value}, returning the resulting string. The result may
	 * grow beyond the original length when {@code value} extends past the end. Does not
	 * modify the underlying bin.
	 *
	 * <pre>{@code
	 * // "hello world" overwrite "earth" at 6 -> "hello earth"
	 * Exp out = StringExp.overwrite(StringPolicy.Default,
	 *     Exp.val(6), Exp.val("earth"), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param index		codepoint index at which to start overwriting
	 * @param value		text to write
	 * @param src		source string expression
	 * @return			string-typed expression yielding the modified string
	 */
	public static Exp overwrite(StringPolicy policy, Exp index, Exp value, Exp src) {
		byte[] bytes = Pack.pack(OVERWRITE, index, value, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that concatenates {@code values} (a list of strings) onto
	 * {@code src} in order, returning the resulting string. Does not modify the
	 * underlying bin.
	 *
	 * <pre>{@code
	 * // "hello" + [" ", "big", " world"] -> "hello big world"
	 * Exp out = StringExp.concat(StringPolicy.Default,
	 *     Exp.val(Arrays.asList(" ", "big", " world")),
	 *     Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param values	expression yielding a list of strings to append
	 * @param src		source string expression
	 * @return			string-typed expression yielding the modified string
	 */
	public static Exp concat(StringPolicy policy, Exp values, Exp src) {
		byte[] bytes = Pack.pack(CONCAT, values, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that removes the half-open codepoint range {@code [start, end)}
	 * from {@code src} and returns the resulting string. Does not modify the underlying
	 * bin.
	 *
	 * <pre>{@code
	 * // "hello beautiful world" snip [5, 15) -> "hello world"
	 * Exp out = StringExp.snip(StringPolicy.Default,
	 *     Exp.val(5), Exp.val(15), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param start		first codepoint to remove (inclusive)
	 * @param end		one past the last codepoint to remove (exclusive)
	 * @param src		source string expression
	 * @return			string-typed expression yielding the modified string
	 */
	public static Exp snip(StringPolicy policy, Exp start, Exp end, Exp src) {
		byte[] bytes = Pack.pack(SNIP, start, end, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that replaces the first occurrence of {@code needle} in
	 * {@code src} with {@code replacement} and returns the resulting string. Does not
	 * modify the underlying bin.
	 *
	 * <pre>{@code
	 * // "hello world world" replace "world"->"earth" -> "hello earth world"
	 * Exp out = StringExp.replace(StringPolicy.Default,
	 *     Exp.val("world"), Exp.val("earth"), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy		write policy controlling NO_FAIL semantics
	 * @param needle		substring to find
	 * @param replacement	text to substitute (may be empty to delete the match)
	 * @param src			source string expression
	 * @return				string-typed expression yielding the modified string
	 */
	public static Exp replace(StringPolicy policy, Exp needle, Exp replacement, Exp src) {
		byte[] bytes = packReplace(REPLACE, needle, replacement, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that replaces every occurrence of {@code needle} in {@code src}
	 * with {@code replacement} and returns the resulting string. Does not modify the
	 * underlying bin.
	 *
	 * <pre>{@code
	 * // "aabaa" replaceAll "a"->"x" -> "xxbxx"
	 * Exp out = StringExp.replaceAll(StringPolicy.Default,
	 *     Exp.val("a"), Exp.val("x"), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy		write policy controlling NO_FAIL semantics
	 * @param needle		substring to find
	 * @param replacement	text to substitute (may be empty to delete each match)
	 * @param src			source string expression
	 * @return				string-typed expression yielding the modified string
	 */
	public static Exp replaceAll(StringPolicy policy, Exp needle, Exp replacement, Exp src) {
		byte[] bytes = packReplace(REPLACE_ALL, needle, replacement, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns {@code src} uppercased. Does not modify the
	 * underlying bin.
	 *
	 * <pre>{@code
	 * Exp out = StringExp.upper(StringPolicy.Default, Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param src		source string expression
	 * @return			string-typed expression yielding the uppercased string
	 */
	public static Exp upper(StringPolicy policy, Exp src) {
		byte[] bytes = Pack.pack(UPPER, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns {@code src} lowercased. Does not modify the
	 * underlying bin.
	 *
	 * <pre>{@code
	 * Exp out = StringExp.lower(StringPolicy.Default, Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param src		source string expression
	 * @return			string-typed expression yielding the lowercased string
	 */
	public static Exp lower(StringPolicy policy, Exp src) {
		byte[] bytes = Pack.pack(LOWER, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns {@code src} case-folded (locale-independent
	 * lowercase). Useful for normalized comparison keys. Does not modify the underlying
	 * bin.
	 *
	 * <pre>{@code
	 * Exp out = StringExp.caseFold(StringPolicy.Default, Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param src		source string expression
	 * @return			string-typed expression yielding the case-folded string
	 */
	public static Exp caseFold(StringPolicy policy, Exp src) {
		byte[] bytes = Pack.pack(CASE_FOLD, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns {@code src} normalized to Unicode NFC form.
	 * Already-normalized strings are unchanged. Does not modify the underlying bin.
	 *
	 * <pre>{@code
	 * Exp out = StringExp.normalizeNFC(StringPolicy.Default, Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param src		source string expression
	 * @return			string-typed expression yielding the NFC-normalized string
	 */
	public static Exp normalizeNFC(StringPolicy policy, Exp src) {
		byte[] bytes = Pack.pack(NORMALIZE_NFC, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns {@code src} with whitespace removed from the start.
	 * Does not modify the underlying bin.
	 *
	 * <pre>{@code
	 * // "  hello  " -> "hello  "
	 * Exp out = StringExp.trimStart(StringPolicy.Default, Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param src		source string expression
	 * @return			string-typed expression yielding the left-trimmed string
	 */
	public static Exp trimStart(StringPolicy policy, Exp src) {
		byte[] bytes = Pack.pack(TRIM_START, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns {@code src} with whitespace removed from the end.
	 * Does not modify the underlying bin.
	 *
	 * <pre>{@code
	 * // "  hello  " -> "  hello"
	 * Exp out = StringExp.trimEnd(StringPolicy.Default, Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param src		source string expression
	 * @return			string-typed expression yielding the right-trimmed string
	 */
	public static Exp trimEnd(StringPolicy policy, Exp src) {
		byte[] bytes = Pack.pack(TRIM_END, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns {@code src} with whitespace removed from both
	 * ends. Does not modify the underlying bin.
	 *
	 * <pre>{@code
	 * // "  hello world  " -> "hello world"
	 * Exp out = StringExp.trim(StringPolicy.Default, Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param src		source string expression
	 * @return			string-typed expression yielding the trimmed string
	 */
	public static Exp trim(StringPolicy policy, Exp src) {
		byte[] bytes = Pack.pack(TRIM, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that prepends {@code padString} to {@code src} repeatedly until
	 * the result reaches {@code targetLength} codepoints. No-op when the source is
	 * already at or above the target length. Does not modify the underlying bin.
	 *
	 * <pre>{@code
	 * // "hello" pad to 10 with "*" -> "*****hello"
	 * Exp out = StringExp.padStart(StringPolicy.Default,
	 *     Exp.val(10), Exp.val("*"), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy		write policy controlling NO_FAIL semantics
	 * @param targetLength	codepoint length to pad up to
	 * @param padString		text used to fill (repeated as needed)
	 * @param src			source string expression
	 * @return				string-typed expression yielding the padded string
	 */
	public static Exp padStart(StringPolicy policy, Exp targetLength, Exp padString, Exp src) {
		byte[] bytes = Pack.pack(PAD_START, targetLength, padString, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that appends {@code padString} to {@code src} repeatedly until
	 * the result reaches {@code targetLength} codepoints. No-op when the source is
	 * already at or above the target length. Does not modify the underlying bin.
	 *
	 * <pre>{@code
	 * // "hello" pad to 10 with "." -> "hello....."
	 * Exp out = StringExp.padEnd(StringPolicy.Default,
	 *     Exp.val(10), Exp.val("."), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy		write policy controlling NO_FAIL semantics
	 * @param targetLength	codepoint length to pad up to
	 * @param padString		text used to fill (repeated as needed)
	 * @param src			source string expression
	 * @return				string-typed expression yielding the padded string
	 */
	public static Exp padEnd(StringPolicy policy, Exp targetLength, Exp padString, Exp src) {
		byte[] bytes = Pack.pack(PAD_END, targetLength, padString, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that returns {@code src} repeated {@code count} times. Does
	 * not modify the underlying bin.
	 *
	 * <pre>{@code
	 * // "ab" repeat 3 -> "ababab"
	 * Exp out = StringExp.repeat(StringPolicy.Default,
	 *     Exp.val(3), Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param count		number of repetitions (must be non-negative)
	 * @param src		source string expression
	 * @return			string-typed expression yielding the repeated string
	 */
	public static Exp repeat(StringPolicy policy, Exp count, Exp src) {
		byte[] bytes = Pack.pack(REPEAT, count, policy.flags);
		return addModify(src, bytes);
	}

	/**
	 * Create expression that replaces matches of {@code pattern} (ICU regex syntax) in
	 * {@code src} with {@code replacement} and returns the resulting string. Pass
	 * {@link StringRegexFlags#GLOBAL} to replace every match. Flag values may be
	 * combined with bitwise OR. Does not modify the underlying bin.
	 *
	 * <pre>{@code
	 * // "abc123def456" regexReplace "[0-9]+"->"NUM" with GLOBAL -> "abcNUMdefNUM"
	 * Exp out = StringExp.regexReplace(StringPolicy.Default,
	 *     Exp.val("[0-9]+"), Exp.val("NUM"), StringRegexFlags.GLOBAL,
	 *     Exp.stringBin("text"));
	 * }</pre>
	 *
	 * @param policy		kept for API symmetry with the other modify ops; unused — the
	 *						regex_replace server op does not accept policy flags
	 *						(see implementation note)
	 * @param pattern		ICU-syntax regex pattern (must be valid UTF-8)
	 * @param replacement	replacement text (must be valid UTF-8)
	 * @param regexFlags	bitwise-OR of {@link StringRegexFlags} constants
	 * @param src			source string expression
	 * @return				string-typed expression yielding the modified string
	 */
	public static Exp regexReplace(
		StringPolicy policy,
		Exp pattern,
		Exp replacement,
		int regexFlags,
		Exp src
	) {
		byte[] bytes = packRegexReplace(pattern, replacement, regexFlags);
		return addModify(src, bytes);
	}

	//-----------------------------------------------------------------
	// Type conversion expression
	//-----------------------------------------------------------------

	/**
	 * Create expression that returns the string representation of {@code src}, where
	 * {@code src} may be any expression yielding an integer, float, string, or blob
	 * value. Returns an error for any other source type.
	 *
	 * <pre>{@code
	 * // integer bin "n" = 42 -> "42"
	 * Exp s = StringExp.toString(Exp.intBin("n"));
	 * }</pre>
	 *
	 * @param src	source expression (integer, float, string, or blob)
	 * @return		string-typed expression yielding the string representation
	 */
	public static Exp toString(Exp src) {
		byte[] bytes = reprPayload();
		return new Exp.Module(src, bytes, Exp.Type.STRING.code, MODULE_REPR);
	}

	//-----------------------------------------------------------------
	// Private helpers
	//-----------------------------------------------------------------

	private static Exp addRead(Exp src, byte[] bytes, Exp.Type retType) {
		return new Exp.Module(src, bytes, retType.code, MODULE);
	}

	private static Exp addModify(Exp src, byte[] bytes) {
		return new Exp.Module(src, bytes, Exp.Type.STRING.code, MODULE | Exp.MODIFY);
	}

	// QUOTED opcode (mirrors Exp.QUOTED = 126; the constant is private in Exp.java).
	// Used to mark an inner msgpack list as a literal — without it, the server's
	// expression compiler at exp.c:3289 treats any bare nested list inside a CALL
	// payload as a sub-expression and recursively compiles its first element as an
	// opcode, which fails with PARAMETER_ERROR for our string-pair lists.
	private static final int QUOTED = 126;

	// [cmd, [QUOTED, [needle, repl]], flags] — needle/replacement nested inside a
	// QUOTED-wrapped 2-element list so the expression compiler treats it as a literal
	// rather than a sub-expression. The direct-op path packs the same logical shape
	// (without QUOTED) because it bypasses the expression engine — see
	// StringOperation.packStringOp(int, List<Value>, int, CTX[]).
	private static byte[] packReplace(int command, Exp needle, Exp replacement, int flags) {
		Packer packer = new Packer();
		for (int i = 0; i < 2; i++) {
			packer.packArrayBegin(3);
			packer.packInt(command);
			packer.packArrayBegin(2);
			packer.packInt(QUOTED);
			packer.packArrayBegin(2);
			needle.pack(packer);
			replacement.pack(packer);
			packer.packInt(flags);
			if (i == 0) packer.createBuffer();
		}
		return packer.getBuffer();
	}

	// [REGEX_REPLACE, [QUOTED, [pattern, repl]], regexFlags] — same QUOTED wrapping as
	// packReplace; without it the expression compiler tries to interpret the
	// (pattern, replacement) pair as a function call. Note: the server's regex_replace
	// op table is declared with max_args=2 (particle_string.c:476), so there is no
	// trailing policy-flags slot — only the regexFlags integer.
	private static byte[] packRegexReplace(Exp pattern, Exp replacement, int regexFlags) {
		Packer packer = new Packer();
		for (int i = 0; i < 2; i++) {
			packer.packArrayBegin(3);
			packer.packInt(REGEX_REPLACE);
			packer.packArrayBegin(2);
			packer.packInt(QUOTED);
			packer.packArrayBegin(2);
			pattern.pack(packer);
			replacement.pack(packer);
			packer.packInt(regexFlags);
			if (i == 0) packer.createBuffer();
		}
		return packer.getBuffer();
	}

	// Single-zero payload [0] for CALL_REPR (StringExp.toString). The server's
	// parse_op_call at exp.c:3244 rejects an empty list (ele_count == 0), so the
	// payload must contain at least one element. The CALL_REPR dispatcher at
	// exp.c:5019 ignores the sub-op id and goes straight to as_bin_to_string, so
	// the value carried here is unused. The spec previously documented this as `[]`;
	// the server is the source of truth — see §2.7 in the cross-client spec.
	private static byte[] reprPayload() {
		Packer packer = new Packer();
		for (int i = 0; i < 2; i++) {
			packer.packArrayBegin(1);
			packer.packInt(0);
			if (i == 0) packer.createBuffer();
		}
		return packer.getBuffer();
	}
}

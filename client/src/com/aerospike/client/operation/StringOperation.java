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
package com.aerospike.client.operation;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.util.Pack;

/**
 * String operations. Create operations to be passed to the client {@code operate}
 * command for inspecting and modifying string bins.
 * <p>
 * Index orientation is left-to-right with codepoint addressing. Negative indexes
 * count from the end of the string ({@code -1} = last codepoint). Out-of-bounds
 * indexes are clamped to the valid range; no error is returned.
 * <p>
 * String operations require server version 8.1.3 or later. A non-empty {@link CTX}
 * argument navigates into a string nested inside a list or map bin; with no CTX
 * the operation targets the bin itself. The CTX-navigated leaf must already be an
 * Aerospike string — operations on non-string leaves return
 * {@code AEROSPIKE_ERR_INCOMPATIBLE_TYPE}.
 *
 * <pre>{@code
 * // Read: bin "text" = "hello world"
 * Record r = client.operate(null, key, StringOperation.strlen("text"));
 * long len = r.getLong("text");        // 11
 *
 * // Modify: uppercase a string nested in a list bin "items" at index 0.
 * client.operate(null, key,
 *     StringOperation.upper(StringPolicy.Default, "items", CTX.listIndex(0)));
 * }</pre>
 */
public final class StringOperation {
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
	private static final int APPEND = 67;
	private static final int PREPEND = 68;

	//-----------------------------------------------------------------
	// Read operations
	//-----------------------------------------------------------------

	/**
	 * Create string {@code strlen} operation. Returns the number of Unicode codepoints
	 * in the string bin as an int64. This matches {@link String#codePointCount(int, int)}
	 * called on the string's full range.
	 * <p>
	 * The returned value is the codepoint count — <strong>not</strong> the count of
	 * user-perceived characters (grapheme clusters). Codepoints and visible characters
	 * agree for ASCII and simple Latin text, but diverge for combining marks, emoji
	 * modifiers, and zero-width-joiner sequences:
	 * <ul>
	 * <li>{@code "é"} encoded as one precomposed codepoint U+00E9 → 1.</li>
	 * <li>{@code "é"} encoded as {@code 'e' + U+0301} (combining acute) → 2, though
	 *     it renders as one visible character.</li>
	 * <li>{@code "👍🏽"} (thumbs up + skin-tone modifier) → 2, though it renders as
	 *     one emoji.</li>
	 * <li>{@code "👨‍👩‍👧‍👦"} (ZWJ family emoji) → 7, though it renders as one emoji.</li>
	 * </ul>
	 * <p>
	 * Two related counts that this op does <strong>not</strong> return:
	 * <ul>
	 * <li>{@link String#length()} — counts UTF-16 code units, so a non-BMP codepoint
	 *     (e.g. {@code "😀"}) counts as 2 there but 1 here.</li>
	 * <li>UTF-8 byte length — use {@link #byteLength(String, CTX...)}.</li>
	 * </ul>
	 *
	 * <pre>{@code
	 * // ASCII: "hello world" -> 11 (codepoint count == byte length here)
	 * Record r = client.operate(null, key, StringOperation.strlen("text"));
	 * long len = r.getLong("text");
	 *
	 * // Multi-byte UTF-8: "héllo" stores as 6 bytes but 5 codepoints -> 5
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning the codepoint count (int64)
	 */
	public static Operation strlen(String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(STRLEN, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code substr} operation that reads from {@code start} to the end of
	 * the string. Negative indexes count from the end.
	 *
	 * <pre>{@code
	 * // "hello world" -> "world"
	 * Record r = client.operate(null, key, StringOperation.substr("text", 6));
	 * String tail = r.getString("text");
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param start		starting codepoint index (negative counts from end)
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning the substring
	 */
	public static Operation substr(String binName, int start, CTX... ctx) {
		byte[] bytes = Pack.pack(SUBSTR, start, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code substr} operation that returns the codepoints of the bin
	 * from {@code start} (inclusive) to {@code end} (exclusive). Negative indexes
	 * count from the end of the string. If, after negative-index normalization,
	 * {@code start >= end}, the result is the empty string.
	 *
	 * <pre>{@code
	 * // "hello world" [0, 5) -> "hello"
	 * Record r = client.operate(null, key, StringOperation.substr("text", 0, 5));
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param start		starting codepoint index, inclusive (negative counts from end)
	 * @param end		end codepoint index, exclusive (negative counts from end)
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning the substring
	 */
	public static Operation substr(String binName, int start, int end, CTX... ctx) {
		byte[] bytes = Pack.pack(SUBSTR, start, end, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code charAt} operation. Returns the codepoint at {@code index}
	 * as a one-codepoint string. Negative indexes count from the end.
	 *
	 * <pre>{@code
	 * // "Hello123World" at index 5 -> "1"
	 * Record r = client.operate(null, key, StringOperation.charAt("text", 5));
	 * String c = r.getString("text");
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param index		codepoint index (negative counts from end)
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning a single-codepoint string
	 */
	public static Operation charAt(String binName, int index, CTX... ctx) {
		byte[] bytes = Pack.pack(CHAR_AT, index, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code find} operation. Returns the codepoint index of the first
	 * occurrence of {@code needle}, or {@code -1} if not found.
	 *
	 * <pre>{@code
	 * // "hello world" -> 6
	 * Record r = client.operate(null, key, StringOperation.find("text", "world"));
	 * long idx = r.getLong("text");
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param needle	substring to search for
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning the codepoint index, or -1 if absent
	 */
	public static Operation find(String binName, String needle, CTX... ctx) {
		byte[] bytes = Pack.pack(FIND, Value.get(needle), ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code find} operation that locates a specific {@code occurrence}
	 * of {@code needle} ({@code 1} = first match, {@code -1} = last match). Returns the
	 * codepoint index of that match, or {@code -1} if not found.
	 *
	 * <pre>{@code
	 * // "ababab" 2nd occurrence of "ab" -> 2
	 * Record r = client.operate(null, key, StringOperation.find("text", "ab", 2));
	 * }</pre>
	 *
	 * @param binName		name of the string bin
	 * @param needle		substring to search for
	 * @param occurrence	1-based occurrence to return (negative counts from the last match)
	 * @param ctx			optional path into a string nested inside a list or map
	 * @return				read operation returning the codepoint index, or -1 if absent
	 */
	public static Operation find(String binName, String needle, int occurrence, CTX... ctx) {
		byte[] bytes = Pack.pack(FIND, Value.get(needle), occurrence, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code contains} operation. Returns {@code true} if the bin contains
	 * {@code needle} as a substring, {@code false} otherwise.
	 *
	 * <pre>{@code
	 * // "hello world" -> true
	 * Record r = client.operate(null, key, StringOperation.contains("text", "hello"));
	 * boolean has = r.getBoolean("text");
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param needle	substring to test for
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning a boolean match flag
	 */
	public static Operation contains(String binName, String needle, CTX... ctx) {
		byte[] bytes = Pack.pack(CONTAINS, Value.get(needle), ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code startsWith} operation. Returns {@code true} if the bin begins
	 * with {@code prefix}, {@code false} otherwise.
	 *
	 * <pre>{@code
	 * // "Hello123World" -> true
	 * Record r = client.operate(null, key, StringOperation.startsWith("text", "Hello"));
	 * }</pre>
	 *
	 * <p>Matching is Unicode canonical, not byte-exact: a prefix stored in a different
	 * normalization form than the bin still matches.
	 *
	 * @param binName	name of the string bin
	 * @param prefix	prefix to test for
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning a boolean match flag
	 */
	public static Operation startsWith(String binName, String prefix, CTX... ctx) {
		byte[] bytes = Pack.pack(STARTS_WITH, Value.get(prefix), ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code endsWith} operation. Returns {@code true} if the bin ends
	 * with {@code suffix}, {@code false} otherwise.
	 *
	 * <pre>{@code
	 * // "Hello123World" -> true
	 * Record r = client.operate(null, key, StringOperation.endsWith("text", "World"));
	 * }</pre>
	 *
	 * <p>Matching is Unicode canonical, not byte-exact: a suffix stored in a different
	 * normalization form than the bin still matches.
	 *
	 * @param binName	name of the string bin
	 * @param suffix	suffix to test for
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning a boolean match flag
	 */
	public static Operation endsWith(String binName, String suffix, CTX... ctx) {
		byte[] bytes = Pack.pack(ENDS_WITH, Value.get(suffix), ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code toInteger} operation. Parses the string as an int64.
	 * Fails with {@link com.aerospike.client.ResultCode#OP_NOT_APPLICABLE} and subcode
	 * {@link com.aerospike.client.SubCode#OPNOT_STRING_CONVERSION_FAILED} if the bin
	 * cannot be parsed as an integer.
	 *
	 * <pre>{@code
	 * // "12345" -> 12345
	 * Record r = client.operate(null, key, StringOperation.toInteger("text"));
	 * long n = r.getLong("text");
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning the parsed int64
	 */
	public static Operation toInteger(String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(TO_INTEGER, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code toDouble} operation. Parses the string as a 64-bit float.
	 * Fails with {@link com.aerospike.client.ResultCode#OP_NOT_APPLICABLE} and subcode
	 * {@link com.aerospike.client.SubCode#OPNOT_STRING_CONVERSION_FAILED} if the bin
	 * cannot be parsed as a double.
	 *
	 * <pre>{@code
	 * // "3.14" -> 3.14
	 * Record r = client.operate(null, key, StringOperation.toDouble("text"));
	 * double v = r.getDouble("text");
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning the parsed double
	 */
	public static Operation toDouble(String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(TO_DOUBLE, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code byteLength} operation. Returns the number of UTF-8 bytes in
	 * the string (int64). Differs from {@link #strlen} for non-ASCII content where one
	 * codepoint can encode to multiple bytes.
	 *
	 * <pre>{@code
	 * // "hello" -> 5
	 * Record r = client.operate(null, key, StringOperation.byteLength("text"));
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning the byte length (int64)
	 */
	public static Operation byteLength(String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(BYTE_LENGTH, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code isNumeric} operation. Returns {@code true} if the bin
	 * contains a valid integer or float, {@code false} otherwise.
	 *
	 * <pre>{@code
	 * // "12345" -> true; "Hello" -> false
	 * Record r = client.operate(null, key, StringOperation.isNumeric("text"));
	 * boolean numeric = r.getBoolean("text");
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning a boolean match flag
	 */
	public static Operation isNumeric(String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(IS_NUMERIC, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code isNumeric} operation that filters by {@code numericType}
	 * (see {@link StringNumericType}). For example, restrict to integer-only or
	 * float-only validation. Note {@code FLOAT} requires a {@code '.'} followed by
	 * a digit, so {@code "5"} is {@code false} under {@code FLOAT}.
	 *
	 * <pre>{@code
	 * // "12345" with INT filter -> true; with FLOAT filter -> false
	 * Record r = client.operate(null, key,
	 *     StringOperation.isNumeric("text", StringNumericType.INT));
	 * }</pre>
	 *
	 * @param binName		name of the string bin
	 * @param numericType	one of the {@link StringNumericType} constants
	 * @param ctx			optional path into a string nested inside a list or map
	 * @return				read operation returning a boolean match flag
	 */
	public static Operation isNumeric(String binName, int numericType, CTX... ctx) {
		byte[] bytes = Pack.pack(IS_NUMERIC, numericType, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code isUpper} operation. Returns {@code true} if every cased
	 * codepoint in the bin is uppercase, {@code false} otherwise.
	 *
	 * <pre>{@code
	 * // "HELLO" -> true; "Hello" -> false
	 * Record r = client.operate(null, key, StringOperation.isUpper("text"));
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning a boolean match flag
	 */
	public static Operation isUpper(String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(IS_UPPER, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code isLower} operation. Returns {@code true} if every cased
	 * codepoint in the bin is lowercase, {@code false} otherwise.
	 *
	 * <pre>{@code
	 * // "hello" -> true; "Hello" -> false
	 * Record r = client.operate(null, key, StringOperation.isLower("text"));
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning a boolean match flag
	 */
	public static Operation isLower(String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(IS_LOWER, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code toBlob} operation. Returns the UTF-8 bytes of the string
	 * as a blob (byte[]).
	 *
	 * <pre>{@code
	 * // "hello" -> [0x68, 0x65, 0x6c, 0x6c, 0x6f]
	 * Record r = client.operate(null, key, StringOperation.toBlob("text"));
	 * byte[] bytes = (byte[]) r.getValue("text");
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning a byte[] blob
	 */
	public static Operation toBlob(String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(TO_BLOB, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code split} operation that splits by Unicode codepoint — each
	 * codepoint becomes its own element of the returned list.
	 *
	 * <pre>{@code
	 * // "abc" -> ["a", "b", "c"]
	 * Record r = client.operate(null, key, StringOperation.split("text"));
	 * List<?> chars = r.getList("text");
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning a list of single-codepoint strings
	 */
	public static Operation split(String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(SPLIT, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code split} operation that splits the bin by the {@code separator}
	 * substring. If the separator is absent the result is a singleton list containing
	 * the whole string.
	 *
	 * <pre>{@code
	 * // "one,two,three" with "," -> ["one", "two", "three"]
	 * Record r = client.operate(null, key, StringOperation.split("text", ","));
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param separator	substring used to split the bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning a list of token strings
	 */
	public static Operation split(String binName, String separator, CTX... ctx) {
		byte[] bytes = Pack.pack(SPLIT, Value.get(separator), ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code b64Decode} operation. Treats the bin as base64-encoded text
	 * and returns the decoded bytes as a blob. Fails with
	 * {@link com.aerospike.client.ResultCode#OP_NOT_APPLICABLE} and subcode
	 * {@link com.aerospike.client.SubCode#OPNOT_STRING_B64_INVALID} if the bin does not
	 * hold valid base64.
	 *
	 * <pre>{@code
	 * // "aGVsbG8=" -> "hello".getBytes()
	 * Record r = client.operate(null, key, StringOperation.b64Decode("text"));
	 * byte[] decoded = (byte[]) r.getValue("text");
	 * }</pre>
	 *
	 * @param binName	name of the string bin holding base64 text
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning the decoded byte[]
	 */
	public static Operation b64Decode(String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(B64_DECODE, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code regexCompare} operation. Matches {@code pattern} (ICU regex
	 * syntax) against the bin and returns {@code true} on match, {@code false} otherwise.
	 *
	 * <pre>{@code
	 * // "Hello123World" matches "[0-9]+" -> true
	 * Record r = client.operate(null, key, StringOperation.regexCompare("text", "[0-9]+"));
	 * boolean matched = r.getBoolean("text");
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param pattern	ICU-syntax regex pattern (must be valid UTF-8)
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning a boolean match flag
	 */
	public static Operation regexCompare(String binName, String pattern, CTX... ctx) {
		byte[] bytes = Pack.pack(REGEX_COMPARE, Value.get(pattern), ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code regexCompare} operation that honors {@link StringRegexFlags}
	 * (e.g. {@link StringRegexFlags#CASE_INSENSITIVE}). Flag values may be combined
	 * with bitwise OR.
	 *
	 * <pre>{@code
	 * // "HELLO" matches "hello" with CASE_INSENSITIVE -> true
	 * Record r = client.operate(null, key,
	 *     StringOperation.regexCompare("text", "hello", StringRegexFlags.CASE_INSENSITIVE));
	 * }</pre>
	 *
	 * @param binName	name of the string bin
	 * @param pattern	ICU-syntax regex pattern (must be valid UTF-8)
	 * @param regexFlags bitwise-OR of {@link StringRegexFlags} constants
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			read operation returning a boolean match flag
	 */
	public static Operation regexCompare(String binName, String pattern, int regexFlags, CTX... ctx) {
		byte[] bytes = Pack.pack(REGEX_COMPARE, Value.get(pattern), regexFlags, ctx);
		return new Operation(Operation.Type.STRING_READ, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	//-----------------------------------------------------------------
	// Modify operations
	//-----------------------------------------------------------------

	/**
	 * Create string {@code insert} operation that splices {@code value} into the bin at
	 * codepoint {@code index}. Negative indexes count from the end of the string.
	 *
	 * <pre>{@code
	 * // "hello world" + insert " beautiful" at 5 -> "hello beautiful world"
	 * client.operate(null, key,
	 *     StringOperation.insert(StringPolicy.Default, "text", 5, " beautiful"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param index		codepoint index at which to insert (negative counts from end)
	 * @param value		text to insert
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation insert(StringPolicy policy, String binName, int index, String value, CTX... ctx) {
		byte[] bytes = Pack.pack(INSERT, index, Value.get(value), policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code overwrite} operation that overwrites codepoints starting at
	 * codepoint {@code index} with {@code value}. The result may grow beyond the
	 * original length when {@code value} extends past the end.
	 *
	 * <pre>{@code
	 * // "hello world" overwrite "earth" at 6 -> "hello earth"
	 * client.operate(null, key,
	 *     StringOperation.overwrite(StringPolicy.Default, "text", 6, "earth"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param index		codepoint index at which to start overwriting
	 * @param value		text to write
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation overwrite(StringPolicy policy, String binName, int index, String value, CTX... ctx) {
		byte[] bytes = Pack.pack(OVERWRITE, index, Value.get(value), policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code concat} operation that appends {@code value} to the bin.
	 *
	 * <pre>{@code
	 * // "hello" + concat "!" -> "hello!"
	 * client.operate(null, key,
	 *     StringOperation.concat(StringPolicy.Default, "text", "!"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param value		text to append
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation concat(StringPolicy policy, String binName, String value, CTX... ctx) {
		List<Value> list = new ArrayList<Value>(1);
		list.add(Value.get(value));
		byte[] bytes = Pack.pack(CONCAT, list, policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code concat} operation that appends each element of {@code values}
	 * to the bin in order.
	 *
	 * <pre>{@code
	 * // "hello" + concat [" ", "big", " world"] -> "hello big world"
	 * client.operate(null, key, StringOperation.concat(StringPolicy.Default, "text",
	 *     Arrays.asList(" ", "big", " world")));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param values	ordered list of strings to append
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation concat(StringPolicy policy, String binName, List<String> values, CTX... ctx) {
		List<Value> list = toValueList(values);
		byte[] bytes = Pack.pack(CONCAT, list, policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code append} operation that appends {@code value} to the end of the bin.
	 * <p>
	 * Unlike the legacy {@link com.aerospike.client.Operation#append(com.aerospike.client.Bin)}, this
	 * operation is Unicode/DBCS-aware and shares the consistent {@link StringPolicy} / CTX interface of
	 * the rest of the string package.
	 *
	 * <pre>{@code
	 * // "hello" + append "!" -> "hello!"
	 * client.operate(null, key,
	 *     StringOperation.append(StringPolicy.Default, "text", "!"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param value		text to append to the end of the string
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation append(StringPolicy policy, String binName, String value, CTX... ctx) {
		byte[] bytes = Pack.pack(APPEND, Value.get(value), policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code prepend} operation that prepends {@code value} to the start of the bin.
	 * <p>
	 * Unlike the legacy {@link com.aerospike.client.Operation#prepend(com.aerospike.client.Bin)}, this
	 * operation is Unicode/DBCS-aware and shares the consistent {@link StringPolicy} / CTX interface of
	 * the rest of the string package.
	 *
	 * <pre>{@code
	 * // "world" prepend "hello " -> "hello world"
	 * client.operate(null, key,
	 *     StringOperation.prepend(StringPolicy.Default, "text", "hello "));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param value		text to prepend to the start of the string
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation prepend(StringPolicy policy, String binName, String value, CTX... ctx) {
		byte[] bytes = Pack.pack(PREPEND, Value.get(value), policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code snip} operation that removes the half-open codepoint range
	 * {@code [start, end)} from the bin.
	 *
	 * <pre>{@code
	 * // "hello beautiful world" snip [5, 15) -> "hello world"
	 * client.operate(null, key,
	 *     StringOperation.snip(StringPolicy.Default, "text", 5, 15));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param start		first codepoint to remove (inclusive)
	 * @param end		one past the last codepoint to remove (exclusive)
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation snip(StringPolicy policy, String binName, int start, int end, CTX... ctx) {
		byte[] bytes = Pack.pack(SNIP, start, end, policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code replace} operation that replaces the first occurrence of
	 * {@code needle} with {@code replacement}.
	 *
	 * <pre>{@code
	 * // "hello world world" replace "world"->"earth" -> "hello earth world"
	 * client.operate(null, key,
	 *     StringOperation.replace(StringPolicy.Default, "text", "world", "earth"));
	 * }</pre>
	 *
	 * @param policy		write policy controlling NO_FAIL semantics
	 * @param binName		name of the string bin
	 * @param needle		substring to find
	 * @param replacement	text to substitute (may be empty to delete the match)
	 * @param ctx			optional path into a string nested inside a list or map
	 * @return				modify operation
	 */
	public static Operation replace(StringPolicy policy, String binName, String needle, String replacement, CTX... ctx) {
		List<Value> list = pair(needle, replacement);
		byte[] bytes = Pack.pack(REPLACE, list, policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code replaceAll} operation that replaces every occurrence of
	 * {@code needle} with {@code replacement}.
	 *
	 * <pre>{@code
	 * // "aabaa" replaceAll "a"->"x" -> "xxbxx"
	 * client.operate(null, key,
	 *     StringOperation.replaceAll(StringPolicy.Default, "text", "a", "x"));
	 * }</pre>
	 *
	 * @param policy		write policy controlling NO_FAIL semantics
	 * @param binName		name of the string bin
	 * @param needle		substring to find
	 * @param replacement	text to substitute (may be empty to delete each match)
	 * @param ctx			optional path into a string nested inside a list or map
	 * @return				modify operation
	 */
	public static Operation replaceAll(StringPolicy policy, String binName, String needle, String replacement, CTX... ctx) {
		List<Value> list = pair(needle, replacement);
		byte[] bytes = Pack.pack(REPLACE_ALL, list, policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code upper} operation that uppercases the bin in place.
	 *
	 * <pre>{@code
	 * // "hello world" -> "HELLO WORLD"
	 * client.operate(null, key, StringOperation.upper(StringPolicy.Default, "text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation upper(StringPolicy policy, String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(UPPER, policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code lower} operation that lowercases the bin in place.
	 *
	 * <pre>{@code
	 * // "HELLO WORLD" -> "hello world"
	 * client.operate(null, key, StringOperation.lower(StringPolicy.Default, "text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation lower(StringPolicy policy, String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(LOWER, policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code caseFold} operation that applies a locale-independent case
	 * fold (lowercase) to the bin. Useful for normalized comparison keys.
	 *
	 * <pre>{@code
	 * // "HELLO World" -> "hello world"
	 * client.operate(null, key, StringOperation.caseFold(StringPolicy.Default, "text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation caseFold(StringPolicy policy, String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(CASE_FOLD, policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code normalizeNFC} operation that normalizes the bin to Unicode
	 * NFC form. Already-normalized strings are unchanged.
	 *
	 * <pre>{@code
	 * client.operate(null, key,
	 *     StringOperation.normalizeNFC(StringPolicy.Default, "text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation normalizeNFC(StringPolicy policy, String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(NORMALIZE_NFC, policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code trimStart} operation that removes whitespace from the start
	 * of the bin.
	 *
	 * <pre>{@code
	 * // "  hello  " -> "hello  "
	 * client.operate(null, key,
	 *     StringOperation.trimStart(StringPolicy.Default, "text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation trimStart(StringPolicy policy, String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(TRIM_START, policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code trimEnd} operation that removes whitespace from the end of
	 * the bin.
	 *
	 * <pre>{@code
	 * // "  hello  " -> "  hello"
	 * client.operate(null, key,
	 *     StringOperation.trimEnd(StringPolicy.Default, "text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation trimEnd(StringPolicy policy, String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(TRIM_END, policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code trim} operation that removes whitespace from both ends of
	 * the bin.
	 *
	 * <pre>{@code
	 * // "  hello world  " -> "hello world"
	 * client.operate(null, key,
	 *     StringOperation.trim(StringPolicy.Default, "text"));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation trim(StringPolicy policy, String binName, CTX... ctx) {
		byte[] bytes = Pack.pack(TRIM, policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code padStart} operation that prepends {@code padString}
	 * repeatedly until the bin reaches {@code targetLength} codepoints. No-op when the
	 * bin is already at or above the target length.
	 *
	 * <pre>{@code
	 * // "hello" pad to 10 with "*" -> "*****hello"
	 * client.operate(null, key,
	 *     StringOperation.padStart(StringPolicy.Default, "text", 10, "*"));
	 * }</pre>
	 *
	 * @param policy		write policy controlling NO_FAIL semantics
	 * @param binName		name of the string bin
	 * @param targetLength	codepoint length to pad up to
	 * @param padString		text used to fill (repeated as needed)
	 * @param ctx			optional path into a string nested inside a list or map
	 * @return				modify operation
	 */
	public static Operation padStart(StringPolicy policy, String binName, int targetLength, String padString, CTX... ctx) {
		byte[] bytes = Pack.pack(PAD_START, targetLength, Value.get(padString), policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code padEnd} operation that appends {@code padString} repeatedly
	 * until the bin reaches {@code targetLength} codepoints. No-op when the bin is
	 * already at or above the target length.
	 *
	 * <pre>{@code
	 * // "hello" pad to 10 with "." -> "hello....."
	 * client.operate(null, key,
	 *     StringOperation.padEnd(StringPolicy.Default, "text", 10, "."));
	 * }</pre>
	 *
	 * @param policy		write policy controlling NO_FAIL semantics
	 * @param binName		name of the string bin
	 * @param targetLength	codepoint length to pad up to
	 * @param padString		text used to fill (repeated as needed)
	 * @param ctx			optional path into a string nested inside a list or map
	 * @return				modify operation
	 */
	public static Operation padEnd(StringPolicy policy, String binName, int targetLength, String padString, CTX... ctx) {
		byte[] bytes = Pack.pack(PAD_END, targetLength, Value.get(padString), policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code repeat} operation that repeats the bin contents {@code count}
	 * times.
	 *
	 * <pre>{@code
	 * // "ab" repeat 3 -> "ababab"
	 * client.operate(null, key,
	 *     StringOperation.repeat(StringPolicy.Default, "text", 3));
	 * }</pre>
	 *
	 * @param policy	write policy controlling NO_FAIL semantics
	 * @param binName	name of the string bin
	 * @param count		number of repetitions (must be non-negative)
	 * @param ctx		optional path into a string nested inside a list or map
	 * @return			modify operation
	 */
	public static Operation repeat(StringPolicy policy, String binName, int count, CTX... ctx) {
		byte[] bytes = Pack.pack(REPEAT, count, policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	/**
	 * Create string {@code regexReplace} operation that replaces the first match of
	 * {@code pattern} with {@code replacement}. Pass {@link StringRegexFlags#GLOBAL}
	 * to replace every match. Flag values from {@link StringRegexFlags} may be combined
	 * with bitwise OR.
	 *
	 * <pre>{@code
	 * // "abc123def456" regexReplace "[0-9]+"->"NUM" with GLOBAL -> "abcNUMdefNUM"
	 * client.operate(null, key,
	 *     StringOperation.regexReplace(StringPolicy.Default, "text",
	 *         "[0-9]+", "NUM", StringRegexFlags.GLOBAL));
	 * }</pre>
	 *
	 * @param policy		write policy controlling NO_FAIL semantics; on this op NO_FAIL also
	 *						suppresses a regex-compile failure
	 * @param binName		name of the string bin
	 * @param pattern		ICU-syntax regex pattern (must be valid UTF-8)
	 * @param replacement	replacement text (must be valid UTF-8)
	 * @param regexFlags	bitwise-OR of {@link StringRegexFlags} constants
	 * @param ctx			optional path into a string nested inside a list or map
	 * @return				modify operation
	 */
	public static Operation regexReplace(
		StringPolicy policy,
		String binName,
		String pattern,
		String replacement,
		int regexFlags,
		CTX... ctx
	) {
		List<Value> list = pair(pattern, replacement);
		// All three args go on the wire: regex flags occupy the slot before policy flags and the
		// two bitmasks collide numerically, so omitting either has the other silently misread.
		byte[] bytes = Pack.pack(REGEX_REPLACE, list, regexFlags, policy.flags, ctx);
		return new Operation(Operation.Type.STRING_MODIFY, binName, new Value.BytesValue(bytes, ParticleType.STRING));
	}

	//-----------------------------------------------------------------
	// Type conversion
	//-----------------------------------------------------------------

	/**
	 * Create {@code toString} operation that converts an integer, float, boolean,
	 * string, or blob bin to its string representation. Returns
	 * {@code AEROSPIKE_ERR_INCOMPATIBLE_TYPE} for any other bin type. A blob bin whose
	 * bytes are not valid UTF-8 fails with
	 * {@link com.aerospike.client.ResultCode#OP_NOT_APPLICABLE} and subcode
	 * {@link com.aerospike.client.SubCode#OPNOT_STRING_UTF8_INVALID}.
	 * <p>
	 * Unlike the other builders in this class, {@code toString} does not accept a
	 * {@link CTX}. The other string operations are sent as {@code STRING_READ} /
	 * {@code STRING_MODIFY} wire ops whose msgpack payload carries the sub-op code,
	 * arguments, and (when CTX is non-empty) a
	 * {@code [0xFF, ctx_list, [inner_op, args...]]} wrapper that the server's
	 * CTX-aware dispatcher unwraps to descend into a list or map.
	 * {@code toString} is a separate top-level wire op
	 * ({@code Operation.Type.TO_STRING}) that carries no payload at all — the bin is
	 * referenced solely by the operation header — and the server-side handler for it
	 * is a different code path that operates on the whole bin particle and never
	 * inspects an op payload, so there is no place to encode a CTX wrapper and the
	 * server would not act on it if there were.
	 * <p>
	 * To convert a value nested inside a list or map, extract the leaf with
	 * {@link com.aerospike.client.cdt.ListOperation#getByIndex} or
	 * {@link com.aerospike.client.cdt.MapOperation#getByKey} (using the appropriate
	 * {@link CTX}) and convert it client-side.
	 *
	 * <pre>{@code
	 * // Bin "n" = 42 (integer) -> "42"
	 * Record r = client.operate(null, key, StringOperation.toString("n"));
	 * String s = r.getString("n");
	 * }</pre>
	 *
	 * @param binName	name of the bin to convert
	 * @return			read operation returning the string representation of the bin
	 */
	public static Operation toString(String binName) {
		return new Operation(Operation.Type.TO_STRING, binName, Value.getAsNull());
	}

	//-----------------------------------------------------------------
	// Private helpers
	//-----------------------------------------------------------------

	private static List<Value> pair(String a, String b) {
		List<Value> list = new ArrayList<Value>(2);
		list.add(Value.get(a));
		list.add(Value.get(b));
		return list;
	}

	private static List<Value> toValueList(List<String> strings) {
		List<Value> list = new ArrayList<Value>(strings.size());
		for (String s : strings) {
			list.add(Value.get(s));
		}
		return list;
	}

	//-----------------------------------------------------------------
	// Wire envelope. Pack.pack emits [SUBOP, args...] or, when CTX is
	// non-empty, the context-eval envelope
	//     [0xFF, [ctx_id_1, ctx_value_1, ...], [SUBOP, args...]]
	// String ops carried their own packer for as long as the server read
	// that envelope flat, with the sub-op and its args as outer siblings of
	// the sentinel rather than nested. SERVER-1483 nests them, matching what
	// the CDT modules always emitted, so the shared overloads now serve both.
	//-----------------------------------------------------------------
}
